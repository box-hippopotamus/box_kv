use crate::sstable::{
    Result, SSTableError,
    data_block::{DataBlockCodec, DataBlockReader, InternalKey},
    format::BlockHandle,
    index_block::IndexKey,
    reader::SSTableReader,
};
use boxkv_common::config::GlobalConfig;
use boxkv_common::types::Entry;
use bytes::Bytes;
use std::sync::Arc;

/// SSTable 迭代器
pub struct SSTableIterator {
    /// SSTable 读取器
    reader: Arc<SSTableReader>,

    /// 当前 IndexBlock 的 key
    current_index_key: Option<Bytes>,

    /// 当前 DataBlock 的 BlockHandle
    current_block_handle: Option<BlockHandle>,

    /// 当前 DataBlock 的数据（
    current_block_data: Option<Bytes>,

    /// 当前在 DataBlock 中的位置
    current_entry_index: usize,

    /// 起始 key
    start_key: Option<Bytes>,

    /// 结束 key
    end_key: Option<Bytes>,

    /// 迭代器是否有效
    valid: bool,

    /// 当前缓存的 entry
    current_entry: Option<Entry>,
}

impl SSTableIterator {
    /// 创建新的迭代器
    pub fn new(
        reader: Arc<SSTableReader>,
        start_key: Option<Bytes>,
        end_key: Option<Bytes>,
    ) -> Result<Self> {
        let mut iter = Self {
            reader,
            current_index_key: None,
            current_block_handle: None,
            current_block_data: None,
            current_entry_index: 0,
            start_key: start_key.clone(),
            end_key,
            valid: false,
            current_entry: None,
        };

        if let Some(ref start) = start_key {
            iter.seek(start.as_ref())?;
        } else {
            iter.seek_to_first()?;
        }

        Ok(iter)
    }

    /// 移动到下一个 entry
    pub fn next(&mut self) -> Result<Option<Entry>> {
        if !self.valid {
            return Ok(None);
        }

        let index_block = self.reader.index_block();

        if self.current_block_handle.is_none() {
            let mut index_iter = index_block.iter();
            match &self.current_index_key {
                None => {
                    if !index_iter.valid() {
                        index_iter.seek_to_first().map_err(|e| {
                            SSTableError::Corrupted(format!("Failed to seek index: {:?}", e))
                        })?;
                    }
                }
                Some(prev_key_bytes) => {
                    let search_key = IndexKey {
                        user_key: prev_key_bytes.clone(),
                    };
                    index_iter.seek(&search_key).map_err(|e| {
                        SSTableError::Corrupted(format!("Failed to seek index: {:?}", e))
                    })?;
                    if index_iter.valid()
                        && let Some(k) = index_iter.key()
                        && k.user_key <= prev_key_bytes
                    {
                        index_iter.next().map_err(|e| {
                            SSTableError::Corrupted(format!(
                                "Failed to move index iterator: {:?}",
                                e
                            ))
                        })?;
                    }
                }
            }

            if !index_iter.valid() {
                self.valid = false;
                return Ok(None);
            }

            if let Some(ref end) = self.end_key
                && let Some(k) = index_iter.key()
                && k.user_key.as_ref() >= end.as_ref()
            {
                self.valid = false;
                return Ok(None);
            }

            let index_value = index_iter.value().map_err(|e| {
                SSTableError::Corrupted(format!("Failed to get index value: {:?}", e))
            })?;

            if let Some(handle) = index_value {
                self.current_block_handle = Some(handle);
                self.current_entry_index = 0;
                if let Some(k) = index_iter.key() {
                    self.current_index_key = Some(k.user_key.clone());
                } else {
                    self.current_index_key = None;
                }

                let mut index_iter_next = index_block.iter();
                if let Some(ref key_bytes) = self.current_index_key {
                    let search_key = IndexKey {
                        user_key: key_bytes.clone(),
                    };
                    index_iter_next.seek(&search_key).map_err(|e| {
                        SSTableError::Corrupted(format!("Failed to seek index: {:?}", e))
                    })?;
                    if index_iter_next.valid() {
                        if let Some(k) = index_iter_next.key()
                            && k.user_key <= key_bytes
                        {
                            index_iter_next.next().map_err(|e| {
                                SSTableError::Corrupted(format!(
                                    "Failed to move index iterator: {:?}",
                                    e
                                ))
                            })?;
                        }
                        if index_iter_next.valid()
                            && let Some(next_key) = index_iter_next.key()
                        {
                            let within = if let Some(ref end) = self.end_key {
                                next_key.user_key.as_ref() < end.as_ref()
                            } else {
                                true
                            };
                            if within && let Ok(Some(next_handle)) = index_iter_next.value() {
                                if GlobalConfig::get().sstable.enable_prefetch {
                                    let _ = self.reader.prefetch_block(&next_handle);
                                }
                            }
                        }
                    }
                }
            } else {
                self.valid = false;
                return Ok(None);
            }
        }

        // 检查是否需要加载新的 DataBlock
        let handle = self.current_block_handle.as_ref().ok_or_else(|| {
            SSTableError::Corrupted("Internal error: current_block_handle is None".to_string())
        })?;
        let need_reload = self.current_block_data.is_none();

        if need_reload {
            let data = self.reader.read_data_block(handle)?;
            self.current_block_data = Some(data);
        }

        // 使用缓存的 Block 数据
        let data_block = DataBlockReader::new(
            DataBlockCodec,
            self.current_block_data
                .as_ref()
                .ok_or_else(|| {
                    SSTableError::Corrupted(
                        "Internal error: current_block_data is None".to_string(),
                    )
                })?
                .clone(),
        )
        .map_err(|e| SSTableError::Corrupted(format!("Failed to parse data block: {:?}", e)))?;

        // 创建迭代器并定位到当前位置
        let mut data_iter = data_block.iter();
        data_iter
            .seek_to_first()
            .map_err(|e| SSTableError::Corrupted(format!("Failed to seek to first: {:?}", e)))?;

        // 跳过已读取的 entries
        for _ in 0..self.current_entry_index {
            if data_iter.valid() {
                data_iter.next().map_err(|e| {
                    SSTableError::Corrupted(format!("Failed to move iterator: {:?}", e))
                })?;
            } else {
                break;
            }
        }

        // 获取当前 entry
        if !data_iter.valid() {
            self.current_block_handle = None;
            self.current_entry_index = 0;
            return self.next();
        }

        let internal_key = match data_iter.key() {
            Some(k) => k.clone(),
            None => {
                self.valid = false;
                return Ok(None);
            }
        };

        // 检查是否超出范围
        if let Some(ref end) = self.end_key
            && internal_key.user_key.as_ref() >= end.as_ref()
        {
            self.valid = false;
            return Ok(None);
        }

        // 获取 value
        let internal_value = match data_iter.value() {
            Ok(Some(v)) => v,
            Ok(None) => {
                // 移动到下一个 entry
                data_iter.next().map_err(|e| {
                    SSTableError::Corrupted(format!("Failed to move iterator: {:?}", e))
                })?;
                self.current_entry_index += 1;
                return self.next();
            }
            Err(e) => {
                return Err(SSTableError::Corrupted(format!(
                    "Failed to get value: {:?}",
                    e
                )));
            }
        };

        // 创建 entry
        let entry = Entry {
            key: internal_key.user_key,
            value: internal_value,
            sequence: internal_key.sequence,
        };

        // 移动到下一个 entry（为下次调用准备）
        data_iter
            .next()
            .map_err(|e| SSTableError::Corrupted(format!("Failed to move iterator: {:?}", e)))?;
        self.current_entry_index += 1;

        // 检查迭代器是否仍然有效
        if !data_iter.valid() {
            self.current_block_handle = None;
            self.current_block_data = None;
            self.current_entry_index = 0;
        }

        Ok(Some(entry))
    }

    /// 定位到指定 key
    pub fn seek(&mut self, target: &[u8]) -> Result<()> {
        let index_block = self.reader.index_block();

        let search_key = IndexKey {
            user_key: Bytes::copy_from_slice(target),
        };
        let mut index_iter = index_block.iter();
        index_iter.seek(&search_key).map_err(|e| {
            SSTableError::Corrupted(format!("Failed to seek in index block: {:?}", e))
        })?;

        if !index_iter.valid() {
            self.valid = false;
            self.current_block_handle = None;
            self.current_block_data = None;
            self.current_entry_index = 0;
            self.current_index_key = None;
            self.current_entry = None;
            return Ok(());
        }

        // 2. 获取对应的 DataBlock handle
        let handle = match index_iter.value() {
            Ok(Some(h)) => h,
            Ok(None) => {
                self.valid = false;
                self.current_block_handle = None;
                self.current_block_data = None;
                self.current_entry_index = 0;
                self.current_index_key = None;
                self.current_entry = None;
                return Ok(());
            }
            Err(e) => {
                return Err(SSTableError::Corrupted(format!(
                    "Failed to get index value: {:?}",
                    e
                )));
            }
        };

        if let Some(ref end) = self.end_key
            && let Some(k) = index_iter.key()
            && k.user_key.as_ref() >= end.as_ref()
        {
            self.valid = false;
            self.current_block_handle = None;
            self.current_block_data = None;
            self.current_entry_index = 0;
            self.current_index_key = None;
            self.current_entry = None;
            return Ok(());
        }

        // 3. 读取并解析 DataBlock
        let data_block_data = self.reader.read_data_block(&handle)?;
        let data_block = DataBlockReader::new(DataBlockCodec, data_block_data)
            .map_err(|e| SSTableError::Corrupted(format!("Failed to parse data block: {:?}", e)))?;

        // 4. 在 DataBlock 中定位
        let mut data_iter = data_block.iter();
        let search_internal_key = InternalKey {
            user_key: Bytes::copy_from_slice(target),
            sequence: u64::MAX,
        };
        data_iter.seek(&search_internal_key).map_err(|e| {
            SSTableError::Corrupted(format!("Failed to seek in data block: {:?}", e))
        })?;

        self.current_block_handle = Some(handle);
        self.current_entry_index = 0;
        self.valid = data_iter.valid();
        if let Some(k) = index_iter.key() {
            self.current_index_key = Some(k.user_key.clone());
        } else {
            self.current_index_key = None;
        }
        self.current_entry = None;

        {
            let mut index_iter_next = index_block.iter();
            let search_key = IndexKey {
                user_key: Bytes::copy_from_slice(target),
            };
            index_iter_next
                .seek(&search_key)
                .map_err(|e| SSTableError::Corrupted(format!("Failed to seek index: {:?}", e)))?;
            if index_iter_next.valid() {
                if let Some(k) = index_iter_next.key()
                    && k.user_key.as_ref() <= target
                {
                    index_iter_next.next().map_err(|e| {
                        SSTableError::Corrupted(format!("Failed to move index iterator: {:?}", e))
                    })?;
                }
                if index_iter_next.valid()
                    && let Some(next_key) = index_iter_next.key()
                {
                    let within = if let Some(ref end) = self.end_key {
                        next_key.user_key.as_ref() < end.as_ref()
                    } else {
                        true
                    };
                    if within && let Ok(Some(next_handle)) = index_iter_next.value() {
                        if GlobalConfig::get().sstable.enable_prefetch {
                            let _ = self.reader.prefetch_block(&next_handle);
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// 定位到第一个 entry
    pub fn seek_to_first(&mut self) -> Result<()> {
        let index_block = self.reader.index_block();

        // 1. 在 IndexBlock 中定位到第一个
        let mut index_iter = index_block.iter();
        index_iter.seek_to_first().map_err(|e| {
            SSTableError::Corrupted(format!("Failed to seek to first in index block: {:?}", e))
        })?;

        if !index_iter.valid() {
            self.valid = false;
            self.current_block_handle = None;
            self.current_block_data = None;
            self.current_entry_index = 0;
            self.current_index_key = None;
            self.current_entry = None;
            return Ok(());
        }

        if let Some(ref end) = self.end_key
            && let Some(k) = index_iter.key()
            && k.user_key.as_ref() >= end.as_ref()
        {
            self.valid = false;
            self.current_block_handle = None;
            self.current_block_data = None;
            self.current_entry_index = 0;
            self.current_index_key = None;
            self.current_entry = None;
            return Ok(());
        }

        // 2. 获取第一个 DataBlock handle
        let handle = match index_iter.value() {
            Ok(Some(h)) => h,
            Ok(None) => {
                self.valid = false;
                self.current_block_handle = None;
                self.current_block_data = None;
                self.current_entry_index = 0;
                self.current_index_key = None;
                self.current_entry = None;
                return Ok(());
            }
            Err(e) => {
                return Err(SSTableError::Corrupted(format!(
                    "Failed to get index value: {:?}",
                    e
                )));
            }
        };

        // 3. 读取并解析 DataBlock
        let data_block_data = self.reader.read_data_block(&handle)?;
        let data_block = DataBlockReader::new(DataBlockCodec, data_block_data.clone())
            .map_err(|e| SSTableError::Corrupted(format!("Failed to parse data block: {:?}", e)))?;

        // 4. 在 DataBlock 中定位到第一个
        let mut data_iter = data_block.iter();
        data_iter.seek_to_first().map_err(|e| {
            SSTableError::Corrupted(format!("Failed to seek to first in data block: {:?}", e))
        })?;

        // 更新状态（缓存 Block 数据）
        self.current_block_handle = Some(handle);
        self.current_block_data = Some(data_block_data);
        self.current_entry_index = 0;
        self.valid = data_iter.valid();
        if let Some(k) = index_iter.key() {
            self.current_index_key = Some(k.user_key.clone());
        } else {
            self.current_index_key = None;
        }
        self.current_entry = None;

        {
            let mut index_iter_next = index_block.iter();
            index_iter_next.seek_to_first().map_err(|e| {
                SSTableError::Corrupted(format!("Failed to seek to first in index block: {:?}", e))
            })?;
            if index_iter_next.valid() {
                if let Some(k) = index_iter_next.key()
                    && Some(k.user_key.clone()) == self.current_index_key
                {
                    index_iter_next.next().map_err(|e| {
                        SSTableError::Corrupted(format!("Failed to move index iterator: {:?}", e))
                    })?;
                }
                if index_iter_next.valid()
                    && let Some(next_key) = index_iter_next.key()
                {
                    let within = if let Some(ref end) = self.end_key {
                        next_key.user_key.as_ref() < end.as_ref()
                    } else {
                        true
                    };
                    if within && let Ok(Some(next_handle)) = index_iter_next.value() {
                        if GlobalConfig::get().sstable.enable_prefetch {
                            let _ = self.reader.prefetch_block(&next_handle);
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// 检查迭代器是否有效
    pub fn valid(&self) -> bool {
        self.valid
    }
}
