// Level Iterator

use super::{InternalKey, KVIterator, SSTableIterator};
use crate::cache::TableCache;
use crate::error::{BoxKVError, BoxKVResult};
use crate::version::FileMeta;
use bytes::Bytes;
use std::sync::Arc;

/// Level 迭代器（Level 1+ 专用）
pub struct LevelIterator {
    /// 该 Level 的所有文件（已排序）
    files: Vec<Arc<FileMeta>>,

    /// TableCache（用于打开 SSTable）
    table_cache: Arc<TableCache>,

    /// 当前文件索引
    current_file_index: Option<usize>,

    /// 当前 SSTable 迭代器
    current_iter: Option<SSTableIterator>,

    /// 错误信息
    error: Option<String>,
}

impl LevelIterator {
    /// 创建新的 Level 迭代器
    pub fn new(files: Vec<Arc<FileMeta>>, table_cache: Arc<TableCache>) -> Self {
        Self {
            files,
            table_cache,
            current_file_index: None,
            current_iter: None,
            error: None,
        }
    }

    /// 打开指定索引的文件
    fn open_file(&mut self, file_idx: usize) -> BoxKVResult<()> {
        if file_idx >= self.files.len() {
            self.current_file_index = None;
            self.current_iter = None;
            return Ok(());
        }

        let file = &self.files[file_idx];

        // 从 TableCache 获取 SSTableReader
        let reader = self.table_cache.get_reader(file.file_number).map_err(|e| {
            BoxKVError::Internal(format!(
                "Failed to open SSTable {}: {:?}",
                file.file_number, e
            ))
        })?;

        // 创建 SSTableIterator
        let iter = SSTableIterator::new(reader)?;

        self.current_file_index = Some(file_idx);
        self.current_iter = Some(iter);
        Ok(())
    }

    /// 定位到包含 target 的文件
    fn find_file(&self, target: &InternalKey) -> Option<usize> {
        // 二分查找：找到第一个 largest >= target.user_key 的文件
        self.files
            .iter()
            .position(|f| f.largest.user_key >= target.user_key)
    }
}

impl KVIterator for LevelIterator {
    fn seek(&mut self, target: &InternalKey) -> BoxKVResult<()> {
        // 找到包含 target 的文件
        let file_idx = match self.find_file(target) {
            Some(idx) => idx,
            None => {
                // 没有文件包含 target
                self.current_file_index = None;
                self.current_iter = None;
                return Ok(());
            }
        };

        // 打开该文件
        self.open_file(file_idx)?;

        // 在文件内 seek
        if let Some(ref mut iter) = self.current_iter {
            iter.seek(target)?;

            // 如果当前文件没有 >= target 的 key，尝试下一个文件
            if !iter.valid() && file_idx + 1 < self.files.len() {
                self.open_file(file_idx + 1)?;
                if let Some(ref mut next_iter) = self.current_iter {
                    next_iter.seek_to_first()?;
                }
            }
        }

        Ok(())
    }

    fn seek_to_first(&mut self) -> BoxKVResult<()> {
        if self.files.is_empty() {
            self.current_file_index = None;
            self.current_iter = None;
            return Ok(());
        }

        self.open_file(0)?;
        if let Some(ref mut iter) = self.current_iter {
            iter.seek_to_first()?;
        }
        Ok(())
    }

    fn seek_to_last(&mut self) -> BoxKVResult<()> {
        if self.files.is_empty() {
            self.current_file_index = None;
            self.current_iter = None;
            return Ok(());
        }

        let last_idx = self.files.len() - 1;
        self.open_file(last_idx)?;
        if let Some(ref mut iter) = self.current_iter {
            iter.seek_to_last()?;
        }
        Ok(())
    }

    fn next(&mut self) -> BoxKVResult<()> {
        let iter = match self.current_iter.as_mut() {
            Some(i) => i,
            None => return Ok(()),
        };

        iter.next()?;

        // 如果当前文件遍历完，切换到下一个文件
        if !iter.valid() {
            if let Some(current_idx) = self.current_file_index {
                let next_idx = current_idx + 1;
                if next_idx < self.files.len() {
                    self.open_file(next_idx)?;
                    if let Some(ref mut next_iter) = self.current_iter {
                        next_iter.seek_to_first()?;
                    }
                } else {
                    self.current_file_index = None;
                    self.current_iter = None;
                }
            }
        }

        Ok(())
    }

    fn prev(&mut self) -> BoxKVResult<()> {
        let iter = match self.current_iter.as_mut() {
            Some(i) => i,
            None => return Ok(()),
        };

        iter.prev()?;

        // 如果当前文件已到头，切换到上一个文件
        if !iter.valid() {
            if let Some(current_idx) = self.current_file_index {
                if current_idx > 0 {
                    let prev_idx = current_idx - 1;
                    self.open_file(prev_idx)?;
                    if let Some(ref mut prev_iter) = self.current_iter {
                        prev_iter.seek_to_last()?;
                    }
                } else {
                    self.current_file_index = None;
                    self.current_iter = None;
                }
            }
        }

        Ok(())
    }

    fn valid(&self) -> bool {
        self.current_iter
            .as_ref()
            .map_or(false, |iter| iter.valid())
    }

    fn key(&self) -> Option<InternalKey> {
        self.current_iter.as_ref().and_then(|iter| iter.key())
    }

    fn value(&self) -> Option<Bytes> {
        self.current_iter.as_ref().and_then(|iter| iter.value())
    }

    fn status(&self) -> BoxKVResult<()> {
        if let Some(ref msg) = self.error {
            Err(BoxKVError::Internal(msg.clone()))
        } else if let Some(ref iter) = self.current_iter {
            iter.status()
        } else {
            Ok(())
        }
    }
}
