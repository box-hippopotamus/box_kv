// SSTable Iterator Adapter

use super::{InternalKey, KVIterator};
use crate::error::{BoxKVError, BoxKVResult};
use crate::sstable::SSTableReader;
use boxkv_common::types::{Entry, ValueType as StorageValueType};
use bytes::Bytes;
use std::sync::Arc;

/// SSTable 迭代器适配器
pub struct SSTableIterator {
    /// 内部迭代器
    inner: crate::sstable::SSTableIterator,

    /// 当前缓存的 entry
    current_entry: Option<Entry>,

    /// 是否有效
    valid: bool,
}

impl SSTableIterator {
    /// 创建新的 SSTable 迭代器适配器
    pub fn new(reader: Arc<SSTableReader>) -> BoxKVResult<Self> {
        // 创建内部迭代器（无范围限制）
        let inner = crate::sstable::SSTableIterator::new(reader, None, None).map_err(|e| {
            BoxKVError::Internal(format!("Failed to create SSTable iterator: {:?}", e))
        })?;

        let mut iter = Self {
            inner,
            current_entry: None,
            valid: false,
        };

        // 立即加载第一个 entry
        iter.load_current()?;

        Ok(iter)
    }

    /// 加载当前 entry
    fn load_current(&mut self) -> BoxKVResult<()> {
        match self.inner.next() {
            Ok(Some(entry)) => {
                self.current_entry = Some(entry);
                self.valid = true;
                Ok(())
            }
            Ok(None) => {
                self.current_entry = None;
                self.valid = false;
                Ok(())
            }
            Err(e) => {
                self.current_entry = None;
                self.valid = false;
                Err(BoxKVError::Internal(format!(
                    "SSTable iterator error: {:?}",
                    e
                )))
            }
        }
    }

    /// 将 Entry 转换为 InternalKey
    fn to_internal_key(entry: &Entry) -> InternalKey {
        InternalKey::new(entry.key.clone(), entry.sequence)
    }

    /// 提取 Entry 的值
    fn to_value(entry: &Entry) -> Bytes {
        match &entry.value {
            StorageValueType::Normal(data) => data.clone(),
            StorageValueType::Expiring { data, .. } => data.clone(),
            StorageValueType::Tombstone => Bytes::new(),
        }
    }
}

impl KVIterator for SSTableIterator {
    fn seek(&mut self, target: &InternalKey) -> BoxKVResult<()> {
        self.inner
            .seek(target.user_key.as_ref())
            .map_err(|e| BoxKVError::Internal(format!("Seek failed: {:?}", e)))?;

        // 加载当前 entry
        if self.inner.valid() {
            self.load_current()?;
        } else {
            self.valid = false;
            self.current_entry = None;
        }

        Ok(())
    }

    fn seek_to_first(&mut self) -> BoxKVResult<()> {
        self.inner
            .seek_to_first()
            .map_err(|e| BoxKVError::Internal(format!("Seek to first failed: {:?}", e)))?;

        // 加载当前 entry
        if self.inner.valid() {
            self.load_current()?;
        } else {
            self.valid = false;
            self.current_entry = None;
        }

        Ok(())
    }

    fn seek_to_last(&mut self) -> BoxKVResult<()> {
        while self.valid {
            let _ = self.load_current();
        }
        Ok(())
    }

    fn next(&mut self) -> BoxKVResult<()> {
        self.load_current()
    }

    fn prev(&mut self) -> BoxKVResult<()> {
        // SSTable 迭代器不支持反向遍历
        Err(BoxKVError::Internal(
            "SSTable iterator does not support backward iteration".to_string(),
        ))
    }

    fn valid(&self) -> bool {
        self.valid
    }

    fn key(&self) -> Option<InternalKey> {
        self.current_entry.as_ref().map(Self::to_internal_key)
    }

    fn value(&self) -> Option<Bytes> {
        self.current_entry.as_ref().map(Self::to_value)
    }

    fn status(&self) -> BoxKVResult<()> {
        Ok(())
    }
}
