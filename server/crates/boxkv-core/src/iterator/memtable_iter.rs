// Memtable Iterator

use super::{InternalKey, KVIterator};
use crate::error::{BoxKVError, BoxKVResult};
use crate::memtable::Memtable;
use boxkv_common::types::ValueType as StorageValueType;
use bytes::Bytes;
use std::sync::Arc;

/// Memtable 迭代器状态
enum IteratorState {
    /// 未初始化
    Uninitialized,
    /// 有效位置
    Valid {
        key: Bytes,
        value: StorageValueType,
        sequence: u64,
    },
    /// 已到末尾
    End,
    /// 错误状态
    Error(String),
}

/// Memtable 迭代器
pub struct MemtableIterator {
    memtable: Arc<Memtable>,
    /// 当前状态
    state: IteratorState,
    /// 缓存的所有条目（按 InternalKey 顺序）
    entries: Vec<(Bytes, StorageValueType, u64)>,
    /// 当前位置索引
    current_index: Option<usize>,
}

impl MemtableIterator {
    /// 创建新的迭代器
    pub fn new(memtable: Arc<Memtable>) -> Self {
        // 预加载所有条目
        let entries: Vec<_> = memtable.iter().collect();

        let mut iter = Self {
            memtable,
            state: IteratorState::Uninitialized,
            entries,
            current_index: None,
        };

        // 自动定位到第一个元素
        if !iter.entries.is_empty() {
            iter.current_index = Some(0);
            iter.update_state();
        }

        iter
    }

    /// 更新当前状态
    fn update_state(&mut self) {
        if let Some(idx) = self.current_index {
            if idx < self.entries.len() {
                let (key, value, sequence) = &self.entries[idx];
                self.state = IteratorState::Valid {
                    key: key.clone(),
                    value: value.clone(),
                    sequence: *sequence,
                };
            } else {
                self.state = IteratorState::End;
                self.current_index = None;
            }
        } else {
            self.state = IteratorState::End;
        }
    }
}

impl KVIterator for MemtableIterator {
    fn seek(&mut self, target: &InternalKey) -> BoxKVResult<()> {
        // 二分查找第一个 >= target 的位置
        let target_key = &target.user_key;
        let target_seq = target.sequence;

        // 查找第一个满足条件的位置
        let pos = self
            .entries
            .iter()
            .position(|(key, _, seq)| match key.cmp(target_key) {
                std::cmp::Ordering::Greater => true,
                std::cmp::Ordering::Equal => *seq <= target_seq,
                std::cmp::Ordering::Less => false,
            });

        self.current_index = pos;
        self.update_state();
        Ok(())
    }

    fn seek_to_first(&mut self) -> BoxKVResult<()> {
        self.current_index = if self.entries.is_empty() {
            None
        } else {
            Some(0)
        };
        self.update_state();
        Ok(())
    }

    fn seek_to_last(&mut self) -> BoxKVResult<()> {
        self.current_index = if self.entries.is_empty() {
            None
        } else {
            Some(self.entries.len() - 1)
        };
        self.update_state();
        Ok(())
    }

    fn next(&mut self) -> BoxKVResult<()> {
        if let Some(idx) = self.current_index {
            self.current_index = Some(idx + 1);
            self.update_state();
        } else {
            self.state = IteratorState::End;
        }
        Ok(())
    }

    fn prev(&mut self) -> BoxKVResult<()> {
        if let Some(idx) = self.current_index {
            if idx > 0 {
                self.current_index = Some(idx - 1);
                self.update_state();
            } else {
                self.current_index = None;
                self.state = IteratorState::End;
            }
        } else {
            self.state = IteratorState::End;
        }
        Ok(())
    }

    fn valid(&self) -> bool {
        matches!(self.state, IteratorState::Valid { .. })
    }

    fn key(&self) -> Option<InternalKey> {
        match &self.state {
            IteratorState::Valid { key, sequence, .. } => {
                Some(InternalKey::new(key.clone(), *sequence))
            }
            _ => None,
        }
    }

    fn value(&self) -> Option<Bytes> {
        match &self.state {
            IteratorState::Valid { value, .. } => match value {
                StorageValueType::Normal(data) => Some(data.clone()),
                StorageValueType::Expiring { data, .. } => Some(data.clone()),
                StorageValueType::Tombstone => Some(Bytes::new()),
            },
            _ => None,
        }
    }

    fn status(&self) -> BoxKVResult<()> {
        match &self.state {
            IteratorState::Error(msg) => Err(BoxKVError::Internal(msg.clone())),
            _ => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use boxkv_common::types::ValueType;

    #[test]
    fn test_memtable_iterator_basic() {
        let mem = Arc::new(Memtable::new());
        mem.insert(Bytes::from("key1"), ValueType::Normal(Bytes::from("v1")), 1);
        mem.insert(Bytes::from("key2"), ValueType::Normal(Bytes::from("v2")), 2);
        mem.insert(Bytes::from("key3"), ValueType::Normal(Bytes::from("v3")), 3);

        let mut iter = MemtableIterator::new(mem);

        // Seek to first
        iter.seek_to_first().unwrap();
        assert!(iter.valid());
        assert_eq!(iter.key().unwrap().user_key, Bytes::from("key1"));
        assert_eq!(iter.value().unwrap(), Bytes::from("v1"));

        // Next
        iter.next().unwrap();
        assert!(iter.valid());
        assert_eq!(iter.key().unwrap().user_key, Bytes::from("key2"));

        iter.next().unwrap();
        assert!(iter.valid());
        assert_eq!(iter.key().unwrap().user_key, Bytes::from("key3"));

        // End
        iter.next().unwrap();
        assert!(!iter.valid());
    }

    #[test]
    fn test_memtable_iterator_seek() {
        let mem = Arc::new(Memtable::new());
        mem.insert(
            Bytes::from("key1"),
            ValueType::Normal(Bytes::from("v1")),
            10,
        );
        mem.insert(
            Bytes::from("key2"),
            ValueType::Normal(Bytes::from("v2")),
            20,
        );
        mem.insert(
            Bytes::from("key3"),
            ValueType::Normal(Bytes::from("v3")),
            30,
        );

        let mut iter = MemtableIterator::new(mem);

        // Seek to key2
        let target = InternalKey::new(Bytes::from("key2"), 20);
        iter.seek(&target).unwrap();
        assert!(iter.valid());
        assert_eq!(iter.key().unwrap().user_key, Bytes::from("key2"));
    }
}
