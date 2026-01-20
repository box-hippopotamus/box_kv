use crate::db::error::{DBError, Result};
use crate::memtable::Memtable;
use crate::sequence::SequenceGenerator;
use crate::wal::Wal;
use arc_swap::ArcSwap;
use boxkv_common::config::GlobalConfig;
use boxkv_common::types::{Entry, ValueType};
use boxkv_storage::FileSystem;
use bytes::Bytes;
use std::sync::atomic::{AtomicBool, Ordering};
/// WriteBatch 批量写入
///
/// 聚合多条变更为一个原子批次：为整批分配连续序列号，先写 WAL，再写入 Memtable。
/// 目标是将 WAL 的 I/O 合并为一次批量追加（具体是否 sync 由上层策略决定）。
use std::sync::{Arc, Mutex};

/// 原子批次容器：按追加顺序保存 put/delete/ttl 变更。
pub struct WriteBatch {
    entries: Vec<BatchEntry>,
}

/// 单条写入语义。
#[derive(Clone)]
pub enum BatchEntry {
    Put {
        key: Bytes,
        value: Bytes,
    },
    Delete {
        key: Bytes,
    },
    PutWithTTL {
        key: Bytes,
        value: Bytes,
        expire_at: u64,
    },
}

impl WriteBatch {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    pub fn put(&mut self, key: Bytes, value: Bytes) {
        self.entries.push(BatchEntry::Put { key, value });
    }

    pub fn delete(&mut self, key: Bytes) {
        self.entries.push(BatchEntry::Delete { key });
    }

    pub fn put_with_ttl(&mut self, key: Bytes, value: Bytes, ttl_secs: u64) {
        let expire_at = boxkv_common::time::current_timestamp_secs() + ttl_secs;
        self.entries.push(BatchEntry::PutWithTTL {
            key,
            value,
            expire_at,
        });
    }

    pub fn clear(&mut self) {
        self.entries.clear();
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    // 供调度/背压使用的粗略估算（含固定元数据开销）
    pub fn estimated_size(&self) -> u64 {
        self.entries
            .iter()
            .map(|entry| match entry {
                BatchEntry::Put { key, value } => (key.len() + value.len() + 16) as u64,
                BatchEntry::Delete { key } => (key.len() + 16) as u64,
                BatchEntry::PutWithTTL { key, value, .. } => (key.len() + value.len() + 24) as u64,
            })
            .sum()
    }
}

impl Default for WriteBatch {
    fn default() -> Self {
        Self::new()
    }
}

/// 执行批量写入：分配序列号 → WAL 追加 → Memtable 应用。
pub fn write_batch<FS: FileSystem>(
    batch: WriteBatch,
    wal: &Arc<Mutex<Wal<FS>>>,
    mem: &Arc<ArcSwap<Memtable>>,
    sequence: &SequenceGenerator,
    closed: &AtomicBool,
) -> Result<()> {
    let cfg = GlobalConfig::get();

    if closed.load(Ordering::Acquire) {
        return Err(DBError::Closed);
    }

    if batch.len() > cfg.limits.max_batch_size {
        return Err(DBError::BatchTooLarge(
            batch.len(),
            cfg.limits.max_batch_size,
        ));
    }

    // 同一批次内序列号连续，便于恢复与一致性检查
    let start_seq = sequence.next_batch(batch.len() as u64);

    // WAL：先落盘再应用到内存结构，恢复时以 WAL 为准
    {
        let mut wal_entries: Vec<Entry> = Vec::with_capacity(batch.entries.len());
        for (i, entry) in batch.entries.iter().enumerate() {
            let seq = start_seq + i as u64;
            match entry {
                BatchEntry::Put { key, value } => {
                    wal_entries.push(Entry::new_normal(key.clone(), value.clone(), seq));
                }
                BatchEntry::Delete { key } => {
                    wal_entries.push(Entry::new_tombstone(key.clone(), seq));
                }
                BatchEntry::PutWithTTL {
                    key,
                    value,
                    expire_at,
                } => {
                    wal_entries.push(Entry::new_expiring(
                        key.clone(),
                        value.clone(),
                        seq,
                        *expire_at,
                    ));
                }
            }
        }

        let mut wal_guard = wal.lock().map_err(|e| {
            tracing::error!("WAL lock poisoned in batch write: {}", e);
            DBError::Internal(format!("WAL lock poisoned: {}", e))
        })?;
        wal_guard
            .append_batch(&wal_entries)
            .map_err(|e| DBError::Internal(format!("wal append_batch failed: {:?}", e)))?;
        // 是否 sync 由 WalSyncMode 决定；此处仅负责追加
    }

    // Memtable：应用与 WAL 相同的顺序与序列号
    {
        let current_mem = mem.load();
        for (i, entry) in batch.entries.iter().enumerate() {
            let seq = start_seq + i as u64;

            match entry {
                BatchEntry::Put { key, value } => {
                    current_mem.insert(key.clone(), ValueType::Normal(value.clone()), seq);
                }
                BatchEntry::Delete { key } => {
                    current_mem.insert(key.clone(), ValueType::Tombstone, seq);
                }
                BatchEntry::PutWithTTL {
                    key,
                    value,
                    expire_at,
                } => {
                    current_mem.insert(
                        key.clone(),
                        ValueType::Expiring {
                            data: value.clone(),
                            expire_at: *expire_at,
                        },
                        seq,
                    );
                }
            }
        }
    }

    Ok(())
}
