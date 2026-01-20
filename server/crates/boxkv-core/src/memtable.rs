use std::cmp::Reverse;
use std::sync::atomic::{AtomicUsize, Ordering};

use boxkv_common::codec::Encode;
use boxkv_common::types::ValueType;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;

/// InternalKey
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct InternalKey {
    /// 用户键
    user_key: Bytes,
    /// 序列号（逆序存储）
    sequence_order: Reverse<u64>,
}

impl InternalKey {
    fn new(user_key: Bytes, sequence: u64) -> Self {
        Self {
            user_key,
            sequence_order: Reverse(sequence),
        }
    }

    fn user_key(&self) -> &Bytes {
        &self.user_key
    }

    fn sequence(&self) -> u64 {
        self.sequence_order.0
    }
}

/// Memtable
pub struct Memtable {
    /// 全局有序存储
    data: SkipMap<InternalKey, ValueType>,
    /// 内存占用估算
    size: AtomicUsize,
}

impl Memtable {
    pub fn new() -> Self {
        Self {
            data: SkipMap::new(),
            size: AtomicUsize::new(0),
        }
    }

    pub fn insert(&self, key: Bytes, value: ValueType, sequence: u64) {
        let internal_key = InternalKey::new(key, sequence);
        let entry_size = value.encoded_len() + size_of::<u64>() + internal_key.user_key.len();

        self.data.insert(internal_key, value);
        self.size.fetch_add(entry_size, Ordering::Relaxed);
    }

    pub fn get(&self, key: Bytes) -> Option<(ValueType, u64)> {
        self.get_at(key, u64::MAX)
    }

    pub fn get_at(&self, key: Bytes, read_sequence: u64) -> Option<(ValueType, u64)> {
        let seek_key = InternalKey::new(key.clone(), read_sequence);
        let entry = self.data.range(seek_key..).next()?;
        if entry.key().user_key() != &key {
            return None;
        }

        Some((entry.value().clone(), entry.key().sequence()))
    }

    pub fn size(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }

    pub fn recompute_size(&self) -> usize {
        let mut total = 0usize;
        for entry in self.data.iter() {
            let key_size = entry.key().user_key.len();
            let value_size = entry.value().encoded_len();
            total += key_size + value_size + size_of::<u64>();
        }
        self.size.store(total, Ordering::Relaxed);
        total
    }

    pub fn iter(&self) -> impl Iterator<Item = (Bytes, ValueType, u64)> + '_ {
        self.data.iter().map(|entry| {
            let user_key = entry.key().user_key().clone();
            let value = entry.value().clone();
            let sequence = entry.key().sequence();
            (user_key, value, sequence)
        })
    }

    pub fn iter_latest(&self) -> impl Iterator<Item = (Bytes, ValueType, u64)> + '_ {
        let mut last_key: Option<Bytes> = None;

        self.data.iter().filter_map(move |entry| {
            let user_key = entry.key().user_key();

            if let Some(ref last) = last_key {
                if last == user_key {
                    return None;
                }
            }

            last_key = Some(user_key.clone());
            Some((
                user_key.clone(),
                entry.value().clone(),
                entry.key().sequence(),
            ))
        })
    }

    pub fn iter_version(
        &self,
        min_snapshot_seq: u64,
    ) -> impl Iterator<Item = (Bytes, ValueType, u64)> + '_ {
        self.data
            .iter()
            .filter(move |entry| entry.key().sequence() >= min_snapshot_seq)
            .map(|entry| {
                (
                    entry.key().user_key().clone(),
                    entry.value().clone(),
                    entry.key().sequence(),
                )
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    fn entry_size(key: &[u8], value: &ValueType) -> usize {
        key.len() + value.encoded_len() + size_of::<u64>()
    }

    #[test]
    fn insert_get_and_size_recompute_work() {
        let mem = Memtable::new();
        let v1 = ValueType::Normal(Bytes::from("v1"));
        let v2 = ValueType::Expiring {
            data: Bytes::from("v2"),
            expire_at: 123,
        };
        let v3 = ValueType::Tombstone;

        mem.insert(Bytes::from("k1"), v1.clone(), 1);
        mem.insert(Bytes::from("k2"), v2.clone(), 2);
        mem.insert(Bytes::from("k3"), v3.clone(), 3);

        // 读取最新版本
        assert_eq!(mem.get(Bytes::from("k1")), Some((v1.clone(), 1)));
        assert_eq!(mem.get(Bytes::from("k2")), Some((v2.clone(), 2)));
        assert_eq!(mem.get(Bytes::from("k3")), Some((v3.clone(), 3)));

        let approx_size = mem.size();
        let recomputed = mem.recompute_size();
        let expected = entry_size(b"k1", &v1) + entry_size(b"k2", &v2) + entry_size(b"k3", &v3);

        assert_eq!(approx_size, expected);
        assert_eq!(recomputed, expected);
    }

    #[test]
    fn mvcc_multi_version_insert_and_read() {
        let mem = Memtable::new();
        let v1 = ValueType::Normal(Bytes::from("v1"));
        let v2 = ValueType::Normal(Bytes::from("v2"));
        let v3 = ValueType::Normal(Bytes::from("v3"));

        // 同一个 key 插入多个版本
        mem.insert(Bytes::from("k"), v1.clone(), 10);
        mem.insert(Bytes::from("k"), v2.clone(), 20);
        mem.insert(Bytes::from("k"), v3.clone(), 30);

        // 读取最新版本
        assert_eq!(mem.get(Bytes::from("k")), Some((v3.clone(), 30)));

        // 按序列号读取（Snapshot 语义）
        assert_eq!(mem.get_at(Bytes::from("k"), 30), Some((v3.clone(), 30)));
        assert_eq!(mem.get_at(Bytes::from("k"), 25), Some((v2.clone(), 20)));
        assert_eq!(mem.get_at(Bytes::from("k"), 20), Some((v2.clone(), 20)));
        assert_eq!(mem.get_at(Bytes::from("k"), 15), Some((v1.clone(), 10)));
        assert_eq!(mem.get_at(Bytes::from("k"), 10), Some((v1.clone(), 10)));
        assert_eq!(mem.get_at(Bytes::from("k"), 5), None); // 早于所有版本

        // 大小应包含所有 3 个版本
        let expected_size = entry_size(b"k", &v1) + entry_size(b"k", &v2) + entry_size(b"k", &v3);
        assert_eq!(mem.recompute_size(), expected_size);
    }

    #[test]
    fn overwrite_same_key_accumulates_size() {
        let mem = Memtable::new();
        let original = ValueType::Normal(Bytes::from_static(b"long-value"));
        let smaller = ValueType::Normal(Bytes::from_static(b"v"));
        let larger = ValueType::Normal(Bytes::from_static(b"much-longer-value"));

        mem.insert(Bytes::from("k"), original.clone(), 10);
        let size_after_original = mem.size();
        assert_eq!(size_after_original, entry_size(b"k", &original));

        // 多版本：累加大小
        mem.insert(Bytes::from("k"), smaller.clone(), 11);
        let size_after_smaller = mem.size();
        assert_eq!(
            size_after_smaller,
            entry_size(b"k", &original) + entry_size(b"k", &smaller)
        );

        mem.insert(Bytes::from("k"), larger.clone(), 12);
        let size_after_larger = mem.size();
        assert_eq!(
            size_after_larger,
            entry_size(b"k", &original) + entry_size(b"k", &smaller) + entry_size(b"k", &larger)
        );

        // 验证读取最新版本
        assert_eq!(mem.get(Bytes::from("k")), Some((larger.clone(), 12)));
    }

    #[test]
    fn iter_covers_all_entries() {
        let mem = Memtable::new();
        let values = [
            (Bytes::from("k1"), ValueType::Normal(Bytes::from("a")), 1),
            (Bytes::from("k2"), ValueType::Tombstone, 2),
            (
                Bytes::from("k3"),
                ValueType::Expiring {
                    data: Bytes::from("b"),
                    expire_at: 42,
                },
                3,
            ),
        ];

        for (k, v, seq) in &values {
            mem.insert(k.clone(), v.clone(), *seq);
        }

        let mut collected: Vec<(Bytes, ValueType, u64)> = mem.iter().collect();
        collected.sort_by_key(|(_, _, seq)| *seq);

        let mut expected: Vec<(Bytes, ValueType, u64)> = values
            .iter()
            .map(|(k, v, s)| (k.clone(), v.clone(), *s))
            .collect();
        expected.sort_by_key(|(_, _, seq)| *seq);

        assert_eq!(collected, expected);
    }

    #[test]
    fn concurrent_inserts_keep_size_consistent() {
        let mem = Arc::new(Memtable::new());
        let threads = 4;
        let inserts_per_thread = 25;
        let value = ValueType::Normal(Bytes::from_static(b"payload"));

        let mut handles = Vec::new();
        for t in 0..threads {
            let mem_cloned = Arc::clone(&mem);
            let value_cloned = value.clone();
            handles.push(thread::spawn(move || {
                for i in 0..inserts_per_thread {
                    let key = Bytes::from(format!("k-{t}-{i}"));
                    let seq = (t * inserts_per_thread + i) as u64;
                    mem_cloned.insert(key, value_cloned.clone(), seq);
                }
            }));
        }

        for handle in handles {
            handle.join().expect("thread should finish");
        }

        // 验证所有条目都成功插入
        let expected_entries = threads * inserts_per_thread;
        let all_entries: Vec<_> = mem.iter().collect();
        assert_eq!(all_entries.len(), expected_entries);

        // 重算大小应与估算大小一致
        let approx_size = mem.size();
        let exact_size = mem.recompute_size();
        assert_eq!(approx_size, exact_size);
    }

    #[test]
    fn test_tombstone_semantics() {
        let mem = Memtable::new();
        let key = Bytes::from("k");

        // 插入普通值
        mem.insert(key.clone(), ValueType::Normal(Bytes::from("v1")), 10);
        assert_eq!(
            mem.get(key.clone()),
            Some((ValueType::Normal(Bytes::from("v1")), 10))
        );

        // 删除（插入 Tombstone）
        mem.insert(key.clone(), ValueType::Tombstone, 20);
        assert_eq!(mem.get(key.clone()), Some((ValueType::Tombstone, 20)));

        // 快照读取旧版本
        assert_eq!(
            mem.get_at(key.clone(), 15),
            Some((ValueType::Normal(Bytes::from("v1")), 10))
        );
        assert_eq!(
            mem.get_at(key.clone(), 20),
            Some((ValueType::Tombstone, 20))
        );
    }

    #[test]
    fn test_iter_latest() {
        let mem = Memtable::new();

        // 同一 key 的多个版本
        mem.insert(Bytes::from("k1"), ValueType::Normal(Bytes::from("v1")), 10);
        mem.insert(Bytes::from("k1"), ValueType::Normal(Bytes::from("v2")), 20);
        mem.insert(Bytes::from("k2"), ValueType::Normal(Bytes::from("v3")), 15);

        // iter_latest 只返回最新版本
        let latest: Vec<_> = mem.iter_latest().collect();
        assert_eq!(latest.len(), 2);

        let k1_entry = latest
            .iter()
            .find(|(k, _, _)| k == &Bytes::from("k1"))
            .unwrap();
        assert_eq!(k1_entry.1, ValueType::Normal(Bytes::from("v2")));
        assert_eq!(k1_entry.2, 20);

        let k2_entry = latest
            .iter()
            .find(|(k, _, _)| k == &Bytes::from("k2"))
            .unwrap();
        assert_eq!(k2_entry.1, ValueType::Normal(Bytes::from("v3")));
        assert_eq!(k2_entry.2, 15);
    }

    #[test]
    fn test_iter_all_versions_with_snapshot() {
        let mem = Memtable::new();

        // 插入多个版本
        mem.insert(Bytes::from("k"), ValueType::Normal(Bytes::from("v1")), 10);
        mem.insert(Bytes::from("k"), ValueType::Normal(Bytes::from("v2")), 20);
        mem.insert(Bytes::from("k"), ValueType::Normal(Bytes::from("v3")), 30);

        // 保留所有版本
        let all: Vec<_> = mem.iter_version(0).collect();
        assert_eq!(all.len(), 3);

        // 仅保留 seq >= 20 的版本
        let filtered: Vec<_> = mem.iter_version(20).collect();
        assert_eq!(filtered.len(), 2);
        assert!(filtered.iter().all(|(_, _, seq)| *seq >= 20));
    }
}
