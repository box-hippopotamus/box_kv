use crate::cache::TableCache;
use crate::db::error::Result;
use crate::db::reader;
use crate::db::types::SuperVersion;
use arc_swap::ArcSwap;
/// Snapshot 快照模块
/// - 一致性时间点读视图
/// - 按序列号冻结读取上限
/// - 复用 reader::get 实现，确保查找路径一致
/// - SnapshotList 追踪所有活跃快照，保护 Compaction 不删除可见版本
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// SnapshotList：管理所有活跃快照的序列号
/// - 用于追踪最小活跃快照序列号，保护 Compaction 不删除历史版本
/// - 快照创建时注册，销毁时自动注销
/// - 使用 SkipMap 实现有序多重集合，支持高并发与 O(log N) 插入/删除
pub struct SnapshotList {
    /// 有序快照映射：(sequence, token) -> ()
    /// - sequence: 快照序列号
    /// - token: 全局唯一标识，用于区分同一 sequence 的多个快照实例
    snapshots: SkipMap<(u64, u64), ()>,
    /// Token 生成器（全局递增）
    next_token: AtomicU64,
}

impl SnapshotList {
    /// 创建空的快照列表
    pub fn new() -> Self {
        Self {
            snapshots: SkipMap::new(),
            next_token: AtomicU64::new(0),
        }
    }

    /// 注册新快照
    /// - 返回唯一 token，用于后续注销
    /// - 复杂度：O(log N)
    pub fn register(&self, sequence: u64) -> u64 {
        let token = self.next_token.fetch_add(1, Ordering::SeqCst);
        self.snapshots.insert((sequence, token), ());
        token
    }

    /// 注销快照
    /// - 需要提供 sequence 和 token 精确定位快照实例
    /// - 复杂度：O(log N)
    pub fn unregister(&self, sequence: u64, token: u64) {
        self.snapshots.remove(&(sequence, token));
    }

    /// 获取最小活跃快照序列号
    /// - 如果没有活跃快照，返回 None
    /// - 用于 Compaction 决定保留哪些历史版本
    /// - 复杂度：O(1)
    pub fn oldest_sequence(&self) -> Option<u64> {
        self.snapshots.front().map(|entry| entry.key().0)
    }

    /// 获取活跃快照数量（用于调试）
    #[allow(dead_code)]
    pub fn count(&self) -> usize {
        self.snapshots.len()
    }
}

/// Snapshot：一致性时间点读视图
/// - `sequence`：该快照可见的最大序列号
/// - `token`：全局唯一标识，用于注销时精确定位
/// - `super_version`：拍下时的只读视图（包含 mem/imm/version）
/// - `table_cache`：用于读取 SSTable
/// - `snapshot_list`：用于自动注销快照
pub struct Snapshot {
    sequence: u64,
    token: u64,
    super_version: SuperVersion,
    table_cache: Arc<TableCache>,
    snapshot_list: Arc<SnapshotList>,
}

impl Snapshot {
    /// 创建快照（内部使用，由 BoxKV::snapshot() 调用）
    pub fn new(
        sequence: u64,
        super_version: SuperVersion,
        table_cache: Arc<TableCache>,
        snapshot_list: Arc<SnapshotList>,
    ) -> Self {
        // 注册到快照列表，获取唯一 token
        let token = snapshot_list.register(sequence);

        Self {
            sequence,
            token,
            super_version,
            table_cache,
            snapshot_list,
        }
    }

    /// 获取快照的序列号
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    /// 点查询（快照一致性）
    /// - 复用 reader::get 实现，传入快照序列号
    /// - 顺序：Mem → Imms（新→旧）→ L0（新→旧）→ L1+（二分查找）
    /// - 只返回 `seq <= snapshot.sequence` 的值
    /// - 考虑 TTL/Tombstone
    pub fn get(&self, key: Bytes) -> Option<Bytes> {
        // 构造临时的 ArcSwap 和 AtomicBool 用于复用 reader::get
        let super_version = Arc::new(ArcSwap::from_pointee(self.super_version.clone()));
        let closed = AtomicBool::new(false);

        // 复用 reader::get，传入快照序列号
        reader::get(
            key,
            &super_version,
            &self.table_cache,
            &closed,
            self.sequence,
        )
        .ok()
        .flatten()
    }

    /// 范围扫描（快照一致性）
    /// - 使用快照的序列号进行 MVCC 过滤
    /// - 只返回 `seq <= snapshot.sequence` 的值
    ///
    /// # 参数
    /// - `start`: 起始 key（包含）
    /// - `end`: 结束 key（不包含）
    /// - `limit`: 最大返回数量
    ///
    /// # 返回
    /// - 按 key 升序排列的 (key, value) 列表
    /// - 已过滤 Tombstone 和不可见版本
    pub fn scan_range(
        &self,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> Result<Vec<(Bytes, Bytes)>> {
        use crate::iterator::{
            KVIterator, LevelIterator, MemtableIterator, MergingIterator,
            SSTableIterator,
        };
        use crate::version::FileMeta;

        if limit == 0 {
            return Ok(Vec::new());
        }

        let _start_key = Bytes::copy_from_slice(start);
        let _end_key = Bytes::copy_from_slice(end);

        // 收集所有迭代器
        let mut iters: Vec<Box<dyn KVIterator>> = Vec::new();

        // 1. Memtable 迭代器
        let mem_iter = MemtableIterator::new(Arc::clone(&self.super_version.mem));
        iters.push(Box::new(mem_iter));

        // 2. Immutable Memtables 迭代器
        for imm in self.super_version.imm.iter() {
            let imm_iter = MemtableIterator::new(Arc::clone(imm));
            iters.push(Box::new(imm_iter));
        }

        // 3. SST files 迭代器
        // Level 0: 每个文件一个迭代器（文件可能重叠）
        if let Some(l0) = self.super_version.version.level(0) {
            for file in l0.iter() {
                // 检查文件是否与范围重叠
                if file.largest.user_key.as_ref() < start || file.smallest.user_key.as_ref() >= end
                {
                    continue;
                }

                if let Ok(reader) = self.table_cache.get_reader(file.file_number) {
                    if let Ok(sst_iter) = SSTableIterator::new(reader) {
                        iters.push(Box::new(sst_iter));
                    }
                }
            }
        }

        // Level 1+: 每个 Level 一个 LevelIterator（文件不重叠）
        for level_idx in 1..self.super_version.version.num_levels() {
            if let Some(level) = self.super_version.version.level(level_idx) {
                let files: Vec<Arc<FileMeta>> = level
                    .iter()
                    .filter(|file| {
                        // 过滤出与范围重叠的文件
                        file.largest.user_key.as_ref() >= start
                            && file.smallest.user_key.as_ref() < end
                    })
                    .map(|f| Arc::new(f.clone()))
                    .collect();

                if !files.is_empty() {
                    let level_iter = LevelIterator::new(files, Arc::clone(&self.table_cache));
                    iters.push(Box::new(level_iter));
                }
            }
        }

        // 创建 MergingIterator，使用快照序列号
        let mut merging_iter = MergingIterator::new(iters, self.sequence);

        // Seek 到 start
        merging_iter.seek_to_first()?;

        // 收集结果
        let mut results = Vec::new();

        while merging_iter.valid() && results.len() < limit {
            if let (Some(key), Some(value)) = (merging_iter.key(), merging_iter.value()) {
                // 检查是否超出范围
                if key.user_key.as_ref() >= end {
                    break;
                }

                // 检查是否在范围内
                if key.user_key.as_ref() >= start {
                    if !value.is_empty() {
                        results.push((key.user_key.clone(), value.clone()));
                    }
                }
            }

            merging_iter.next()?;
        }

        Ok(results)
    }
}

impl Drop for Snapshot {
    /// 快照销毁时自动从 SnapshotList 注销
    fn drop(&mut self) {
        self.snapshot_list.unregister(self.sequence, self.token);
    }
}
