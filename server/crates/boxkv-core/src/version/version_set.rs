use crate::sstable::InternalKey;
use boxkv_common::time::current_timestamp_secs;
use bytes::Bytes;
use parking_lot::{Mutex, RwLock};
use std::cmp::Ordering;
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, Weak};
use thiserror::Error;
use tracing::debug;

use super::VersionEdit;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct VersionId(pub u64);

#[derive(Debug, Error)]
pub enum VersionError {
    #[error("invalid level count {0}")]
    InvalidLevelCount(u32),

    #[error("invalid level index {level}, max_levels {max_levels}")]
    InvalidLevel { level: u32, max_levels: u32 },

    #[error("overlapping files in level {level} at index {index}")]
    OverlappingFiles { level: u32, index: usize },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileMeta {
    pub file_number: u64,
    pub level: u32,
    pub size_bytes: u64,
    pub smallest: InternalKey,
    pub largest: InternalKey,
    pub num_entries: u64,
    pub num_deletions: u64,
    pub creation_time_unix: u64,
}

impl FileMeta {
    pub fn new(
        file_number: u64,
        level: u32,
        size_bytes: u64,
        smallest: InternalKey,
        largest: InternalKey,
        num_entries: u64,
        num_deletions: u64,
    ) -> Self {
        let creation_time_unix = current_timestamp_secs();
        Self {
            file_number,
            level,
            size_bytes,
            smallest,
            largest,
            num_entries,
            num_deletions,
            creation_time_unix,
        }
    }
}

#[derive(Clone, Debug)]
pub struct LevelMetadata {
    pub level: u32,
    files: Vec<FileMeta>,
    pub total_size_bytes: u64,
}

impl LevelMetadata {
    /// 创建层级元数据
    pub fn new(level: u32, mut files: Vec<FileMeta>) -> Result<Self, VersionError> {
        for f in files.iter_mut() {
            f.level = level;
        }

        if level == 0 {
            // L0：按 file_number 升序排序，最老文件在前
            files.sort_by(|a, b| a.file_number.cmp(&b.file_number));
        } else {
            files.sort_by(|a, b| a.smallest.cmp(&b.smallest));
            for i in 1..files.len() {
                let prev = &files[i - 1];
                let curr = &files[i];
                if prev.largest.cmp(&curr.smallest) != Ordering::Less {
                    return Err(VersionError::OverlappingFiles { level, index: i });
                }
            }
        }

        let total_size_bytes = files.iter().map(|f| f.size_bytes).sum();

        Ok(Self {
            level,
            files,
            total_size_bytes,
        })
    }

    pub fn empty(level: u32) -> Self {
        Self {
            level,
            files: Vec::new(),
            total_size_bytes: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.files.len()
    }

    pub fn is_empty(&self) -> bool {
        self.files.is_empty()
    }

    pub fn iter(&self) -> std::slice::Iter<'_, FileMeta> {
        self.files.iter()
    }

    pub fn files(&self) -> &Vec<FileMeta> {
        &self.files
    }

    pub fn add_file(&mut self, mut file: FileMeta) -> Result<(), VersionError> {
        file.level = self.level;
        if self.level == 0 {
            let idx = self
                .files
                .binary_search_by(|f| f.file_number.cmp(&file.file_number))
                .unwrap_or_else(|pos| pos);

            self.total_size_bytes = self.total_size_bytes.saturating_add(file.size_bytes);
            self.files.insert(idx, file);
            return Ok(());
        }

        let idx = self
            .files
            .binary_search_by(|f| f.smallest.cmp(&file.smallest))
            .unwrap_or_else(|pos| pos);

        if idx > 0 {
            let prev = &self.files[idx - 1];
            if prev.largest.cmp(&file.smallest) != Ordering::Less {
                return Err(VersionError::OverlappingFiles {
                    level: self.level,
                    index: idx,
                });
            }
        }

        if idx < self.files.len() {
            let next = &self.files[idx];
            if file.largest.cmp(&next.smallest) != Ordering::Less {
                return Err(VersionError::OverlappingFiles {
                    level: self.level,
                    index: idx,
                });
            }
        }

        self.total_size_bytes = self.total_size_bytes.saturating_add(file.size_bytes);
        self.files.insert(idx, file);
        Ok(())
    }

    pub fn remove_file(&mut self, file_number: u64) {
        if let Some(pos) = self.files.iter().position(|f| f.file_number == file_number) {
            let sz = self.files[pos].size_bytes;
            self.total_size_bytes = self.total_size_bytes.saturating_sub(sz);
            self.files.remove(pos);
        }
    }

    /// 根据下标范围获取文件切片
    pub fn slice(&self, range: std::ops::Range<usize>) -> &[FileMeta] {
        &self.files[range]
    }

    /// 根据下标列表获取文件集合
    pub fn gather(&self, indices: &[usize]) -> Vec<FileMeta> {
        indices.iter().map(|&i| self.files[i].clone()).collect()
    }

    /// 在有序层（L1+）中查找包含指定 key 的唯一文件
    pub fn find_file(&self, key: &Bytes) -> Option<&FileMeta> {
        if self.level == 0 {
            // L0 层文件可能重叠，不支持单文件查找
            return None;
        }

        if self.files.is_empty() {
            return None;
        }

        // 二分查找：找到第一个 smallest <= key 的文件
        let mut left = 0;
        let mut right = self.files.len();

        while left < right {
            let mid = (left + right) / 2;
            if self.files[mid].smallest.user_key().as_ref() <= key.as_ref() {
                left = mid + 1;
            } else {
                right = mid;
            }
        }

        // left 指向第一个 smallest > key 的位置，需要检查 left-1
        if left == 0 {
            return None;
        }

        let candidate = &self.files[left - 1];

        // 检查 key 是否在 [smallest, largest] 范围内
        if candidate.smallest.user_key().as_ref() <= key.as_ref()
            && key.as_ref() <= candidate.largest.user_key().as_ref()
        {
            Some(candidate)
        } else {
            None
        }
    }

    /// 轻量级重叠查询：返回下标范围/列表 + 统计信息，不 clone 文件
    pub fn overlap_stats(&self, begin: &Bytes, end: &Bytes) -> (Vec<usize>, u64) {
        if self.files.is_empty() {
            return (Vec::new(), 0);
        }

        if self.level == 0 {
            // L0：线性扫描，返回下标列表 + 总字节数
            let mut indices = Vec::new();
            let mut total_bytes = 0u64;
            for (i, f) in self.files.iter().enumerate() {
                let fs = f.smallest.user_key();
                let fl = f.largest.user_key();
                if !(fs > end.as_ref() || fl < begin.as_ref()) {
                    indices.push(i);
                    total_bytes += f.size_bytes;
                }
            }
            (indices, total_bytes)
        } else {
            // L1+：二分查找，返回连续区间 + 总字节数
            let mut left = 0usize;
            let mut right = self.files.len();
            while left < right {
                let mid = (left + right) / 2;
                if self.files[mid].smallest.user_key().as_ref() <= begin.as_ref() {
                    left = mid + 1;
                } else {
                    right = mid;
                }
            }

            let start = left.saturating_sub(1);
            let mut end_idx = start;
            let mut total_bytes = 0u64;
            while end_idx < self.files.len() {
                let f = &self.files[end_idx];
                let fs = f.smallest.user_key();
                if fs.as_ref() > end.as_ref() {
                    break;
                }

                let fl = f.largest.user_key();
                if !(fs > end.as_ref() || fl < begin.as_ref()) {
                    total_bytes += f.size_bytes;
                }

                end_idx += 1;
            }

            // 返回连续区间的下标列表
            let indices: Vec<usize> = (start..end_idx).collect();
            (indices, total_bytes)
        }
    }

    /// 返回与 [begin, end]（闭区间）有重叠的文件集合。
    /// L1+ 层利用“按 smallest 有序 + 不重叠”的性质进行二分；
    /// L0 层采用线性扫描
    pub fn overlap(&self, begin: &Bytes, end: &Bytes) -> Vec<FileMeta> {
        let (indices, _) = self.overlap_stats(begin, end);
        self.gather(&indices)
    }
}

#[derive(Clone)]
pub struct Version {
    inner: Arc<VersionInner>,
}

impl Version {
    pub fn id(&self) -> VersionId {
        self.inner.id
    }

    pub fn num_levels(&self) -> u32 {
        self.inner.num_levels
    }

    pub fn total_size_bytes(&self) -> u64 {
        self.inner.total_size_bytes
    }

    pub fn level(&self, level: u32) -> Option<&LevelMetadata> {
        self.inner.levels.get(level as usize)
    }

    pub fn levels(&self) -> &[LevelMetadata] {
        &self.inner.levels
    }

    /// 创建当前版本的快照
    pub fn build_snapshot(&self) -> VersionEdit {
        let mut edit = VersionEdit::default();
        for level_meta in &self.inner.levels {
            for file in level_meta.iter() {
                edit.add_file(level_meta.level, file.clone());
            }
        }
        edit
    }
}

#[derive(Debug)]
struct VersionInner {
    id: VersionId,
    num_levels: u32,
    levels: Vec<LevelMetadata>,
    total_size_bytes: u64,
}

impl VersionInner {
    fn new_empty(id: VersionId, num_levels: u32) -> Result<Self, VersionError> {
        if num_levels == 0 {
            return Err(VersionError::InvalidLevelCount(num_levels));
        }

        let mut levels = Vec::with_capacity(num_levels as usize);
        for l in 0..num_levels {
            levels.push(LevelMetadata::empty(l));
        }

        Ok(Self {
            id,
            num_levels,
            levels,
            total_size_bytes: 0,
        })
    }

    fn apply_edit(&self, edit: &VersionEdit, id: VersionId) -> Result<Self, VersionError> {
        let mut levels = self.levels.clone();

        for (level, file_number) in edit.deleted_files() {
            let level_idx = *level as usize;
            if level_idx >= levels.len() {
                return Err(VersionError::InvalidLevel {
                    level: *level,
                    max_levels: self.num_levels,
                });
            }
            levels[level_idx].remove_file(*file_number);
        }

        for (level, file_meta) in edit.new_files() {
            let level_idx = *level as usize;
            if level_idx >= levels.len() {
                return Err(VersionError::InvalidLevel {
                    level: *level,
                    max_levels: self.num_levels,
                });
            }
            levels[level_idx].add_file(file_meta.clone())?;
        }

        let total_size_bytes = levels.iter().map(|l| l.total_size_bytes).sum();

        Ok(Self {
            id,
            num_levels: self.num_levels,
            levels,
            total_size_bytes,
        })
    }
}

/// VersionSet：管理所有 Version 的生命周期
pub struct VersionSet {
    /// 当前最新 Version
    current: RwLock<Arc<VersionInner>>,
    /// 所有存活 Version 的弱引用列表（用于追踪文件生命周期）
    versions: Mutex<Vec<Weak<VersionInner>>>,
    num_levels: u32,
    next_version_id: AtomicU64,
    next_file_number: AtomicU64,
    last_sequence: AtomicU64,
    log_number: AtomicU64,
}

impl VersionSet {
    pub fn new(
        num_levels: u32,
        next_file_number: u64,
        last_sequence: u64,
    ) -> Result<Self, VersionError> {
        let initial_id = VersionId(0);
        let inner = VersionInner::new_empty(initial_id, num_levels)?;
        let arc_inner = Arc::new(inner);

        // 初始化 Version 链表，包含初始 Version 的弱引用
        let versions = Mutex::new(vec![Arc::downgrade(&arc_inner)]);

        Ok(Self {
            current: RwLock::new(arc_inner),
            versions,
            num_levels,
            next_version_id: AtomicU64::new(1),
            next_file_number: AtomicU64::new(next_file_number),
            last_sequence: AtomicU64::new(last_sequence),
            log_number: AtomicU64::new(0),
        })
    }

    pub fn num_levels(&self) -> u32 {
        self.num_levels
    }

    pub fn current(&self) -> Version {
        let guard = self.current.read();
        Version {
            inner: guard.clone(),
        }
    }

    /// 应用 VersionEdit，生成新的 Version
    pub fn apply_edit(&self, edit: &VersionEdit) -> Result<Version, VersionError> {
        let mut guard = self.current.write();
        let base = guard.clone();
        let old_id = base.id;
        let new_id = VersionId(self.next_version_id.fetch_add(1, AtomicOrdering::SeqCst));
        let new_inner = base.apply_edit(edit, new_id)?;

        if let Some(seq) = edit.new_last_sequence() {
            self.last_sequence.store(seq, AtomicOrdering::SeqCst);
        }

        if let Some(next_file) = edit.new_next_file_number() {
            self.next_file_number
                .store(next_file, AtomicOrdering::SeqCst);
        }

        if let Some(log) = edit.log_number() {
            self.log_number.store(log, AtomicOrdering::SeqCst);
        }

        let new_arc = Arc::new(new_inner);

        {
            let mut versions = self.versions.lock();
            versions.push(Arc::downgrade(&new_arc));
            debug!(
                target: "version_set",
                "apply_edit: old_version={} new_version={} total_versions={}",
                old_id.0, new_id.0, versions.len()
            );
        }

        *guard = new_arc.clone();

        Ok(Version { inner: new_arc })
    }

    pub fn allocate_file_number(&self) -> u64 {
        self.next_file_number.fetch_add(1, AtomicOrdering::SeqCst)
    }

    pub fn next_file_number(&self) -> u64 {
        self.next_file_number.load(AtomicOrdering::SeqCst)
    }

    pub fn last_sequence(&self) -> u64 {
        self.last_sequence.load(AtomicOrdering::SeqCst)
    }

    pub fn set_last_sequence(&self, sequence: u64) {
        self.last_sequence.store(sequence, AtomicOrdering::SeqCst);
    }

    pub fn log_number(&self) -> u64 {
        self.log_number.load(AtomicOrdering::SeqCst)
    }

    pub fn set_log_number(&self, log_number: u64) {
        self.log_number.store(log_number, AtomicOrdering::SeqCst);
    }

    /// 收集所有存活 Version 引用的文件号集合
    pub fn collect_live_files(&self) -> HashSet<u64> {
        let mut live_files = HashSet::new();
        let versions = self.versions.lock();

        for weak_ver in versions.iter() {
            // 尝试升级弱引用：如果成功，说明该 Version 仍被某处持有
            if let Some(arc_ver) = weak_ver.upgrade() {
                for level_meta in &arc_ver.levels {
                    for file in level_meta.iter() {
                        live_files.insert(file.file_number);
                    }
                }
            }
        }

        debug!(
            target: "version_set",
            "collect_live_files: total_live_files={}",
            live_files.len()
        );

        live_files
    }

    /// 清理已经没有任何引用的 Version
    pub fn cleanup_obsolete_versions(&self) -> usize {
        let mut versions = self.versions.lock();
        let original_count = versions.len();

        // 保留所有仍然存活的 Version（Weak::upgrade 成功）
        versions.retain(|weak_ver| weak_ver.upgrade().is_some());

        let removed_count = original_count - versions.len();
        if removed_count > 0 {
            debug!(
                target: "version_set",
                "cleanup_obsolete_versions: removed={} remaining={}",
                removed_count,
                versions.len()
            );
        }

        removed_count
    }

    /// 获取当前存活的 Version 数量
    pub fn num_alive_versions(&self) -> usize {
        let versions = self.versions.lock();
        versions.iter().filter(|w| w.upgrade().is_some()).count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn ik(user_key: &str, seq: u64) -> InternalKey {
        InternalKey::new(Bytes::from(user_key.as_bytes().to_vec()), seq)
    }

    #[test]
    fn level_metadata_non_overlapping() {
        let f1 = FileMeta::new(1, 1, 10, ik("a", 100), ik("m", 90), 0, 0);
        let f2 = FileMeta::new(2, 1, 20, ik("n", 100), ik("z", 90), 0, 0);
        let level = LevelMetadata::new(1, vec![f2, f1]).unwrap();
        assert_eq!(level.len(), 2);
        let nums: Vec<u64> = level.iter().map(|f| f.file_number).collect();
        assert_eq!(nums[0], 1);
        assert_eq!(nums[1], 2);
    }

    #[test]
    fn level_metadata_overlapping_detected() {
        let f1 = FileMeta::new(1, 1, 10, ik("a", 100), ik("m", 90), 0, 0);
        let f2 = FileMeta::new(2, 1, 20, ik("k", 100), ik("z", 90), 0, 0);
        let res = LevelMetadata::new(1, vec![f1, f2]);
        assert!(matches!(res, Err(VersionError::OverlappingFiles { .. })));
    }

    #[test]
    fn version_set_apply_edit_add_and_delete() {
        let vs = VersionSet::new(3, 100, 0).unwrap();
        let mut edit = VersionEdit::default();

        let f1 = FileMeta::new(1, 1, 10, ik("a", 5), ik("m", 4), 100, 0);
        edit.add_file(1, f1.clone());
        let f2 = FileMeta::new(2, 2, 20, ik("n", 5), ik("z", 4), 200, 10);
        edit.add_file(2, f2.clone());
        edit.set_last_sequence(123);
        edit.set_next_file_number(200);

        let v1 = vs.apply_edit(&edit).unwrap();
        assert_eq!(v1.num_levels(), 3);
        assert_eq!(vs.last_sequence(), 123);
        assert_eq!(vs.allocate_file_number(), 200);

        let level1 = v1.level(1).unwrap();
        assert_eq!(level1.len(), 1);
        assert_eq!(level1.iter().next().unwrap().file_number, f1.file_number);

        let level2 = v1.level(2).unwrap();
        assert_eq!(level2.len(), 1);
        assert_eq!(level2.iter().next().unwrap().file_number, f2.file_number);

        let mut edit2 = VersionEdit::default();
        edit2.delete_file(1, f1.file_number);
        let v2 = vs.apply_edit(&edit2).unwrap();
        let level1_v2 = v2.level(1).unwrap();
        assert!(level1_v2.is_empty());
    }
}
