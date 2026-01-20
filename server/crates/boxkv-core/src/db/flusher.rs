use crate::compaction::types::{TablePathProvider, VersionCommit};
use crate::db::types::SuperVersion;
use crate::memtable::Memtable;
use crate::sequence::SequenceGenerator;
use crate::sstable::{InternalKey, SSTableBuilder, SSTableContext};
use crate::version::{FileMeta, VersionEdit, VersionSet};
use boxkv_common::types::Entry;
use bytes::Bytes;
use std::collections::VecDeque;
use std::path::Path;
/// Flush 后台任务模块
/// - 将不可变 Memtable 转换为 L0 SSTable
/// - 更新 VersionSet 和 Manifest
/// - Flush 后触发 Compaction
use std::sync::{Arc, Mutex};

/// Flush 一个 imm Memtable 到 L0 SSTable
/// - 返回：是否成功 Flush
pub fn flush_one_memtable<C, P>(
    memtable: Arc<Memtable>,
    versions: &VersionSet,
    path_provider: &P,
    ctx: &SSTableContext,
    wal_file_id: u64,
    commit: &C,
) -> bool
where
    C: VersionCommit,
    P: TablePathProvider,
{
    // 分配文件号
    let file_no = versions.allocate_file_number();
    let file_path = path_provider.sst_path(file_no);

    // 创建 SSTable Builder
    let mut builder = match SSTableBuilder::create(
        Path::new(&file_path),
        ctx,
        0, // L0
    ) {
        Ok(b) => b,
        Err(e) => {
            tracing::error!("flush: create builder failed: {:?}", e);
            return false;
        }
    };

    // 写入所有条目
    let mut num_deletions: u64 = 0;
    let mut min_key: Option<Bytes> = None;
    let mut max_key: Option<Bytes> = None;

    for (k, v, seq) in memtable.iter() {
        if min_key.is_none() {
            min_key = Some(k.clone());
        }
        max_key = Some(k.clone());

        if v.is_tombstone() {
            num_deletions += 1;
        }

        let e = Entry {
            key: k.clone(),
            value: v.clone(),
            sequence: seq,
        };

        if let Err(e) = builder.add(&e) {
            tracing::error!("flush: builder add failed: {:?}", e);
            return false;
        }
    }

    // 完成 SSTable 构建
    let meta = match builder.finish() {
        Ok(m) => m,
        Err(e) => {
            tracing::error!("flush: builder finish failed: {:?}", e);
            return false;
        }
    };

    // 构建 FileMeta
    let smallest = InternalKey::new(min_key.unwrap_or_else(|| Bytes::from_static(b"")), u64::MAX);
    let largest = InternalKey::new(max_key.unwrap_or_else(|| Bytes::from_static(b"")), 0);

    let fmeta = FileMeta::new(
        file_no,
        0, // level
        meta.file_size,
        smallest,
        largest,
        meta.entry_count as u64,
        num_deletions,
    );

    // 创建 VersionEdit
    let mut edit = VersionEdit::default();
    edit.add_file(0, fmeta);
    edit.set_last_sequence(versions.last_sequence());
    edit.set_next_file_number(versions.next_file_number());
    edit.set_log_number(wal_file_id);

    // 提交到 Manifest 和 VersionSet
    if let Err(e) = commit.commit(&edit) {
        tracing::error!("flush: commit failed: {:?}", e);
        return false;
    }

    true
}

/// 构建新的 SuperVersion
pub fn build_super_version(
    mem: Arc<Memtable>,
    imm: &Mutex<VecDeque<Arc<Memtable>>>,
    versions: &VersionSet,
    sequence: &SequenceGenerator,
) -> SuperVersion {
    let imm_vec = {
        let imm_guard = match imm.lock() {
            Ok(g) => g,
            Err(poisoned) => {
                tracing::error!("Immutable memtable lock poisoned during build_super_version");
                poisoned.into_inner()
            }
        };
        Arc::new(imm_guard.iter().cloned().collect::<Vec<_>>())
    };
    let version = Arc::new(versions.current().clone());
    let seq = sequence.current();

    SuperVersion {
        mem,
        imm: imm_vec,
        version,
        sequence: seq,
    }
}
