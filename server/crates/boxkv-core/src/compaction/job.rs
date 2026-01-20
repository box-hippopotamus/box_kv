use std::io::{Error as IoError, ErrorKind};
use std::path::Path;
use std::sync::Arc;
use std::thread;

use bytes::Bytes;
use tracing::{debug, info, trace};

use crate::sstable::{InternalKey, SSTableIterator};
use crate::sstable::{SSTableBuilder, SSTableContext, SSTableReader};
use crate::version::{FileMeta, Version, VersionEdit, VersionSet};
use boxkv_common::codec::Encode;

use super::merge::{MergeHeap, MergeSource};
use super::types::{CompactionError, CompactionPlan, TablePathProvider, VersionCommit};
use boxkv_common::config::CompactionConfig;

/// 完成当前输出文件并生成元数据
fn flush_output_builder(
    plan: &CompactionPlan,
    cur_builder: &mut Option<(u64, SSTableBuilder)>,
    outputs: &mut Vec<FileMeta>,
    cur_deletions: &mut u64,
    approx_written: &mut u64,
) -> Result<(), CompactionError> {
    if let Some((file_no, builder)) = cur_builder.take() {
        let meta = builder.finish()?;
        let deletions = *cur_deletions;
        let meta_f = FileMeta::new(
            file_no,
            plan.target_level,
            meta.file_size,
            InternalKey::new(meta.min_key.clone(), u64::MAX),
            InternalKey::new(meta.max_key.clone(), 0),
            meta.entry_count as u64,
            deletions,
        );
        debug!(
            target: "compaction_job",
            "output file created file_no={} level={} size={} entries={} deletions={}",
            file_no,
            plan.target_level,
            meta.file_size,
            meta.entry_count,
            deletions
        );
        outputs.push(meta_f);
        *cur_deletions = 0;
        *approx_written = 0;
    }
    Ok(())
}

fn create_output_builder(
    path_provider: &dyn TablePathProvider,
    version_set: &VersionSet,
    ctx: &SSTableContext,
    target_level: u32,
) -> Result<(u64, SSTableBuilder), CompactionError> {
    let file_no = version_set.allocate_file_number();
    let file_path = path_provider.sst_path(file_no);
    let builder = SSTableBuilder::create(Path::new(&file_path), ctx, target_level as i32)?;
    Ok((file_no, builder))
}

/// 执行一次 Compaction
///
/// 打开输入文件，多路归并，应用删除/TTL/快照语义，生成输出文件
pub fn execute_compaction(
    cfg: &CompactionConfig,
    ctx: &SSTableContext,
    path_provider: &dyn TablePathProvider,
    version_set: &VersionSet,
    plan: &CompactionPlan,
    now_secs: u64,
    min_snapshot_seq: u64,
    commit: &dyn VersionCommit,
) -> Result<Version, CompactionError> {
    // Trivial move: 无重叠时直接移动文件
    if let Some(v) = try_trivial_move(version_set, plan, commit)? {
        info!(target: "compaction_job", "trivial-move L{} -> L{} files={}", plan.level, plan.target_level, plan.inputs_level.len());
        return Ok(v);
    }
    // 打开输入文件并构建迭代器
    let mut iters = Vec::new();
    for f in plan
        .inputs_level
        .iter()
        .chain(plan.inputs_next_level.iter())
    {
        let path = path_provider.sst_path(f.file_number);
        let reader = SSTableReader::open(Path::new(&path), ctx)?;
        let reader = Arc::new(reader);
        let mut iter = SSTableIterator::new(reader, None, None)?;
        if let Some(e) = iter.next()? {
            iters.push(MergeSource {
                iter,
                current: Some(e),
            });
        }
    }

    if iters.is_empty() {
        let edit = VersionEdit::default();
        return commit.commit(&edit);
    }

    // 计算 bottommost 和目标文件大小
    let bottommost = plan.target_level + 1 >= cfg.max_levels;
    let target_file_bytes = cfg.target_file_size_bytes(plan.target_level);
    info!(target: "compaction_job", "start L{} -> L{} inputs_k={} inputs_k+1={} bottommost={} target_bytes={}",
        plan.level, plan.target_level, plan.inputs_level.len(), plan.inputs_next_level.len(), bottommost, target_file_bytes);

    // 初始化多路归并堆
    let mut heap = MergeHeap::new(iters);

    // 输出构建上下文
    let mut outputs: Vec<FileMeta> = Vec::new();
    let mut cur_builder: Option<(u64, SSTableBuilder)> = None;
    let mut cur_deletions: u64 = 0;
    let mut approx_written: u64 = 0;
    let mut last_user_key: Option<Bytes> = None;

    // 主归并循环
    while let Some(e) = heap.pop() {
        let src_idx = e.src_idx;

        let is_new_key = match &last_user_key {
            None => true,
            Some(prev) => prev.as_ref() != e.key.as_ref(),
        };

        if is_new_key {
            // 新 key: 判断是否保留
            let keep;
            let is_tombstone = e.value.is_tombstone();
            let is_expired = match &e.value {
                boxkv_common::types::ValueType::Expiring { expire_at, .. } => {
                    *expire_at <= now_secs
                }
                _ => false,
            };

            if is_tombstone {
                keep = !bottommost;
            } else if is_expired {
                keep = e.sequence >= min_snapshot_seq;
            } else {
                keep = true;
            }

            if keep {
                // 创建 builder
                if cur_builder.is_none() {
                    cur_builder = Some(create_output_builder(
                        path_provider,
                        version_set,
                        ctx,
                        plan.target_level,
                    )?);
                }
                // 文件达到目标大小时旋转
                if approx_written >= target_file_bytes {
                    flush_output_builder(
                        plan,
                        &mut cur_builder,
                        &mut outputs,
                        &mut cur_deletions,
                        &mut approx_written,
                    )?;
                    let (file_no, builder) =
                        create_output_builder(path_provider, version_set, ctx, plan.target_level)?;
                    cur_builder = Some((file_no, builder));
                    trace!(
                        target: "compaction_job",
                        "rotate new output file file_no={}",
                        file_no
                    );
                }

                // 写入 entry
                let (_file_no, builder) = cur_builder.as_mut().ok_or_else(|| {
                    CompactionError::Io(IoError::new(
                        ErrorKind::Other,
                        "missing output builder during compaction",
                    ))
                })?;
                builder.add(&boxkv_common::types::Entry {
                    key: e.key.clone(),
                    value: e.value.clone(),
                    sequence: e.sequence,
                })?;

                // 更新统计
                if is_tombstone {
                    cur_deletions += 1;
                }
                approx_written += (e.key.len() + e.value.encoded_len()) as u64;
            }

            last_user_key = Some(e.key.clone());
            heap.push_next_from_source(src_idx)?;
            // 丢弃旧版本
            heap.discard_same_user_key(e.key.as_ref())?;
        } else {
            heap.push_next_from_source(src_idx)?;
        }
    }

    flush_output_builder(
        plan,
        &mut cur_builder,
        &mut outputs,
        &mut cur_deletions,
        &mut approx_written,
    )?;

    let mut edit = VersionEdit::default();
    for f in &plan.inputs_level {
        edit.delete_file(plan.level, f.file_number);
    }
    for f in &plan.inputs_next_level {
        edit.delete_file(plan.target_level, f.file_number);
    }
    for f in &outputs {
        edit.add_file(plan.target_level, f.clone());
    }
    edit.set_next_file_number(version_set.next_file_number());
    edit.set_last_sequence(version_set.last_sequence());

    let out_bytes: u64 = outputs.iter().map(|f| f.size_bytes).sum();
    let v = commit.commit(&edit)?;
    info!(target: "compaction_job", "commit L{} -> L{} outputs={} bytes_out={}", plan.level, plan.target_level, outputs.len(), out_bytes);
    Ok(v)
}

/// 判断是否可 trivial move
fn can_trivial_move(plan: &CompactionPlan) -> bool {
    if plan.inputs_next_level.is_empty() {
        if plan.level == 0 {
            return plan.inputs_level.len() == 1;
        } else {
            return !plan.inputs_level.is_empty();
        }
    }
    false
}

/// 尝试 trivial move
fn try_trivial_move(
    vs: &VersionSet,
    plan: &CompactionPlan,
    commit: &dyn VersionCommit,
) -> Result<Option<Version>, CompactionError> {
    if !can_trivial_move(plan) {
        return Ok(None);
    }
    let mut edit = VersionEdit::default();
    for f in &plan.inputs_level {
        edit.delete_file(plan.level, f.file_number);
    }

    for f in &plan.inputs_level {
        let moved = FileMeta::new(
            f.file_number,
            plan.target_level,
            f.size_bytes,
            f.smallest.clone(),
            f.largest.clone(),
            f.num_entries,
            f.num_deletions,
        );
        edit.add_file(plan.target_level, moved);
    }
    edit.set_next_file_number(vs.next_file_number());
    edit.set_last_sequence(vs.last_sequence());
    let v = commit.commit(&edit)?;
    debug!(target: "compaction_job", "trivial-move committed L{} -> L{} files_moved={}", plan.level, plan.target_level, plan.inputs_level.len());
    Ok(Some(v))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn ik(k: &str, s: u64) -> crate::sstable::InternalKey {
        crate::sstable::InternalKey::new(Bytes::from(k.as_bytes().to_vec()), s)
    }

    struct MockPath;
    impl TablePathProvider for MockPath {
        fn sst_path(&self, _file_number: u64) -> std::path::PathBuf {
            std::env::temp_dir().join("mock.sst")
        }
    }

    struct MockCommit {
        vs: std::sync::Arc<VersionSet>,
    }
    impl VersionCommit for MockCommit {
        fn commit(&self, edit: &VersionEdit) -> Result<Version, CompactionError> {
            let v = self.vs.apply_edit(edit)?;
            Ok(v)
        }
    }

    #[test]
    fn trivial_move_level1_to_level2() {
        let vs = VersionSet::new(3, 100, 0).unwrap();
        // Add one file in L1, no overlap in L2
        let mut edit = VersionEdit::default();
        let f = FileMeta::new(1, 1, 10, ik("a", 5), ik("m", 1), 10, 0);
        edit.add_file(1, f.clone());
        let _ = vs.apply_edit(&edit).unwrap();

        let plan = CompactionPlan {
            level: 1,
            target_level: 2,
            inputs_level: vec![f.clone()],
            inputs_next_level: vec![],
            smallest: Bytes::copy_from_slice(f.smallest.user_key()),
            largest: Bytes::copy_from_slice(f.largest.user_key()),
            reason: super::super::types::CompactionReason::LevelScore {
                level: 1,
                score: 2.0,
            },
        };

        let cfg = CompactionConfig::default();
        let ctx = crate::sstable::SSTableContext::minimal();
        let path = MockPath;
        let commit = MockCommit {
            vs: std::sync::Arc::new(vs),
        };
        // Should take trivial move path and not try to read/write files
        let res = execute_compaction(&cfg, &ctx, &path, &commit.vs, &plan, 0, 0, &commit).unwrap();
        // Validate moved
        let lv2 = res.level(2).unwrap();
        assert_eq!(lv2.len(), 1);
        assert_eq!(lv2.iter().next().unwrap().file_number, f.file_number);
        let lv1 = res.level(1).unwrap();
        assert!(lv1.is_empty());
    }
}

// ----------------------
// Subcompaction job (parallel)
// ----------------------

/// 为指定范围创建迭代器
fn build_iters_for_range(
    ctx: &SSTableContext,
    path_provider: &dyn TablePathProvider,
    plan: &CompactionPlan,
    start: Option<Bytes>,
    end: Option<Bytes>,
) -> Result<Vec<MergeSource>, CompactionError> {
    let mut iters = Vec::new();
    for f in plan
        .inputs_level
        .iter()
        .chain(plan.inputs_next_level.iter())
    {
        let path = path_provider.sst_path(f.file_number);
        let reader = SSTableReader::open(Path::new(&path), ctx)?;
        let reader = std::sync::Arc::new(reader);
        let mut iter = crate::sstable::SSTableIterator::new(reader, start.clone(), end.clone())?;
        if let Some(e) = iter.next()? {
            iters.push(MergeSource {
                iter,
                current: Some(e),
            });
        }
    }
    Ok(iters)
}

/// 在子区间内执行合并
fn compact_collect_outputs(
    cfg: &CompactionConfig,
    ctx: &SSTableContext,
    path_provider: &dyn TablePathProvider,
    version_set: &VersionSet,
    plan: &CompactionPlan,
    now_secs: u64,
    min_snapshot_seq: u64,
    start: Option<Bytes>,
    end: Option<Bytes>,
) -> Result<Vec<FileMeta>, CompactionError> {
    // 构建迭代器
    let iters = build_iters_for_range(ctx, path_provider, plan, start, end)?;
    if iters.is_empty() {
        return Ok(Vec::new());
    }

    // 初始化
    let bottommost = plan.target_level + 1 >= cfg.max_levels;
    let target_file_bytes = cfg.target_file_size_bytes(plan.target_level);
    let mut heap = MergeHeap::new(iters);
    let mut outputs: Vec<FileMeta> = Vec::new();
    let mut cur_builder: Option<(u64, SSTableBuilder)> = None;
    let mut cur_deletions: u64 = 0;
    let mut approx_written: u64 = 0;
    let mut last_user_key: Option<Bytes> = None;

    // 归并循环
    while let Some(e) = heap.pop() {
        let src_idx = e.src_idx;

        let is_new_key = match &last_user_key {
            None => true,
            Some(prev) => prev.as_ref() != e.key.as_ref(),
        };
        if is_new_key {
            // 判断是否保留
            let keep;
            let is_tombstone = e.value.is_tombstone();
            let is_expired = match &e.value {
                boxkv_common::types::ValueType::Expiring { expire_at, .. } => {
                    *expire_at <= now_secs
                }
                _ => false,
            };
            if is_tombstone {
                keep = !bottommost;
            } else if is_expired {
                keep = e.sequence >= min_snapshot_seq;
            } else {
                keep = true;
            }
            if keep {
                // 创建 builder
                if cur_builder.is_none() {
                    cur_builder = Some(create_output_builder(
                        path_provider,
                        version_set,
                        ctx,
                        plan.target_level,
                    )?);
                }
                // 旋转输出
                if approx_written >= target_file_bytes {
                    flush_output_builder(
                        plan,
                        &mut cur_builder,
                        &mut outputs,
                        &mut cur_deletions,
                        &mut approx_written,
                    )?;
                    let (file_no, builder) =
                        create_output_builder(path_provider, version_set, ctx, plan.target_level)?;
                    cur_builder = Some((file_no, builder));
                }
                // 写入
                let (_file_no, builder) = cur_builder.as_mut().ok_or_else(|| {
                    CompactionError::Io(IoError::new(
                        ErrorKind::Other,
                        "missing output builder during subcompaction",
                    ))
                })?;
                builder.add(&boxkv_common::types::Entry {
                    key: e.key.clone(),
                    value: e.value.clone(),
                    sequence: e.sequence,
                })?;
                if is_tombstone {
                    cur_deletions += 1;
                }
                approx_written += (e.key.len() + e.value.encoded_len()) as u64;
            }
            last_user_key = Some(e.key.clone());
            heap.push_next_from_source(src_idx)?;
            // 丢弃旧版本
            heap.discard_same_user_key(e.key.as_ref())?;
        } else {
            heap.push_next_from_source(src_idx)?;
        }
    }
    // 完成最后一个文件
    flush_output_builder(
        plan,
        &mut cur_builder,
        &mut outputs,
        &mut cur_deletions,
        &mut approx_written,
    )?;
    Ok(outputs)
}

/// 采样划分子区间
fn sample_partition_boundaries(plan: &CompactionPlan, max_parts: usize) -> Vec<Bytes> {
    let mut b: Vec<Bytes> = Vec::new();
    for f in plan
        .inputs_level
        .iter()
        .chain(plan.inputs_next_level.iter())
    {
        b.push(f.smallest.user_key().clone());
        b.push(f.largest.user_key().clone());
    }
    b.sort();
    b.dedup_by(|a, b| a.as_ref() == b.as_ref());
    if b.len() <= 2 || max_parts <= 1 {
        return Vec::new();
    }
    let mut cuts = Vec::new();
    let step = ((b.len() - 1) as f64 / max_parts as f64).ceil() as usize;
    let mut i = step;
    while i < b.len() - 1 && cuts.len() + 1 < max_parts {
        cuts.push(b[i].clone());
        i += step;
    }
    cuts
}

/// 执行 Compaction（并行子分区）
pub fn execute_compaction_sub(
    cfg: &CompactionConfig,
    ctx: &SSTableContext,
    path_provider: Arc<dyn TablePathProvider>,
    version_set: Arc<VersionSet>,
    plan: &CompactionPlan,
    now_secs: u64,
    min_snapshot_seq: u64,
    commit: &dyn VersionCommit,
) -> Result<Version, CompactionError> {
    let parts = cfg.max_subcompactions.max(1);
    if parts == 1 {
        return execute_compaction(
            cfg,
            ctx,
            path_provider.as_ref(),
            &version_set,
            plan,
            now_secs,
            min_snapshot_seq,
            commit,
        );
    }
    let cuts = sample_partition_boundaries(plan, parts);
    let mut ranges: Vec<(Option<Bytes>, Option<Bytes>)> = Vec::new();
    if cuts.is_empty() {
        ranges.push((None, None));
    } else {
        let mut last: Option<Bytes> = None;
        for c in cuts.into_iter() {
            ranges.push((last.clone(), Some(c.clone())));
            last = Some(c);
        }
        ranges.push((last, None));
    }

    let mut handles = Vec::new();
    let ctx_arc = ctx.clone();
    for (start, end) in ranges.into_iter() {
        let cfg_c = cfg.clone();
        let ctx_c = ctx_arc.clone();
        let plan_c = plan.clone();
        let path_c = path_provider.clone();
        let vs_c = version_set.clone();
        let start_c = start.clone();
        let end_c = end.clone();
        let h = thread::spawn(move || {
            trace!(target: "compaction_job", "sub-range start={:?} end={:?}", start_c, end_c);
            compact_collect_outputs(
                &cfg_c,
                &ctx_c,
                path_c.as_ref(),
                &vs_c,
                &plan_c,
                now_secs,
                min_snapshot_seq,
                start_c,
                end_c,
            )
        });
        handles.push(h);
    }

    let mut all_outputs: Vec<FileMeta> = Vec::new();
    for h in handles {
        let outs = h.join().map_err(|_| {
            CompactionError::Io(IoError::new(
                ErrorKind::Other,
                "subcompaction worker panicked",
            ))
        })?;
        let outs = outs?;
        all_outputs.extend(outs);
    }
    all_outputs.sort_by(|a, b| a.smallest.user_key().cmp(b.smallest.user_key()));

    let mut edit = VersionEdit::default();
    for f in &plan.inputs_level {
        edit.delete_file(plan.level, f.file_number);
    }
    for f in &plan.inputs_next_level {
        edit.delete_file(plan.target_level, f.file_number);
    }
    for f in &all_outputs {
        edit.add_file(plan.target_level, f.clone());
    }
    edit.set_next_file_number(version_set.next_file_number());
    edit.set_last_sequence(version_set.last_sequence());
    let v = commit.commit(&edit)?;
    Ok(v)
}
