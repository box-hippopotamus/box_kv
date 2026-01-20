use super::types::{CompactionPlan, CompactionReason};
use crate::version::{FileMeta, LevelMetadata, Version};
use boxkv_common::config::CompactionConfig;
use bytes::Bytes;
use std::collections::HashSet;
use tracing::{trace, warn};

/// 选择一次压缩任务
///
/// 策略：
/// - L0 优先：达到触发阈值时强制 L0->L1
/// - 否则选择 score 最高的层 Lk->L(k+1)，并进行同层 clean-cut 扩展
pub fn pick_compaction(cfg: &CompactionConfig, current: &Version) -> Option<CompactionPlan> {
    // L0 触发：文件数达到阈值则立即压缩到 L1
    let l0 = current.level(0)?;
    if l0.len() >= cfg.level0_trigger {
        // 使用带预算约束的增量扩张策略选择 L0 输入
        let (inputs_level, inputs_next_level, smallest, largest) = pick_l0_inputs(cfg, current, l0);
        trace!(target: "compaction_picker", "L0 trigger -> L1: l0_files={}, overlaps_l1={}, range=[{:?}..{:?}]",
               inputs_level.len(), inputs_next_level.len(), smallest, largest);

        return Some(CompactionPlan {
            level: 0,
            target_level: 1,
            inputs_level,
            inputs_next_level,
            smallest,
            largest,
            reason: CompactionReason::L0Files(l0.len()),
        });
    }

    // Compute scores for levels >= 1 and < max_levels-1 (avoid compacting from last level)
    let mut best: Option<(u32, f64)> = None;
    for level in 1..cfg.max_levels.saturating_sub(1) {
        if let Some(meta) = current.level(level) {
            let used = meta.total_size_bytes;
            let tgt = cfg.target_level_bytes(level);
            let score = if tgt == 0 {
                0.0
            } else {
                used as f64 / tgt as f64
            };
            if score > 1.0 {
                match best {
                    None => best = Some((level, score)),
                    Some((_, s)) if score > s => best = Some((level, score)),
                    _ => {}
                }
            }
        }
    }

    let (level, score) = best?;
    let level_meta = current.level(level)?;
    if level_meta.is_empty() {
        return None;
    }

    // 以“tombstone 密度”优先作为种子，其次才按文件大小
    // - tombstone_ratio = num_deletions / (num_entries + num_deletions)
    // - 目标：优先压缩删除占比高的文件，加速空间回收
    let mut iter = level_meta.iter();
    let mut seed = iter.next().cloned()?;
    let ratio = |fm: &FileMeta| -> f64 {
        let denom = fm.num_entries.saturating_add(fm.num_deletions);
        if denom == 0 {
            0.0
        } else {
            (fm.num_deletions as f64) / (denom as f64)
        }
    };
    for f in iter {
        let r_new = ratio(f);
        let r_seed = ratio(&seed);
        if r_new > r_seed + f64::EPSILON
            || ((r_new - r_seed).abs() <= f64::EPSILON && f.size_bytes > seed.size_bytes)
        {
            seed = f.clone();
        }
    }

    let inputs_level = vec![seed.clone()];
    // Clean-cut 扩展：同层尽可能扩展到与下层重叠集合稳定后再确定输入
    let (inputs_level, inputs_next_level, smallest, largest) =
        expand_inputs_clean_cut(current, level, inputs_level);

    // enforce input size limits：若超限则缩小到 seed-only；仍超限则强制调度 seed-only
    let (exceeds, total_files, total_bytes, _fl, _bl) =
        check_input_limits(cfg, &inputs_level, &inputs_next_level);
    if exceeds {
        warn!(target: "compaction_picker", "L{} plan exceeds input limits: files={} bytes={} -> shrink to seed-only",
              level, total_files, total_bytes);
        // seed-only 方案
        let s_b = seed.smallest.user_key().clone();
        let l_b = seed.largest.user_key().clone();
        let inputs_level_b = vec![seed.clone()];
        let inputs_next_level_b = overlap_in_level(current, level + 1, &s_b, &l_b);
        let (exceeds_b, tf_b, tb_b, _flb, _blb) =
            check_input_limits(cfg, &inputs_level_b, &inputs_next_level_b);
        if exceeds_b {
            warn!(target: "compaction_picker", "L{} forced despite limits: files={} bytes={} (limits files={} bytes={})",
                  level, tf_b, tb_b, cfg.max_compaction_input_files, cfg.max_compaction_input_bytes);
        }
        return Some(CompactionPlan {
            level,
            target_level: level + 1,
            inputs_level: inputs_level_b,
            inputs_next_level: inputs_next_level_b,
            smallest: s_b,
            largest: l_b,
            reason: CompactionReason::LevelScore { level, score },
        });
    }

    trace!(target: "compaction_picker",
        "L{} -> L{} score={:.3} inputs_k={} overlaps_k+1={} range=[{:?}..{:?}]",
        level, level + 1, score, inputs_level.len(), inputs_next_level.len(), smallest, largest);

    Some(CompactionPlan {
        level,
        target_level: level + 1,
        inputs_level,
        inputs_next_level,
        smallest,
        largest,
        reason: CompactionReason::LevelScore { level, score },
    })
}

/// 计算文件集合的最小/最大 user key（闭区间），用于确定此次压缩的 key 范围
fn range_of_files(files: &[FileMeta]) -> (Bytes, Bytes) {
    if files.is_empty() {
        return (Bytes::new(), Bytes::new());
    }

    let mut sm = files[0].smallest.user_key();
    let mut lg = files[0].largest.user_key();
    for f in &files[1..] {
        let s = f.smallest.user_key();
        let l = f.largest.user_key();
        if s.as_ref() < sm.as_ref() {
            sm = s;
        }
        if l.as_ref() > lg.as_ref() {
            lg = l;
        }
    }

    (sm.clone(), lg.clone())
}

/// 在指定层级中查找与给定 [smallest, largest] 区间有重叠的文件
fn overlap_in_level(
    current: &Version,
    level: u32,
    smallest: &Bytes,
    largest: &Bytes,
) -> Vec<FileMeta> {
    match current.level(level) {
        None => vec![],
        Some(meta) => meta.overlap(smallest, largest),
    }
}

/// 计算 inputs_k 与 inputs_k+1 总文件数与字节数，并与配置上限对比
fn check_input_limits(
    cfg: &CompactionConfig,
    inputs_level: &[FileMeta],
    inputs_next_level: &[FileMeta],
) -> (bool, usize, u64, bool, bool) {
    let total_files = inputs_level.len() + inputs_next_level.len();
    let total_bytes: u64 = inputs_level.iter().map(|f| f.size_bytes).sum::<u64>()
        + inputs_next_level.iter().map(|f| f.size_bytes).sum::<u64>();
    let files_limited =
        cfg.max_compaction_input_files > 0 && total_files > cfg.max_compaction_input_files;
    let bytes_limited =
        cfg.max_compaction_input_bytes > 0 && total_bytes > cfg.max_compaction_input_bytes;
    (
        files_limited || bytes_limited,
        total_files,
        total_bytes,
        files_limited,
        bytes_limited,
    )
}

/// L0 输入选择：带预算约束的增量扩张策略
///
/// 策略说明：
/// 1. 选择起始文件（当前为最老文件 first()，L0 按 file_number 升序排序）
/// 2. 增量扩张：单次扫描 L0，逐个尝试加入与当前范围重叠的文件
/// 3. 预算检查：每次加入前验证是否超过 max_compaction_input_files/bytes
/// 4. 早停机制：一旦加入会超限，立即停止扩张，返回当前输入集合
/// 5. 前进性保证：即使只有起始文件，也返回有效计划（极端情况下强制调度）
fn pick_l0_inputs(
    cfg: &CompactionConfig,
    current: &Version,
    l0: &LevelMetadata,
) -> (Vec<FileMeta>, Vec<FileMeta>, Bytes, Bytes) {
    let base = if let Some(base) = l0.files().first() {
        // 选择起始文件：L0 按 file_number 升序排序，first() 是最老文件
        // 优先清理最老数据，推动数据下沉，减少版本堆积
        base
    } else {
        // 边界情况：L0 为空
        return (vec![], vec![], Bytes::new(), Bytes::new());
    };

    let mut inputs = vec![base.clone()];
    let mut smallest = base.smallest.user_key().clone();
    let mut largest = base.largest.user_key().clone();
    let mut inputs_bytes = base.size_bytes; // 累计输入字节数，避免重复 sum

    // 预算上限：0 表示无限制
    let budget_files = if cfg.max_compaction_input_files > 0 {
        cfg.max_compaction_input_files
    } else {
        usize::MAX
    };

    let budget_bytes = if cfg.max_compaction_input_bytes > 0 {
        cfg.max_compaction_input_bytes
    } else {
        u64::MAX
    };

    // 已选文件集合快速判重
    let mut selected: HashSet<u64> = HashSet::new();
    selected.insert(base.file_number);

    // 增量扩张：单次扫描 L0，O(n)
    for f in l0.files() {
        // 跳过已选文件
        if selected.contains(&f.file_number) {
            continue;
        }

        // 检查是否与当前范围重叠
        let fs = f.smallest.user_key();
        let fl = f.largest.user_key();
        if fs.as_ref() > largest.as_ref() || fl.as_ref() < smallest.as_ref() {
            continue; // 不重叠，跳过
        }

        // 试探性加入：计算新的范围和 L1 重叠
        let tentative_smallest = if fs.as_ref() < smallest.as_ref() {
            fs.clone()
        } else {
            smallest.clone()
        };

        let tentative_largest = if fl.as_ref() > largest.as_ref() {
            fl.clone()
        } else {
            largest.clone()
        };

        // 使用 overlap_stats 只获取统计信息，避免 clone
        let (l1_indices, l1_bytes) = if let Some(l1_meta) = current.level(1) {
            l1_meta.overlap_stats(&tentative_smallest, &tentative_largest)
        } else {
            (Vec::new(), 0)
        };

        // 预算检查：加入 f 后是否超限（累计计算，避免 O(n²)）
        let total_files = inputs.len() + 1 + l1_indices.len();
        let total_bytes = inputs_bytes + f.size_bytes + l1_bytes;

        if total_files > budget_files || total_bytes > budget_bytes {
            // 超限，跳过此文件，继续尝试后续文件（可能更小/更合适）
            continue;
        }

        // 通过预算检查，加入 f 并更新范围和累计字节数
        inputs.push(f.clone());
        selected.insert(f.file_number);
        inputs_bytes += f.size_bytes;
        smallest = tentative_smallest;
        largest = tentative_largest;
    }

    // 计算最终的 L1 重叠集合（确定计划后再获取完整文件）
    let overlaps_l1 = overlap_in_level(current, 1, &smallest, &largest);

    // 最终预算检查（防御性验证）
    let (exceeds, total_files, total_bytes, _fl, _bl) =
        check_input_limits(cfg, &inputs, &overlaps_l1);
    if exceeds {
        // 极端情况：扩张后的计划仍超限（L1 重叠过大）
        // 强制回退到 base-only，保证前进性
        let base_only = vec![base.clone()];
        let bs = base.smallest.user_key().clone();
        let bl = base.largest.user_key().clone();

        // base-only 的 L1 重叠统计
        let (base_l1_indices, base_l1_bytes) = if let Some(l1_meta) = current.level(1) {
            l1_meta.overlap_stats(&bs, &bl)
        } else {
            (Vec::new(), 0)
        };

        let overlaps_base = if let Some(l1_meta) = current.level(1) {
            l1_meta.gather(&base_l1_indices)
        } else {
            Vec::new()
        };
        let base_total_files = 1 + base_l1_indices.len();
        let base_total_bytes = base.size_bytes + base_l1_bytes;

        warn!(target: "compaction_picker",
              "L0 forced base-only despite limits: expanded plan files={} bytes={}, base-only files={} bytes={} (limits files={} bytes={})",
              total_files, total_bytes, base_total_files, base_total_bytes,
              cfg.max_compaction_input_files, cfg.max_compaction_input_bytes);

        return (base_only, overlaps_base, bs, bl);
    }

    (inputs, overlaps_l1, smallest, largest)
}

/// 将同层输入扩展到目标 key 范围 [s, l]，直到稳定为止
fn expand_level_to_range(
    level_meta: &LevelMetadata,
    _base: &[FileMeta],
    mut s: Bytes,
    mut l: Bytes,
) -> (Vec<FileMeta>, Bytes, Bytes) {
    loop {
        let overlapped = level_meta.overlap(&s, &l);
        let (ns, nl) = range_of_files(&overlapped);
        if ns == s && nl == l {
            return (overlapped, s, l);
        }
        s = ns;
        l = nl;
    }
}

/// clean-cut 扩展：
/// 1) 计算与下一层的重叠集合，并据此拉伸 [s,l]
/// 2) 回到当前层按 [s,l] 扩展输入
/// 3) 循环直到当前层输入与下一层重叠集合均稳定
fn expand_inputs_clean_cut(
    current: &Version,
    level: u32,
    initial: Vec<FileMeta>,
) -> (Vec<FileMeta>, Vec<FileMeta>, Bytes, Bytes) {
    // Safety check: level should always exist in a valid Version
    // If it doesn't, this is a programming error, but we handle it gracefully
    let level_meta = current.level(level).unwrap_or_else(|| {
        tracing::error!(
            "Invalid level {} requested in expand_inputs_clean_cut, max level is {}",
            level,
            current.num_levels() - 1
        );
        panic!(
            "Internal error: invalid level {} in compaction picker",
            level
        );
    });
    let mut inputs = initial;
    loop {
        // 1) 从当前 inputs 推出范围，并计算下一层重叠集合 overlaps1
        let (s1, l1) = range_of_files(&inputs);
        let overlaps1 = overlap_in_level(current, level + 1, &s1, &l1);

        // 2) 用 overlaps1 拉伸目标范围 [target_s, target_l]
        let mut target_s = s1.clone();
        let mut target_l = l1.clone();
        for f in &overlaps1 {
            let fs = f.smallest.user_key().clone();
            let fl = f.largest.user_key().clone();
            if fs.as_ref() < target_s.as_ref() {
                target_s = fs;
            }
            if fl.as_ref() > target_l.as_ref() {
                target_l = fl;
            }
        }

        // 3) 回到当前层，将 inputs 扩展到 [target_s, target_l]
        let (inputs2, s2, l2) = expand_level_to_range(level_meta, &inputs, target_s, target_l);
        let overlaps2 = overlap_in_level(current, level + 1, &s2, &l2);

        // 4) 稳定判定：先快速检查计数和范围，再比较集合
        if inputs2.len() == inputs.len()
            && overlaps2.len() == overlaps1.len()
            && s2 == s1
            && l2 == l1
        {
            let ids_inputs: HashSet<u64> = inputs.iter().map(|f| f.file_number).collect();
            let ids_inputs2: HashSet<u64> = inputs2.iter().map(|f| f.file_number).collect();
            if ids_inputs2 == ids_inputs {
                let ids_overlaps1: HashSet<u64> = overlaps1.iter().map(|f| f.file_number).collect();
                let ids_overlaps2: HashSet<u64> = overlaps2.iter().map(|f| f.file_number).collect();
                if ids_overlaps2 == ids_overlaps1 {
                    return (inputs2, overlaps2, s2, l2);
                }
            }
        }
        inputs = inputs2;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::version::{FileMeta, VersionSet};
    use bytes::Bytes;

    fn ik(k: &str, s: u64) -> crate::sstable::InternalKey {
        crate::sstable::InternalKey::new(Bytes::from(k.as_bytes().to_vec()), s)
    }

    #[test]
    fn pick_l0_trigger() {
        let vs = VersionSet::new(3, 100, 0).unwrap();
        // add 4 files to L0
        let mut edit = crate::version::VersionEdit::default();
        for i in 0..4u64 {
            let f = FileMeta::new(10 + i, 0, 10, ik("a", 5), ik("z", 1), 10, 0);
            edit.add_file(0, f);
        }
        let _ = vs.apply_edit(&edit).unwrap();

        let cfg = CompactionConfig {
            level0_trigger: 4,
            ..Default::default()
        };
        let plan = pick_compaction(&cfg, &vs.current()).unwrap();
        assert_eq!(plan.level, 0);
        assert_eq!(plan.target_level, 1);
        assert_eq!(plan.inputs_level.len(), 4);
    }

    #[test]
    fn pick_level_by_score_and_expand() {
        let vs = VersionSet::new(3, 100, 0).unwrap();
        // Level 1: two adjacent files [a-m], [n-z]
        let mut edit = crate::version::VersionEdit::default();
        let f1 = FileMeta::new(1, 1, 200 * 1024 * 1024, ik("a", 5), ik("m", 1), 10, 0);
        let f2 = FileMeta::new(2, 1, 200 * 1024 * 1024, ik("n", 5), ik("z", 1), 10, 0);
        edit.add_file(1, f1.clone());
        edit.add_file(1, f2.clone());
        // Level 2: overlap with [a-z] fully
        let f3 = FileMeta::new(3, 2, 1, ik("k", 9), ik("p", 1), 10, 0);
        edit.add_file(2, f3.clone());
        let _ = vs.apply_edit(&edit).unwrap();

        let mut cfg = CompactionConfig::default();
        cfg.level1_size_mb = 128; // small base to make score > 1
        let plan = pick_compaction(&cfg, &vs.current()).unwrap();
        assert_eq!(plan.level, 1);
        assert!(plan.inputs_level.len() >= 2); // expanded to include both neighbors
        assert!(!plan.inputs_next_level.is_empty());
    }

    #[test]
    fn overlap_edges_l1_and_empty() {
        // Build a LevelMetadata for L1 with two non-overlapping sorted files
        let f1 = FileMeta::new(1, 1, 10, ik("a", 5), ik("m", 1), 0, 0);
        let f2 = FileMeta::new(2, 1, 10, ik("n", 5), ik("z", 1), 0, 0);
        let lv = crate::version::LevelMetadata::new(1, vec![f2.clone(), f1.clone()]).unwrap();
        // equal boundary at 'm' -> only f1
        let r = lv.overlap(&Bytes::from("m"), &Bytes::from("m"));
        let ids: Vec<u64> = r.iter().map(|f| f.file_number).collect();
        assert_eq!(ids, vec![1]);
        // equal boundary at 'n' -> only f2
        let r = lv.overlap(&Bytes::from("n"), &Bytes::from("n"));
        let ids: Vec<u64> = r.iter().map(|f| f.file_number).collect();
        assert_eq!(ids, vec![2]);
        // full contain [a..z]
        let r = lv.overlap(&Bytes::from("a"), &Bytes::from("z"));
        let ids: Vec<u64> = r.iter().map(|f| f.file_number).collect();
        assert_eq!(ids, vec![1, 2]);
        // empty level
        let lv_empty = crate::version::LevelMetadata::empty(1);
        let r = lv_empty.overlap(&Bytes::from("a"), &Bytes::from("a"));
        assert!(r.is_empty());
    }

    #[test]
    fn overlap_l0_linear_scan() {
        // L0 allows overlap; verify overlap_cloned correctness
        let f1 = FileMeta::new(10, 0, 10, ik("b", 5), ik("d", 1), 0, 0);
        let f2 = FileMeta::new(11, 0, 10, ik("c", 5), ik("e", 1), 0, 0);
        let f3 = FileMeta::new(12, 0, 10, ik("x", 5), ik("z", 1), 0, 0);
        let lv0 = crate::version::LevelMetadata::new(0, vec![f2.clone(), f1.clone(), f3.clone()])
            .unwrap();
        let r = lv0.overlap(&Bytes::from("c"), &Bytes::from("c"));
        let mut ids: Vec<u64> = r.iter().map(|f| f.file_number).collect();
        ids.sort();
        assert_eq!(ids, vec![10, 11]);
        let r = lv0.overlap(&Bytes::from("f"), &Bytes::from("w"));
        assert!(r.is_empty());
    }

    #[test]
    fn clean_cut_stability_converges() {
        // Build a small 3-level layout to exercise clean-cut stability
        let vs = VersionSet::new(4, 100, 0).unwrap();
        let mut edit = crate::version::VersionEdit::default();
        // L1 two adjacent
        let f1 = FileMeta::new(1, 1, 50, ik("a", 5), ik("m", 1), 0, 0);
        let f2 = FileMeta::new(2, 1, 50, ik("n", 5), ik("z", 1), 0, 0);
        edit.add_file(1, f1.clone());
        edit.add_file(1, f2.clone());
        // L2 overlaps mid
        let f3 = FileMeta::new(3, 2, 10, ik("k", 5), ik("p", 1), 0, 0);
        edit.add_file(2, f3.clone());
        let v = vs.apply_edit(&edit).unwrap();
        let (i1, o1, s1, l1) = expand_inputs_clean_cut(&v, 1, vec![f2.clone()]);
        let (i2, o2, s2, l2) = expand_inputs_clean_cut(&v, 1, i1.clone());
        // stable by one more round
        assert_eq!(
            i1.iter().map(|f| f.file_number).collect::<Vec<_>>(),
            i2.iter().map(|f| f.file_number).collect::<Vec<_>>()
        );
        assert_eq!(
            o1.iter().map(|f| f.file_number).collect::<Vec<_>>(),
            o2.iter().map(|f| f.file_number).collect::<Vec<_>>()
        );
        assert_eq!(s1, s2);
        assert_eq!(l1, l2);
    }
}
