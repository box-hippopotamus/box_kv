use crate::manifest::Manifest;
use crate::version::VersionSet;
use boxkv_storage::FileSystem;
use bytes::Bytes;
use std::sync::{
    Arc, Condvar, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::thread::{self, JoinHandle};

use super::job::execute_compaction_sub;
use super::metrics::CompactionMetrics;
use super::picker::pick_compaction;
use super::types::TablePathProvider;
use crate::db::snapshot::SnapshotList;
use boxkv_common::config::CompactionConfig;
use boxkv_executor::{GlobalScheduler, SizeHint, TaskSpec, WorkClass};
use tracing::{error, info, warn};

#[derive(Clone)]
struct BusyRange {
    // 同一 compaction 在源层与目标层各登记一条记录，使用相同 id 关联
    id: u64,
    level: u32,
    start: Bytes,
    end: Bytes,
}

struct BusySet {
    ranges: Vec<BusyRange>,
    next_id: u64,
}

impl BusySet {
    fn new() -> Self {
        Self {
            ranges: Vec::new(),
            next_id: 1,
        }
    }

    // 闭区间重叠：任一端点落入对方区间或包含关系
    fn overlap(a_start: &Bytes, a_end: &Bytes, b_start: &Bytes, b_end: &Bytes) -> bool {
        !(a_start > b_end || a_end < b_start)
    }

    // 在源层与目标层按 (level, key-range) 登记占用，用于避免与在途任务产生范围冲突
    fn try_reserve(&mut self, plan: &crate::compaction::types::CompactionPlan) -> Option<u64> {
        for r in &self.ranges {
            if r.level == plan.level
                && Self::overlap(&r.start, &r.end, &plan.smallest, &plan.largest)
            {
                return None;
            }
            if r.level == plan.target_level
                && Self::overlap(&r.start, &r.end, &plan.smallest, &plan.largest)
            {
                return None;
            }
        }

        let id = self.next_id;
        self.next_id += 1;

        self.ranges.push(BusyRange {
            id,
            level: plan.level,
            start: plan.smallest.clone(),
            end: plan.largest.clone(),
        });
        self.ranges.push(BusyRange {
            id,
            level: plan.target_level,
            start: plan.smallest.clone(),
            end: plan.largest.clone(),
        });

        Some(id)
    }

    fn release(&mut self, id: u64) {
        self.ranges.retain(|r| r.id != id);
    }
}

/// CompactionScheduler 负责计划选择、范围冲突控制与背压，并将作业提交给 GlobalScheduler。
/// 后台线程通过条件变量驱动事件循环；外部通过 schedule_compaction() 发起唤醒。
pub struct CompactionScheduler<FS: FileSystem> {
    cfg: CompactionConfig,
    sst_ctx: crate::sstable::SSTableContext,
    vs: Arc<VersionSet>,
    manifest: Arc<Mutex<Manifest<FS>>>,
    path: Arc<dyn TablePathProvider>,
    exec: Arc<GlobalScheduler>,

    // (level, key-range) 粒度的占用集合，用于避免同层/相邻层 compaction 范围交叉
    busy: Mutex<BusySet>,

    // 当前在途作业数
    running: Mutex<usize>,

    // 在途作业的累计输入字节数，用于软/硬背压
    pending_bytes: Mutex<u64>,

    metrics: Arc<CompactionMetrics>,

    // 用于获取最小活跃快照序列号，避免回收仍被快照可见的数据
    snapshot_list: Arc<SnapshotList>,

    // (pending_flag, condvar)：pending_flag 仅用于避免丢信号/合并唤醒
    schedule_signal: Arc<(Mutex<bool>, Condvar)>,

    // 后台线程退出信号
    shutdown: Arc<AtomicBool>,

    bg_thread: Mutex<Option<JoinHandle<()>>>,
}

impl<FS: FileSystem + 'static> CompactionScheduler<FS> {
    pub fn new(
        cfg: CompactionConfig,
        sst_ctx: crate::sstable::SSTableContext,
        vs: Arc<VersionSet>,
        manifest: Arc<Mutex<Manifest<FS>>>,
        path: Arc<dyn TablePathProvider>,
        exec: Arc<GlobalScheduler>,
        snapshot_list: Arc<SnapshotList>,
    ) -> Arc<Self> {
        let schedule_signal = Arc::new((Mutex::new(false), Condvar::new()));
        let shutdown = Arc::new(AtomicBool::new(false));

        let scheduler = Arc::new(Self {
            cfg,
            sst_ctx,
            vs,
            manifest,
            path,
            exec,
            busy: Mutex::new(BusySet::new()),
            running: Mutex::new(0),
            pending_bytes: Mutex::new(0),
            metrics: CompactionMetrics::new(),
            snapshot_list,
            schedule_signal,
            shutdown,
            bg_thread: Mutex::new(None),
        });

        // 后台调度线程
        let scheduler_clone = Arc::clone(&scheduler);
        let handle = thread::spawn(move || {
            scheduler_clone.background_scheduler_loop();
        });

        match scheduler.bg_thread.lock() {
            Ok(mut guard) => *guard = Some(handle),
            Err(poisoned) => {
                error!(target: "compaction_sched", "bg_thread lock poisoned during init, recovering");
                *poisoned.into_inner() = Some(handle);
            }
        }
        scheduler
    }

    /// 后台线程：等待唤醒后尽可能多地调度可执行计划；无计划时再次进入等待。
    fn background_scheduler_loop(self: &Arc<Self>) {
        info!(target: "compaction_sched", "background scheduler thread started");

        loop {
            let (lock, cvar) = &*self.schedule_signal;
            let mut pending = match lock.lock() {
                Ok(guard) => guard,
                Err(poisoned) => {
                    error!(target: "compaction_sched", "schedule_signal lock poisoned in background loop, recovering");
                    poisoned.into_inner()
                }
            };
            while !*pending && !self.shutdown.load(Ordering::Relaxed) {
                pending = match cvar.wait(pending) {
                    Ok(guard) => guard,
                    Err(poisoned) => {
                        error!(target: "compaction_sched", "condvar wait returned poisoned lock, recovering");
                        poisoned.into_inner()
                    }
                };
            }

            if self.shutdown.load(Ordering::Relaxed) {
                info!(target: "compaction_sched", "background scheduler shutting down");
                break;
            }

            *pending = false;
            drop(pending);

            let now_secs = boxkv_common::time::current_timestamp_secs();
            let min_snapshot_seq = self.snapshot_list.oldest_sequence().unwrap_or(0);

            // 同一栈帧内循环尝试，避免递归式“调度-回调-再调度”的栈增长
            while self.try_schedule_once(now_secs, min_snapshot_seq) {}
        }
    }

    /// 发起一次调度唤醒；不要求调用方阻塞等待结果。
    pub fn schedule_compaction(&self) {
        let (lock, cvar) = &*self.schedule_signal;
        let mut pending = match lock.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                error!(target: "compaction_sched", "schedule_signal lock poisoned in schedule_compaction, recovering");
                poisoned.into_inner()
            }
        };
        *pending = true;
        cvar.notify_one();
    }

    /// 停止后台线程并等待退出。
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Relaxed);

        let (lock, cvar) = &*self.schedule_signal;
        let mut pending = match lock.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                error!(target: "compaction_sched", "schedule_signal lock poisoned in shutdown, recovering");
                poisoned.into_inner()
            }
        };
        *pending = true;
        cvar.notify_one();
        drop(pending);

        let handle_opt = match self.bg_thread.lock() {
            Ok(mut guard) => guard.take(),
            Err(poisoned) => {
                error!(target: "compaction_sched", "bg_thread lock poisoned in shutdown, recovering");
                poisoned.into_inner().take()
            }
        };
        if let Some(handle) = handle_opt {
            let _ = handle.join();
        }
    }

    /// 单次调度尝试：选择计划 → 做范围占用与背压检查 → 提交作业。
    fn try_schedule_once(self: &Arc<Self>, now_secs: u64, min_snapshot_seq: u64) -> bool {
        let current = self.vs.current();
        let plan = match pick_compaction(&self.cfg, &current) {
            Some(p) => p,
            None => return false,
        };

        info!(
            target: "compaction_sched",
            "picked L{} -> L{} inputs_k={} inputs_k+1={}",
            plan.level,
            plan.target_level,
            plan.inputs_level.len(),
            plan.inputs_next_level.len()
        );

        let mut busy = match self.busy.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                error!(target: "compaction_sched", "busy lock poisoned, recovering");
                poisoned.into_inner()
            }
        };
        let token = match busy.try_reserve(&plan) {
            Some(id) => id,
            None => return false,
        };
        drop(busy);

        // 以输入字节为粒度做配额/背压与调度 size hint
        let bytes_in: u64 = plan.inputs_level.iter().map(|f| f.size_bytes).sum::<u64>()
            + plan
                .inputs_next_level
                .iter()
                .map(|f| f.size_bytes)
                .sum::<u64>();

        {
            let mut running = match self.running.lock() {
                Ok(guard) => guard,
                Err(poisoned) => {
                    error!(target: "compaction_sched", "running lock poisoned, recovering");
                    poisoned.into_inner()
                }
            };
            if *running >= self.cfg.max_background_jobs {
                let mut b = match self.busy.lock() {
                    Ok(guard) => guard,
                    Err(poisoned) => {
                        error!(target: "compaction_sched", "busy lock poisoned on release, recovering");
                        poisoned.into_inner()
                    }
                };
                b.release(token);
                warn!(target: "compaction_sched", "throttle: max_background_jobs reached");
                return false;
            }

            let mut pending = match self.pending_bytes.lock() {
                Ok(guard) => guard,
                Err(poisoned) => {
                    error!(target: "compaction_sched", "pending_bytes lock poisoned, recovering");
                    poisoned.into_inner()
                }
            };

            if *pending + bytes_in >= self.cfg.hard_pending_compaction_bytes {
                let mut b = match self.busy.lock() {
                    Ok(guard) => guard,
                    Err(poisoned) => {
                        error!(target: "compaction_sched", "busy lock poisoned on hard limit release, recovering");
                        poisoned.into_inner()
                    }
                };
                b.release(token);
                warn!(
                    target: "compaction_sched",
                    "reject: hard pending bytes exceeded pending={} add={}",
                    *pending,
                    bytes_in
                );
                return false;
            }

            // 超过软阈值且已有在途作业时，倾向于先等待回落，避免堆积扩大
            if *pending + bytes_in >= self.cfg.soft_pending_compaction_bytes && *running >= 1 {
                let mut b = match self.busy.lock() {
                    Ok(guard) => guard,
                    Err(poisoned) => {
                        error!(target: "compaction_sched", "busy lock poisoned on soft limit release, recovering");
                        poisoned.into_inner()
                    }
                };
                b.release(token);
                warn!(
                    target: "compaction_sched",
                    "skip: soft pending bytes exceeded pending={} add={}",
                    *pending,
                    bytes_in
                );
                return false;
            }

            *pending += bytes_in;
            self.metrics.add_bytes_in(bytes_in);
            self.metrics
                .add_files_in((plan.inputs_level.len() + plan.inputs_next_level.len()) as u64);
            self.metrics.set_pending_bytes(*pending);
            self.metrics.inc_jobs_started();
            *running += 1;

            info!(
                target: "compaction_sched",
                "schedule job running={} pending_bytes={}",
                *running,
                *pending
            );
        }

        let this = Arc::clone(self);
        let plan_clone = plan.clone();

        let spec =
            TaskSpec::new(WorkClass::BackgroundWriteAmp, SizeHint::Bytes(bytes_in)).with_tag(
                format!("compaction_L{}_to_L{}", plan.level, plan.target_level),
            );

        let _ = self.exec.spawn_with_spec_blocking(spec, move |_cancel| {
            info!(
                target: "compaction_sched",
                "begin job L{} -> L{}",
                plan_clone.level,
                plan_clone.target_level
            );

            use crate::compaction::defaults::DefaultVersionCommit;
            let commit = DefaultVersionCommit {
                vs: Arc::clone(&this.vs),
                manifest: Arc::clone(&this.manifest),
            };

            let _ = execute_compaction_sub(
                &this.cfg,
                &this.sst_ctx,
                this.path.clone(),
                this.vs.clone(),
                &plan_clone,
                now_secs,
                min_snapshot_seq,
                &commit,
            );

            // 作业结束：释放占用、回收 pending、递减 running，并触发下一轮调度
            let mut b = match this.busy.lock() {
                Ok(guard) => guard,
                Err(poisoned) => {
                    error!(target: "compaction_sched", "busy lock poisoned on job finish, recovering");
                    poisoned.into_inner()
                }
            };
            b.release(token);
            drop(b);

            let mut running = match this.running.lock() {
                Ok(guard) => guard,
                Err(poisoned) => {
                    error!(target: "compaction_sched", "running lock poisoned on job finish, recovering");
                    poisoned.into_inner()
                }
            };
            *running -= 1;
            let current_running = *running;
            drop(running);

            let bytes_in: u64 = plan_clone
                .inputs_level
                .iter()
                .map(|f| f.size_bytes)
                .sum::<u64>()
                + plan_clone
                    .inputs_next_level
                    .iter()
                    .map(|f| f.size_bytes)
                    .sum::<u64>();

            let mut pending = match this.pending_bytes.lock() {
                Ok(guard) => guard,
                Err(poisoned) => {
                    error!(target: "compaction_sched", "pending_bytes lock poisoned on job finish, recovering");
                    poisoned.into_inner()
                }
            };
            *pending = pending.saturating_sub(bytes_in);
            let current_pending = *pending;
            this.metrics.set_pending_bytes(*pending);
            drop(pending);

            this.metrics.inc_jobs_finished();

            // 移除引用计数已归零的旧版本
            let cleaned = this.vs.cleanup_obsolete_versions();
            if cleaned > 0 {
                info!(
                    target: "compaction_sched",
                    "cleaned {} obsolete versions, alive={}",
                    cleaned,
                    this.vs.num_alive_versions()
                );
            }

            info!(
                target: "compaction_sched",
                "finish job L{} -> L{} running={} pending_bytes={}",
                plan_clone.level,
                plan_clone.target_level,
                current_running,
                current_pending
            );

            this.schedule_compaction();

            None
        });

        true
    }
}

impl<FS: FileSystem> Drop for CompactionScheduler<FS> {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);

        let (lock, cvar) = &*self.schedule_signal;
        if let Ok(mut pending) = lock.lock() {
            *pending = true;
            cvar.notify_one();
        }
        cvar.notify_one();

        if let Ok(mut handle_opt) = self.bg_thread.lock() {
            if let Some(handle) = handle_opt.take() {
                let _ = handle.join();
                info!(target: "compaction_sched", "后台调度线程已退出");
            }
        }
    }
}
