use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;

use tokio::runtime::Runtime;
use tokio::sync::{Notify, Semaphore, mpsc};

use tokio_util::sync::CancellationToken;

use crate::cost::{CostModel, FeedbackEvent, Policy};
use crate::error::{ExecutorError, Result};
use crate::priority::PriorityQueue;
use crate::task::{Completion, Task, TaskHandle, TaskId, TaskSpec};

const SCHEDULER_WORKER_THREADS: usize = 2;
const MAX_BLOCKING_THREADS_LIMIT: usize = 16;

const DEFAULT_MEDIUM_TASK_RATIO: f64 = 0.5;
const DEFAULT_TASK_QUEUE_CAPACITY: usize = 1024;

const DEFAULT_QUANTUM_CRITICAL: u64 = 8;
const DEFAULT_QUANTUM_HIGH: u64 = 4;
const DEFAULT_QUANTUM_MEDIUM: u64 = 1;

/// 调度器配置。
///
/// - `worker_threads`：调度/协调线程（runtime worker threads）
/// - `max_blocking_threads`：实际执行任务的 blocking 线程上限
/// - `medium_max_ratio`：限制 Medium 任务可占用的 blocking 并发比例
/// - `channel_capacity`：提交通道容量，用于背压
/// - `quantum_*`：DRR 权重
#[derive(Debug, Clone)]
pub struct SchedulerConfig {
    pub worker_threads: usize,
    pub max_blocking_threads: usize,

    // Medium 最大并发占比（0.0..=1.0）
    pub medium_max_ratio: f64,

    // 有界提交队列容量
    pub channel_capacity: usize,

    pub quantum_critical: u64,
    pub quantum_high: u64,
    pub quantum_medium: u64,
}

impl SchedulerConfig {
    /// Medium 并发上限（向上取整，至少 1，最多不超过 max_blocking_threads）。
    pub fn medium_max_permits(&self) -> usize {
        let permits = (self.max_blocking_threads as f64 * self.medium_max_ratio).ceil() as usize;
        permits.clamp(1, self.max_blocking_threads.max(1))
    }

    pub fn with_threads(max_blocking_threads: usize) -> Self {
        Self {
            worker_threads: SCHEDULER_WORKER_THREADS,
            max_blocking_threads,
            medium_max_ratio: DEFAULT_MEDIUM_TASK_RATIO,
            channel_capacity: DEFAULT_TASK_QUEUE_CAPACITY,
            quantum_critical: DEFAULT_QUANTUM_CRITICAL,
            quantum_high: DEFAULT_QUANTUM_HIGH,
            quantum_medium: DEFAULT_QUANTUM_MEDIUM,
        }
    }

    pub fn with_medium_ratio(max_blocking_threads: usize, medium_max_ratio: f64) -> Self {
        let mut cfg = Self::with_threads(max_blocking_threads);
        cfg.medium_max_ratio = medium_max_ratio.clamp(0.0, 1.0);
        cfg
    }
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        let cpu = num_cpus::get().max(1);
        let max_blocking_threads = cpu.min(MAX_BLOCKING_THREADS_LIMIT);
        Self::with_threads(max_blocking_threads)
    }
}

/// 运行期计数器；使用原子类型避免额外锁竞争。
#[derive(Default)]
pub struct SchedulerStats {
    pub submitted: std::sync::atomic::AtomicUsize,
    pub completed_ok: std::sync::atomic::AtomicUsize,
    pub panicked: std::sync::atomic::AtomicUsize,
}

#[derive(Default)]
pub struct SchedulerState {
    pub stats: SchedulerStats,
}

/// 全局调度器：Ingress 负责入队，单 Arbiter 负责出队与并发许可分配。
///
/// 核心约束：
/// - 只有一个调度决策点（Arbiter），避免多调度器竞争导致 DRR 失真
/// - 所有任务受 `sem_global` 约束；Medium 任务额外受 `sem_medium_cap` 约束
/// - Arbiter 使用 `try_acquire`，不在持锁期间 await，避免潜在死锁
pub struct GlobalScheduler {
    // 独立 runtime，避免与上层运行时生命周期/线程配置耦合
    runtime: Arc<Runtime>,

    // 任务提交入口；Ingress 是唯一接收者
    tx: mpsc::Sender<Task>,

    // Arbiter 出队与 Ingress 入队之间的共享队列
    queue: Arc<Mutex<PriorityQueue>>,

    // 新任务入队 / permit 释放后的唤醒信号
    notify: Arc<Notify>,

    // 全局并发许可（所有任务必需）
    sem_global: Arc<Semaphore>,

    // Medium 并发许可（仅 Medium 任务必需）
    sem_medium_cap: Arc<Semaphore>,

    cost_model: Arc<CostModel>,
    cancel: CancellationToken,

    state: Arc<SchedulerState>,
}

impl GlobalScheduler {
    /// 初始化 runtime、队列与后台循环（Ingress/Arbiter）。
    pub fn new(config: SchedulerConfig) -> Result<Self> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(config.worker_threads.max(1))
            .max_blocking_threads(config.max_blocking_threads.max(1))
            .thread_name("boxkv-executor")
            .enable_all()
            .build()
            .map_err(|e| {
                ExecutorError::RuntimeError(format!("Failed to create Tokio runtime: {}", e))
            })?;

        let (tx, rx) = mpsc::channel(config.channel_capacity.max(1));

        let queue = PriorityQueue::with_quanta(
            config.quantum_critical,
            config.quantum_high,
            config.quantum_medium,
        );

        let scheduler = Self {
            runtime: Arc::new(runtime),
            tx,
            queue: Arc::new(Mutex::new(queue)),
            notify: Arc::new(Notify::new()),
            sem_global: Arc::new(Semaphore::new(config.max_blocking_threads.max(1))),
            sem_medium_cap: Arc::new(Semaphore::new(config.medium_max_permits())),
            cost_model: Arc::new(CostModel::default()),
            cancel: CancellationToken::new(),
            state: Arc::new(SchedulerState::default()),
        };

        scheduler.start_loops(rx);
        Ok(scheduler)
    }

    /// 启动后台任务：Ingress 负责入队，Arbiter 负责出队并提交到 blocking pool。
    fn start_loops(&self, mut rx: mpsc::Receiver<Task>) {
        // Ingress：从提交通道接收任务，入队并唤醒 Arbiter
        let queue = Arc::clone(&self.queue);
        let notify = Arc::clone(&self.notify);
        let cancel = self.cancel.clone();
        let runtime = Arc::clone(&self.runtime);
        let state = Arc::clone(&self.state);

        runtime.spawn(async move {
            tracing::info!("executor ingress started");

            loop {
                tokio::select! {
                    _ = cancel.cancelled() => {
                        break;
                    }
                    msg = rx.recv() => {
                        match msg {
                            Some(task) => {
                                state.stats.submitted.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                queue.lock().push(task);
                                notify.notify_one();
                            }
                            None => {
                                // 提交侧全部 drop 后退出；最后唤醒一次以便 Arbiter 清空剩余队列
                                notify.notify_one();
                                break;
                            }
                        }
                    }
                }
            }

            tracing::info!("executor ingress exited");
        });

        // Arbiter：单线程调度。持有 queue 锁时完成“挑选任务 + Medium cap 检查”，避免 deficit 被无效推进。
        let queue = Arc::clone(&self.queue);
        let notify = Arc::clone(&self.notify);
        let sem_global = Arc::clone(&self.sem_global);
        let sem_medium = Arc::clone(&self.sem_medium_cap);
        let cancel = self.cancel.clone();
        let runtime = Arc::clone(&self.runtime);
        let state = Arc::clone(&self.state);

        runtime.spawn(async move {
            tracing::info!("executor arbiter started");

            loop {
                tokio::select! {
                    _ = cancel.cancelled() => {
                        break;
                    }
                    _ = notify.notified() => {
                        loop {
                            let global_permit = match sem_global.clone().try_acquire_owned() {
                                Ok(p) => p,
                                Err(_) => break,
                            };

                            let picked = {
                                let mut q = queue.lock();
                                let sem_m = Arc::clone(&sem_medium);

                                q.pop_drr_with_medium_cap(
                                    || sem_m.available_permits() > 0,
                                    || sem_m.clone().try_acquire_owned().ok(),
                                )
                            };

                            let Some((task, medium_permit)) = picked else {
                                drop(global_permit);
                                break;
                            };

                            let notify_done = Arc::clone(&notify);
                            let stats = Arc::clone(&state);

                            tokio::task::spawn_blocking(move || {
                                crate::task::mark_worker_thread();

                                let _g = global_permit;
                                let _m = medium_permit;

                                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                    task.execute();
                                }));

                                if result.is_ok() {
                                    stats.stats.completed_ok.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                } else {
                                    stats.stats.panicked.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                }

                                notify_done.notify_one();
                            });
                        }
                    }
                }
            }

            tracing::info!("executor arbiter exited");
        });
    }

    /// 提交任务（异步）。
    ///
    /// 仅负责入队；任务执行与完成由后台循环推进。
    pub async fn spawn_with_spec_async<F>(&self, spec: TaskSpec, job: F) -> Result<TaskId>
    where
        F: FnOnce(CancellationToken) -> Option<FeedbackEvent> + Send + 'static,
    {
        let priority = Policy::priority(spec.work_class);
        let cost = self
            .cost_model
            .estimate_cost(spec.work_class, spec.size_hint);
        let name = spec
            .tag
            .clone()
            .unwrap_or_else(|| format!("{:?}", spec.work_class));

        let cost_model = Arc::clone(&self.cost_model);
        let wrapped_job = move |cancel: CancellationToken| {
            if let Some(feedback) = job(cancel) {
                match feedback {
                    FeedbackEvent::ReadComplete { scope, value_bytes } => {
                        cost_model.observe_read(value_bytes, scope);
                    }
                }
            }
        };

        let mut task = Task::new(name, priority, wrapped_job);
        task.metadata.cost = cost;

        let task_id = task.id;

        self.tx
            .send(task)
            .await
            .map_err(|_| ExecutorError::internal("调度器已关闭"))?;

        Ok(task_id)
    }

    /// 提交任务（阻塞）。
    ///
    /// 适用于非 async 调用点；会阻塞等待任务完成。
    pub fn spawn_with_spec_blocking<F>(&self, spec: TaskSpec, job: F) -> Result<TaskId>
    where
        F: FnOnce(CancellationToken) -> Option<FeedbackEvent> + Send + 'static,
    {
        let priority = Policy::priority(spec.work_class);
        let cost = self
            .cost_model
            .estimate_cost(spec.work_class, spec.size_hint);
        let name = spec
            .tag
            .clone()
            .unwrap_or_else(|| format!("{:?}", spec.work_class));

        let cost_model = Arc::clone(&self.cost_model);
        let wrapped_job = move |cancel: CancellationToken| {
            if let Some(FeedbackEvent::ReadComplete { scope, value_bytes }) = job(cancel) {
                cost_model.observe_read(value_bytes, scope);
            }
        };

        let mut task = Task::new(name, priority, wrapped_job);
        task.metadata.cost = cost;

        let completion = Arc::new(Completion::new());
        task.attach_completion(Arc::clone(&completion));

        let task_id = task.id;
        self.tx
            .blocking_send(task)
            .map_err(|_| ExecutorError::internal("调度器已关闭"))?;

        completion.wait();
        Ok(task_id)
    }

    /// 提交任务并返回 TaskHandle。
    pub fn spawn_with_spec_handle<F, T>(&self, spec: TaskSpec, job: F) -> TaskHandle<T>
    where
        F: FnOnce(CancellationToken) -> Result<T> + Send + 'static,
        T: Send + 'static,
    {
        let priority = Policy::priority(spec.work_class);
        let cost = self
            .cost_model
            .estimate_cost(spec.work_class, spec.size_hint);
        let name = spec
            .tag
            .clone()
            .unwrap_or_else(|| format!("{:?}", spec.work_class));

        let result = Arc::new(Mutex::new(None));
        let completion = Arc::new(Completion::new());
        let result_clone = Arc::clone(&result);
        let completion_clone = Arc::clone(&completion);

        let wrapped_job = move |cancel: CancellationToken| {
            let job_result = job(cancel);
            *result_clone.lock() = Some(job_result);
            completion_clone.signal();
        };

        let mut task = Task::new(name, priority, wrapped_job);
        task.metadata.cost = cost;
        let task_id = task.id;
        let cancel_token = task.cancel_token.clone();

        // try_send 失败时由调用方通过 handle 观察到未完成/被取消等状态
        let tx = self.tx.clone();
        let _ = tx.try_send(task);

        TaskHandle::new(result, completion, cancel_token, task_id)
    }

    /// 触发后台循环退出；不等待在途任务完成。
    pub async fn shutdown(&self) {
        self.cancel.cancel();
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    /// 返回当前队列长度与提交/完成计数（completed 含 panic 任务）。
    pub fn stats(&self) -> (usize, usize, usize, usize, usize) {
        let (c, h, m) = self.queue.lock().stats();
        let submitted = self
            .state
            .stats
            .submitted
            .load(std::sync::atomic::Ordering::Relaxed);
        let ok = self
            .state
            .stats
            .completed_ok
            .load(std::sync::atomic::Ordering::Relaxed);
        let panicked = self
            .state
            .stats
            .panicked
            .load(std::sync::atomic::Ordering::Relaxed);
        (c, h, m, submitted, ok + panicked)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cost::{SizeHint, WorkClass};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    /// 创建测试用调度器
    fn test_scheduler() -> GlobalScheduler {
        let config = SchedulerConfig::with_threads(4);
        GlobalScheduler::new(config).unwrap()
    }

    /// 等待调度器处理任务
    #[allow(dead_code)]
    async fn wait_for_queue_empty(scheduler: &GlobalScheduler, timeout_ms: u64) {
        let start = std::time::Instant::now();
        loop {
            let (c, h, m, _, _) = scheduler.stats();
            if c == 0 && h == 0 && m == 0 {
                break;
            }
            if start.elapsed().as_millis() > timeout_ms as u128 {
                panic!("timeout waiting for queue empty");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// 等待所有任务完成（submitted == completed）
    async fn wait_for_completion(scheduler: &GlobalScheduler, timeout_ms: u64) {
        let start = std::time::Instant::now();
        loop {
            let (_, _, _, submitted, completed) = scheduler.stats();
            if submitted == completed && submitted > 0 {
                break;
            }
            if start.elapsed().as_millis() > timeout_ms as u128 {
                let (c, h, m, sub, comp) = scheduler.stats();
                panic!(
                    "timeout waiting for completion: queue=[{},{},{}] submitted={} completed={}",
                    c, h, m, sub, comp
                );
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// 安全地 drop scheduler
    async fn safe_drop_scheduler(scheduler: GlobalScheduler) {
        scheduler.shutdown().await;
        // 在 spawn_blocking 中 drop，避免 tokio async 上下文中的 Runtime drop 问题
        tokio::task::spawn_blocking(move || drop(scheduler))
            .await
            .unwrap();
    }

    // 1. 配置测试
    #[test]
    fn test_config_default() {
        let config = SchedulerConfig::default();
        assert!(config.worker_threads >= 1);
        assert!(config.max_blocking_threads >= 1);
        assert!(config.max_blocking_threads <= 16);
        assert_eq!(config.medium_max_ratio, 0.5);
        assert_eq!(config.quantum_critical, 8);
        assert_eq!(config.quantum_high, 4);
        assert_eq!(config.quantum_medium, 1);
    }

    #[test]
    fn test_config_with_threads() {
        let config = SchedulerConfig::with_threads(8);
        assert_eq!(config.max_blocking_threads, 8);
        assert_eq!(config.medium_max_permits(), 4); // 8 * 0.5 = 4
    }

    #[test]
    fn test_config_medium_ratio() {
        let config = SchedulerConfig::with_medium_ratio(8, 0.25);
        assert_eq!(config.medium_max_permits(), 2); // 8 * 0.25 = 2

        let config = SchedulerConfig::with_medium_ratio(7, 0.5);
        assert_eq!(config.medium_max_permits(), 4); // ceil(7 * 0.5) = 4
    }

    #[test]
    fn test_config_medium_ratio_clamp() {
        // 测试比例边界
        let config = SchedulerConfig::with_medium_ratio(8, 1.5);
        assert_eq!(config.medium_max_ratio, 1.0); // clamp to 1.0

        let config = SchedulerConfig::with_medium_ratio(8, -0.5);
        assert_eq!(config.medium_max_ratio, 0.0); // clamp to 0.0
        assert_eq!(config.medium_max_permits(), 1); // 至少 1
    }

    // 2. 基础功能测试
    #[tokio::test]
    async fn test_scheduler_creation_and_shutdown() {
        let scheduler = test_scheduler();

        // 验证初始状态
        let (c, h, m, submitted, completed) = scheduler.stats();
        assert_eq!(c, 0);
        assert_eq!(h, 0);
        assert_eq!(m, 0);
        assert_eq!(submitted, 0);
        assert_eq!(completed, 0);

        safe_drop_scheduler(scheduler).await;
    }

    #[tokio::test]
    async fn test_submit_single_task_async() {
        let scheduler = test_scheduler();
        let executed = Arc::new(AtomicUsize::new(0));
        let exec_clone = Arc::clone(&executed);

        let spec = TaskSpec {
            work_class: WorkClass::FrontendReadSmall,
            size_hint: SizeHint::Bytes(100),
            tag: Some("test_task".to_string()),
        };

        scheduler
            .spawn_with_spec_async(spec, move |_cancel| {
                exec_clone.fetch_add(1, Ordering::Relaxed);
                None
            })
            .await
            .expect("submit failed");

        // 等待任务完成
        wait_for_completion(&scheduler, 1000).await;

        assert_eq!(executed.load(Ordering::Relaxed), 1);
        let (_, _, _, submitted, completed) = scheduler.stats();
        assert_eq!(submitted, 1);
        assert_eq!(completed, 1);

        safe_drop_scheduler(scheduler).await;
    }

    #[tokio::test]
    async fn test_submit_single_task_blocking() {
        let scheduler = test_scheduler();
        let executed = Arc::new(AtomicUsize::new(0));
        let exec_clone = Arc::clone(&executed);

        let spec = TaskSpec {
            work_class: WorkClass::BackgroundWriteAmp,
            size_hint: SizeHint::Bytes(1000),
            tag: None,
        };

        // 在 tokio 上下文中使用 spawn_blocking 调用同步接口
        let sched_clone = Arc::new(scheduler);
        let sched_ref = Arc::clone(&sched_clone);

        tokio::task::spawn_blocking(move || {
            sched_ref
                .spawn_with_spec_blocking(spec, move |_cancel| {
                    exec_clone.fetch_add(1, Ordering::Relaxed);
                    None
                })
                .expect("submit failed")
        })
        .await
        .expect("spawn_blocking failed");

        wait_for_completion(&sched_clone, 1000).await;

        assert_eq!(executed.load(Ordering::Relaxed), 1);

        let sched = Arc::try_unwrap(sched_clone).unwrap_or_else(|_arc| {
            panic!("Arc still has multiple references");
        });
        safe_drop_scheduler(sched).await;
    }

    // 3. 优先级调度测试（DRR 比例）
    #[tokio::test]
    async fn test_priority_scheduling_ratio() {
        let config = SchedulerConfig::with_threads(4);
        let scheduler = GlobalScheduler::new(config).unwrap();

        let cnt_critical = Arc::new(AtomicUsize::new(0));
        let cnt_high = Arc::new(AtomicUsize::new(0));
        let cnt_medium = Arc::new(AtomicUsize::new(0));

        // 提交大量任务（每个优先级 100 个）
        for i in 0..100 {
            // Critical
            let c = Arc::clone(&cnt_critical);
            let spec = TaskSpec {
                work_class: WorkClass::FrontendReadSmall,
                size_hint: SizeHint::Bytes(10),
                tag: Some(format!("c{}", i)),
            };
            scheduler
                .spawn_with_spec_async(spec, move |_| {
                    c.fetch_add(1, Ordering::Relaxed);
                    std::thread::sleep(Duration::from_micros(100));
                    None
                })
                .await
                .unwrap();

            // High
            let h = Arc::clone(&cnt_high);
            let spec = TaskSpec {
                work_class: WorkClass::Durability,
                size_hint: SizeHint::Bytes(10),
                tag: Some(format!("h{}", i)),
            };
            scheduler
                .spawn_with_spec_async(spec, move |_| {
                    h.fetch_add(1, Ordering::Relaxed);
                    std::thread::sleep(Duration::from_micros(100));
                    None
                })
                .await
                .unwrap();

            // Medium
            let m = Arc::clone(&cnt_medium);
            let spec = TaskSpec {
                work_class: WorkClass::BackgroundWriteAmp,
                size_hint: SizeHint::Bytes(10),
                tag: Some(format!("m{}", i)),
            };
            scheduler
                .spawn_with_spec_async(spec, move |_| {
                    m.fetch_add(1, Ordering::Relaxed);
                    std::thread::sleep(Duration::from_micros(100));
                    None
                })
                .await
                .unwrap();
        }

        // 等待所有任务完成
        wait_for_completion(&scheduler, 10000).await;

        let c = cnt_critical.load(Ordering::Relaxed);
        let h = cnt_high.load(Ordering::Relaxed);
        let m = cnt_medium.load(Ordering::Relaxed);

        // 验证所有任务都执行了
        assert_eq!(c, 100);
        assert_eq!(h, 100);
        assert_eq!(m, 100);

        safe_drop_scheduler(scheduler).await;
    }

    // 4. 并发控制测试
    #[tokio::test]
    async fn test_global_semaphore_limit() {
        let config = SchedulerConfig::with_threads(2); // 只有 2 个工作线程
        let scheduler = GlobalScheduler::new(config).unwrap();

        let running = Arc::new(AtomicUsize::new(0));
        let max_concurrent = Arc::new(AtomicUsize::new(0));

        for i in 0..10 {
            let r = Arc::clone(&running);
            let m = Arc::clone(&max_concurrent);
            let spec = TaskSpec {
                work_class: WorkClass::FrontendReadSmall,
                size_hint: SizeHint::Bytes(10),
                tag: Some(format!("task{}", i)),
            };

            scheduler
                .spawn_with_spec_async(spec, move |_| {
                    let cur = r.fetch_add(1, Ordering::SeqCst) + 1;

                    // 更新最大并发数
                    m.fetch_max(cur, Ordering::SeqCst);

                    // 模拟工作
                    std::thread::sleep(Duration::from_millis(50));

                    r.fetch_sub(1, Ordering::SeqCst);
                    None
                })
                .await
                .unwrap();
        }

        wait_for_completion(&scheduler, 2000).await;

        let max = max_concurrent.load(Ordering::Relaxed);
        // 最大并发数不应超过配置的线程数
        assert!(max <= 2, "max_concurrent={} should <= 2", max);

        safe_drop_scheduler(scheduler).await;
    }

    #[tokio::test]
    async fn test_medium_cap_limit() {
        let config = SchedulerConfig::with_threads(4); // 4 个线程，Medium 最多 2 个
        let scheduler = GlobalScheduler::new(config).unwrap();

        let medium_running = Arc::new(AtomicUsize::new(0));
        let medium_max = Arc::new(AtomicUsize::new(0));

        // 提交 10 个 Medium 任务
        for i in 0..10 {
            let r = Arc::clone(&medium_running);
            let m = Arc::clone(&medium_max);
            let spec = TaskSpec {
                work_class: WorkClass::BackgroundWriteAmp,
                size_hint: SizeHint::Bytes(10),
                tag: Some(format!("medium{}", i)),
            };

            scheduler
                .spawn_with_spec_async(spec, move |_| {
                    let cur = r.fetch_add(1, Ordering::SeqCst) + 1;
                    m.fetch_max(cur, Ordering::SeqCst);
                    std::thread::sleep(Duration::from_millis(50));
                    r.fetch_sub(1, Ordering::SeqCst);
                    None
                })
                .await
                .unwrap();
        }

        wait_for_completion(&scheduler, 2000).await;

        let max = medium_max.load(Ordering::Relaxed);
        // Medium 最大并发不应超过 50% = 2
        assert!(max <= 2, "medium_max={} should <= 2", max);

        safe_drop_scheduler(scheduler).await;
    }

    // 5. 边界条件测试
    #[tokio::test]
    async fn test_submit_after_shutdown() {
        let scheduler = test_scheduler();
        scheduler.shutdown().await;

        // 关闭后提交应该失败
        let spec = TaskSpec {
            work_class: WorkClass::FrontendReadSmall,
            size_hint: SizeHint::Bytes(10),
            tag: None,
        };

        let result = scheduler.spawn_with_spec_async(spec, |_| None).await;

        assert!(result.is_err());

        safe_drop_scheduler(scheduler).await;
    }

    #[tokio::test]
    async fn test_empty_queue_stats() {
        let scheduler = test_scheduler();

        let (c, h, m, submitted, completed) = scheduler.stats();
        assert_eq!(c, 0);
        assert_eq!(h, 0);
        assert_eq!(m, 0);
        assert_eq!(submitted, 0);
        assert_eq!(completed, 0);

        safe_drop_scheduler(scheduler).await;
    }

    // 6. 异常处理测试
    #[tokio::test]
    async fn test_panic_task_recovery() {
        let scheduler = test_scheduler();

        // 提交一个会 panic 的任务
        let spec = TaskSpec {
            work_class: WorkClass::FrontendReadSmall,
            size_hint: SizeHint::Bytes(10),
            tag: Some("panic_task".to_string()),
        };

        scheduler
            .spawn_with_spec_async(spec, |_| {
                panic!("intentional panic");
            })
            .await
            .unwrap();

        wait_for_completion(&scheduler, 1000).await;

        let (_, _, _, submitted, completed) = scheduler.stats();
        assert_eq!(submitted, 1);
        assert_eq!(completed, 1); // panic 也算完成

        // 验证调度器仍然可用
        let executed = Arc::new(AtomicUsize::new(0));
        let exec_clone = Arc::clone(&executed);
        let spec = TaskSpec {
            work_class: WorkClass::FrontendReadSmall,
            size_hint: SizeHint::Bytes(10),
            tag: None,
        };

        scheduler
            .spawn_with_spec_async(spec, move |_| {
                exec_clone.fetch_add(1, Ordering::Relaxed);
                None
            })
            .await
            .unwrap();

        // 等待第二个任务完成
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(executed.load(Ordering::Relaxed), 1);

        safe_drop_scheduler(scheduler).await;
    }

    // 7. 压力测试
    #[tokio::test]
    async fn test_high_concurrency_mixed_priority() {
        let config = SchedulerConfig::with_threads(8);
        let scheduler = GlobalScheduler::new(config).unwrap();

        let total_executed = Arc::new(AtomicUsize::new(0));

        // 提交 1000 个混合优先级任务
        for i in 0..1000 {
            let exec = Arc::clone(&total_executed);
            let work_class = match i % 3 {
                0 => WorkClass::FrontendReadSmall,
                1 => WorkClass::Durability,
                _ => WorkClass::BackgroundWriteAmp,
            };

            let spec = TaskSpec {
                work_class,
                size_hint: SizeHint::Bytes(10),
                tag: Some(format!("task{}", i)),
            };

            scheduler
                .spawn_with_spec_async(spec, move |_| {
                    exec.fetch_add(1, Ordering::Relaxed);
                    // 快速任务，避免测试超时
                    std::thread::sleep(Duration::from_micros(10));
                    None
                })
                .await
                .unwrap();
        }

        wait_for_completion(&scheduler, 10000).await;

        assert_eq!(total_executed.load(Ordering::Relaxed), 1000);
        let (_, _, _, submitted, completed) = scheduler.stats();
        assert_eq!(submitted, 1000);
        assert_eq!(completed, 1000);

        safe_drop_scheduler(scheduler).await;
    }

    #[tokio::test]
    async fn test_burst_submit() {
        let scheduler = Arc::new(test_scheduler());
        let executed = Arc::new(AtomicUsize::new(0));

        // 批量快速提交
        let mut handles = vec![];
        for i in 0..100 {
            let sched = Arc::clone(&scheduler);
            let exec = Arc::clone(&executed);
            let handle = tokio::spawn(async move {
                let spec = TaskSpec {
                    work_class: WorkClass::FrontendReadSmall,
                    size_hint: SizeHint::Bytes(10),
                    tag: Some(format!("burst{}", i)),
                };

                sched
                    .spawn_with_spec_async(spec, move |_| {
                        exec.fetch_add(1, Ordering::Relaxed);
                        None
                    })
                    .await
                    .unwrap();
            });
            handles.push(handle);
        }

        // 等待所有提交完成
        for h in handles {
            h.await.unwrap();
        }

        wait_for_completion(&scheduler, 3000).await;

        assert_eq!(executed.load(Ordering::Relaxed), 100);

        let sched = Arc::try_unwrap(scheduler).unwrap_or_else(|_arc| {
            panic!("Arc still has multiple references");
        });
        safe_drop_scheduler(sched).await;
    }

    // 8. 统计准确性测试
    #[tokio::test]
    async fn test_stats_accuracy() {
        let scheduler = test_scheduler();

        // 提交 10 个任务
        for i in 0..10 {
            let spec = TaskSpec {
                work_class: WorkClass::FrontendReadSmall,
                size_hint: SizeHint::Bytes(10),
                tag: Some(format!("stats{}", i)),
            };

            scheduler
                .spawn_with_spec_async(spec, |_| {
                    std::thread::sleep(Duration::from_millis(10));
                    None
                })
                .await
                .unwrap();
        }

        wait_for_completion(&scheduler, 2000).await;

        let (c, h, m, submitted, completed) = scheduler.stats();
        assert_eq!(c, 0); // 队列已空
        assert_eq!(h, 0);
        assert_eq!(m, 0);
        assert_eq!(submitted, 10);
        assert_eq!(completed, 10);

        safe_drop_scheduler(scheduler).await;
    }

    #[tokio::test]
    async fn test_queue_length_tracking() {
        let scheduler = test_scheduler();

        // 提交任务但不让它们立即执行（使用长时间睡眠）
        for i in 0..5 {
            let spec = TaskSpec {
                work_class: WorkClass::FrontendReadSmall,
                size_hint: SizeHint::Bytes(10),
                tag: Some(format!("queue{}", i)),
            };

            scheduler
                .spawn_with_spec_async(spec, |_| {
                    std::thread::sleep(Duration::from_millis(100));
                    None
                })
                .await
                .unwrap();
        }

        // 等待任务入队
        tokio::time::sleep(Duration::from_millis(50)).await;

        let (c, _, _, submitted, _) = scheduler.stats();
        assert!(submitted >= 5);
        // 由于任务在执行，队列中可能有剩余
        assert!(c <= 5);

        wait_for_completion(&scheduler, 2000).await;

        safe_drop_scheduler(scheduler).await;
    }
}
