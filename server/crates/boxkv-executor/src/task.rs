use crate::cost::{SizeHint, WorkClass};
use crate::error::ExecutorError;
use crate::priority::Priority;
use parking_lot::{Condvar, Mutex};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

thread_local! {
    /// 工作线程标记
    static IS_WORKER_THREAD: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// 标记当前线程为工作线程
pub(crate) fn mark_worker_thread() {
    IS_WORKER_THREAD.with(|f| f.set(true));
}

/// 检查当前是否为工作线程
pub(crate) fn is_worker_thread() -> bool {
    IS_WORKER_THREAD.with(|f| f.get())
}

/// 全局任务 ID 生成器
static NEXT_TASK_ID: AtomicU64 = AtomicU64::new(1);

/// 任务唯一标识
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TaskId(u64);

impl TaskId {
    /// 生成新的任务 ID
    pub fn new() -> Self {
        Self(NEXT_TASK_ID.fetch_add(1, Ordering::Relaxed))
    }

    /// 获取 ID 值
    pub fn as_u64(self) -> u64 {
        self.0
    }
}

impl Default for TaskId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for TaskId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "task#{}", self.0)
    }
}

/// 任务规格
#[derive(Debug, Clone)]
pub struct TaskSpec {
    /// 工作负载类别
    pub work_class: WorkClass,

    /// 大小提示
    pub size_hint: SizeHint,

    /// 业务标签
    pub tag: Option<String>,
}

impl TaskSpec {
    /// 创建任务规格
    pub fn new(work_class: WorkClass, size_hint: SizeHint) -> Self {
        Self {
            work_class,
            size_hint,
            tag: None,
        }
    }

    /// 设置业务标签
    pub fn with_tag(mut self, tag: impl Into<String>) -> Self {
        self.tag = Some(tag.into());
        self
    }
}

/// 任务元数据
#[derive(Debug, Clone)]
pub struct TaskMetadata {
    /// 任务名称（用于日志）
    pub name: String,

    /// 优先级
    pub priority: Priority,

    /// 任务成本
    pub cost: u64,

    /// 入队时间
    pub(crate) enqueued_at: Option<Instant>,

    /// 硬截止时间
    pub deadline: Option<Instant>,

    /// 存活时间
    pub ttl: Option<Duration>,
}

impl TaskMetadata {
    pub fn new(name: impl Into<String>, priority: Priority) -> Self {
        Self {
            name: name.into(),
            priority,
            cost: 1,
            enqueued_at: None,
            deadline: None,
            ttl: None,
        }
    }

    /// 设置硬截止时间
    pub fn with_deadline(mut self, deadline: Instant) -> Self {
        self.deadline = Some(deadline);
        self
    }

    /// 设置存活时间（TTL）
    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.ttl = Some(ttl);
        self
    }

    /// 检查任务是否已过期
    pub fn is_expired(&self) -> bool {
        if let Some(deadline) = self.deadline {
            return Instant::now() > deadline;
        }
        if let (Some(enqueued_at), Some(ttl)) = (self.enqueued_at, self.ttl) {
            return enqueued_at.elapsed() > ttl;
        }
        false
    }

    /// 获取排队延迟（用于指标）
    pub fn queue_latency(&self) -> Duration {
        self.enqueued_at
            .map(|t| t.elapsed())
            .unwrap_or(Duration::ZERO)
    }
}

/// 任务抽象
pub struct Task {
    pub id: TaskId,
    pub metadata: TaskMetadata,

    /// 取消令牌（用于外部取消）
    pub cancel_token: tokio_util::sync::CancellationToken,

    /// 执行载荷（接收 CancellationToken）
    pub(crate) payload: Box<dyn FnOnce(tokio_util::sync::CancellationToken) + Send + 'static>,

    /// 任务完成通知（阻塞等待用）
    pub(crate) completion: Option<Arc<Completion>>,
}

impl Task {
    /// 创建新任务（接收 CancellationToken）
    pub(crate) fn new<F>(name: impl Into<String>, priority: Priority, f: F) -> Self
    where
        F: FnOnce(tokio_util::sync::CancellationToken) + Send + 'static,
    {
        let cancel_token = tokio_util::sync::CancellationToken::new();
        Self {
            id: TaskId::new(),
            metadata: TaskMetadata::new(name, priority),
            cancel_token: cancel_token.clone(),
            payload: Box::new(f),
            completion: None,
        }
    }

    /// 附加完成通知器（供阻塞 join 使用）
    pub(crate) fn attach_completion(&mut self, c: Arc<Completion>) {
        self.completion = Some(c);
    }

    /// 取消任务
    pub fn cancel(&self) {
        self.cancel_token.cancel();
    }

    /// 检查是否已取消
    pub fn is_cancelled(&self) -> bool {
        self.cancel_token.is_cancelled()
    }

    /// 执行任务
    pub fn execute(self) {
        use std::panic::{AssertUnwindSafe, catch_unwind};

        let task_id = self.id;
        let name = self.metadata.name.clone();
        let priority = self.metadata.priority;
        let cost = self.metadata.cost;
        let queue_latency = self.metadata.queue_latency();

        // 创建 tracing span（结构化日志）
        let span = tracing::debug_span!(
            "task_execute",
            task_id = %task_id,
            name = %name,
            priority = ?priority,
            cost = cost,
            queue_latency_ms = queue_latency.as_millis(),
        );
        let _guard = span.enter();

        // 1. 检查是否已取消
        if self.cancel_token.is_cancelled() {
            tracing::debug!("task_cancelled_before_start");
            return;
        }

        // 2. 检查是否已过期
        if self.metadata.is_expired() {
            tracing::warn!(
                expired_at_ms = self.metadata.deadline.map(|d| d.elapsed().as_millis()),
                "task_expired_dropped"
            );
            return;
        }

        // 3. 执行任务
        tracing::trace!("task_start");
        let start = Instant::now();

        let result = catch_unwind(AssertUnwindSafe(|| (self.payload)(self.cancel_token)));

        let elapsed = start.elapsed();

        // 4. 处理结果
        match result {
            Ok(()) => {
                tracing::trace!(elapsed_ms = elapsed.as_millis(), "task_completed");
            }
            Err(panic_info) => {
                // Panic 被捕获，记录错误但不杀死 worker
                let panic_msg = if let Some(s) = panic_info.downcast_ref::<&str>() {
                    (*s).to_string()
                } else if let Some(s) = panic_info.downcast_ref::<String>() {
                    s.clone()
                } else {
                    "unknown panic".to_string()
                };

                tracing::error!(
                    elapsed_ms = elapsed.as_millis(),
                    panic_msg = %panic_msg,
                    "task_panicked"
                );
            }
        }

        // 发出完成信号（无论是否 panic）
        if let Some(c) = self.completion {
            c.signal();
        }
    }
}

/// 任务完成等待器
pub(crate) struct Completion {
    done: Mutex<bool>,
    cv: Condvar,
    waker: Mutex<Option<Waker>>,
}

impl Completion {
    pub(crate) fn new() -> Self {
        Self {
            done: Mutex::new(false),
            cv: Condvar::new(),
            waker: Mutex::new(None),
        }
    }

    pub(crate) fn signal(&self) {
        {
            let mut done = self.done.lock();
            *done = true;
            self.cv.notify_one();
        }

        // 唤醒异步等待者
        if let Some(waker) = self.waker.lock().take() {
            waker.wake();
        }
    }

    /// 同步阻塞等待
    pub(crate) fn wait(&self) {
        let mut done = self.done.lock();
        while !*done {
            self.cv.wait(&mut done);
        }
    }

    /// 带超时的同步阻塞等待
    pub(crate) fn wait_timeout(&self, timeout: Duration) -> bool {
        let mut done = self.done.lock();
        let deadline = Instant::now() + timeout;

        while !*done {
            let now = Instant::now();
            if now >= deadline {
                return false; // 超时
            }

            let remaining = deadline - now;
            let result = self.cv.wait_for(&mut done, remaining);

            if result.timed_out() && !*done {
                return false; // 超时
            }
        }

        true // 完成
    }

    /// 检查是否已完成
    pub(crate) fn is_done(&self) -> bool {
        *self.done.lock()
    }

    /// 注册异步等待者的 waker
    pub(crate) fn register_waker(&self, waker: &Waker) {
        *self.waker.lock() = Some(waker.clone());
    }
}

/// 任务等待句柄（统一同步/异步等待）
pub struct TaskHandle<T> {
    /// 任务结果存储
    result: Arc<Mutex<Option<Result<T, ExecutorError>>>>,

    /// 完成信号
    completion: Arc<Completion>,

    /// 取消令牌
    cancel_token: tokio_util::sync::CancellationToken,

    /// 任务 ID（用于日志）
    task_id: TaskId,
}

impl<T> TaskHandle<T> {
    /// 创建新的任务句柄
    pub(crate) fn new(
        result: Arc<Mutex<Option<Result<T, ExecutorError>>>>,
        completion: Arc<Completion>,
        cancel_token: tokio_util::sync::CancellationToken,
        task_id: TaskId,
    ) -> Self {
        Self {
            result,
            completion,
            cancel_token,
            task_id,
        }
    }

    /// 同步阻塞等待任务完成
    pub fn join(self) -> Result<T, ExecutorError> {
        // 工作线程调用保护
        if is_worker_thread() {
            return Err(ExecutorError::DeadlockRisk(
                "Cannot call join() from worker thread, use spawn_async instead".to_string(),
            ));
        }

        // 阻塞等待完成
        self.completion.wait();

        // 提取结果
        self.take_result()
    }

    /// 带超时的同步阻塞等待
    pub fn join_timeout(self, timeout: Duration) -> Result<T, ExecutorError> {
        // 工作线程调用保护
        if is_worker_thread() {
            return Err(ExecutorError::DeadlockRisk(
                "Cannot call join_timeout() from worker thread".to_string(),
            ));
        }

        // 阻塞等待完成（带超时）
        let completed = self.completion.wait_timeout(timeout);

        if !completed {
            // 超时，取消任务
            self.cancel_token.cancel();
            return Err(ExecutorError::Timeout);
        }

        // 提取结果
        self.take_result()
    }

    /// 取消任务
    pub fn cancel(&self) {
        self.cancel_token.cancel();
    }

    /// 检查是否已完成
    pub fn is_done(&self) -> bool {
        self.completion.is_done()
    }

    /// 提取结果（内部方法）
    fn take_result(&self) -> Result<T, ExecutorError> {
        let mut result_guard = self.result.lock();
        result_guard.take().unwrap_or_else(|| {
            Err(ExecutorError::Internal(
                "Task result not available".to_string(),
            ))
        })
    }
}

/// 实现 Future trait，支持异步等待
impl<T> Future for TaskHandle<T> {
    type Output = Result<T, ExecutorError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // 检查是否已完成
        if self.completion.is_done() {
            return Poll::Ready(self.take_result());
        }

        // 注册 waker
        self.completion.register_waker(cx.waker());

        // 再次检查（避免竞态）
        if self.completion.is_done() {
            Poll::Ready(self.take_result())
        } else {
            Poll::Pending
        }
    }
}

impl<T> std::fmt::Debug for TaskHandle<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskHandle")
            .field("task_id", &self.task_id)
            .field("is_done", &self.is_done())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_task_id_uniqueness() {
        let id1 = TaskId::new();
        let id2 = TaskId::new();
        let id3 = TaskId::new();

        assert_ne!(id1, id2);
        assert_ne!(id2, id3);
        assert!(id1.as_u64() < id2.as_u64());
        assert!(id2.as_u64() < id3.as_u64());
    }

    #[test]
    fn test_task_execution() {
        use std::sync::atomic::AtomicBool;

        let executed = Arc::new(AtomicBool::new(false));
        let executed_clone = Arc::clone(&executed);

        let task = Task::new("test", Priority::Medium, move |_cancel| {
            executed_clone.store(true, Ordering::Relaxed);
        });

        task.execute();

        assert!(executed.load(Ordering::Relaxed));
    }

    #[test]
    fn test_task_cancellation() {
        use std::sync::atomic::AtomicBool;

        let executed = Arc::new(AtomicBool::new(false));
        let executed_clone = Arc::clone(&executed);

        let task = Task::new("test", Priority::Medium, move |cancel| {
            if cancel.is_cancelled() {
                return;
            }
            executed_clone.store(true, Ordering::Relaxed);
        });

        // 取消任务
        task.cancel();
        task.execute();

        // 不应执行
        assert!(!executed.load(Ordering::Relaxed));
    }

    #[test]
    fn test_task_deadline() {
        let meta = TaskMetadata::new("test", Priority::High)
            .with_deadline(Instant::now() - Duration::from_secs(1));

        // 已过期
        assert!(meta.is_expired());
    }

    #[test]
    fn test_task_ttl() {
        let mut meta = TaskMetadata::new("test", Priority::High).with_ttl(Duration::from_millis(1));

        // 手动设置 enqueued_at（模拟 PriorityQueue::push 行为）
        meta.enqueued_at = Some(Instant::now());

        std::thread::sleep(Duration::from_millis(10));

        // TTL 已过期
        assert!(meta.is_expired());
    }

    #[test]
    fn test_task_panic_isolation() {
        let task = Task::new("panic_task", Priority::Medium, |_cancel| {
            panic!("intentional panic");
        });

        // 不应该 panic，应被捕获
        task.execute();
    }
}
