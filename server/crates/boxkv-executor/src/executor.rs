use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::runtime::Runtime;

/// 后台任务的 Future 类型
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// 后台执行器抽象
pub trait BackgroundExecutor: Send + Sync {
    /// 提交 CPU 密集型任务（阻塞）
    fn spawn(&self, job: Box<dyn FnOnce() + Send + 'static>);
}

/// 基于 Tokio 的执行器实现
pub struct TokioExecutor {
    runtime: Arc<Runtime>,
}

impl TokioExecutor {
    /// 创建 Tokio 执行器
    pub fn new(worker_threads: usize, max_blocking_threads: usize) -> std::io::Result<Self> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(worker_threads)
            .max_blocking_threads(max_blocking_threads)
            .thread_name("boxkv-executor")
            .enable_all()
            .build()?;

        tracing::info!(
            "Tokio executor initialized: worker={}, blocking={}",
            worker_threads,
            max_blocking_threads
        );

        Ok(Self {
            runtime: Arc::new(runtime),
        })
    }

    /// 使用默认配置创建
    pub fn default_config() -> std::io::Result<Self> {
        let cpus = num_cpus::get();
        Self::new(cpus, cpus * 2)
    }

    /// 获取 Runtime 引用
    pub fn runtime(&self) -> &Runtime {
        &self.runtime
    }
}

impl BackgroundExecutor for TokioExecutor {
    fn spawn(&self, job: Box<dyn FnOnce() + Send + 'static>) {
        self.runtime.spawn_blocking(move || {
            job();
        });
    }
}

/// 基于标准库线程的简单执行器
pub struct StdThreadExecutor;

impl BackgroundExecutor for StdThreadExecutor {
    fn spawn(&self, job: Box<dyn FnOnce() + Send + 'static>) {
        std::thread::spawn(job);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    #[test]
    fn test_tokio_executor_spawn_blocking() {
        let executor = TokioExecutor::default_config().unwrap();
        let counter = Arc::new(AtomicUsize::new(0));

        for _ in 0..10 {
            let c = Arc::clone(&counter);
            executor.spawn(Box::new(move || {
                c.fetch_add(1, Ordering::Relaxed);
            }));
        }

        // 等待任务完成
        std::thread::sleep(Duration::from_millis(100));
        assert_eq!(counter.load(Ordering::Relaxed), 10);
    }

    #[test]
    fn test_std_executor() {
        let executor = StdThreadExecutor;
        let counter = Arc::new(AtomicUsize::new(0));

        for _ in 0..5 {
            let c = Arc::clone(&counter);
            executor.spawn(Box::new(move || {
                c.fetch_add(1, Ordering::Relaxed);
            }));
        }

        std::thread::sleep(Duration::from_millis(100));
        assert_eq!(counter.load(Ordering::Relaxed), 5);
    }
}
