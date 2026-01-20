use crate::error::{ExecutorError, Result};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// 资源配额管理器
pub struct QuotaManager {
    /// 最大并发数
    max_concurrency: AtomicUsize,

    /// 当前运行中任务数
    current_tasks: AtomicUsize,
}

impl QuotaManager {
    /// 创建配额管理器
    pub fn new(max_concurrency: usize) -> Self {
        Self {
            max_concurrency: AtomicUsize::new(max_concurrency),
            current_tasks: AtomicUsize::new(0),
        }
    }

    /// 尝试获取执行配额
    pub fn try_acquire(self: &Arc<Self>) -> Result<QuotaGuard> {
        let max = self.max_concurrency.load(Ordering::Relaxed);

        // 0 表示不限制
        if max == 0 {
            self.current_tasks.fetch_add(1, Ordering::Relaxed);
            return Ok(QuotaGuard::new(Arc::clone(self)));
        }

        // CAS 增加计数
        let mut current = self.current_tasks.load(Ordering::Relaxed);
        loop {
            if current >= max {
                return Err(ExecutorError::quota_exceeded(format!(
                    "并发任务数已达上限: {}/{}",
                    current, max
                )));
            }

            match self.current_tasks.compare_exchange_weak(
                current,
                current + 1,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Ok(QuotaGuard::new(Arc::clone(self))),
                Err(new) => current = new,
            }
        }
    }

    /// 释放配额
    fn release(&self) {
        self.current_tasks.fetch_sub(1, Ordering::Relaxed);
    }

    /// 获取当前任务数
    pub fn current_tasks(&self) -> usize {
        self.current_tasks.load(Ordering::Relaxed)
    }

    /// 获取最大并发数
    pub fn max_concurrency(&self) -> usize {
        self.max_concurrency.load(Ordering::Relaxed)
    }

    /// 动态调整最大并发数
    pub fn set_max_concurrency(&self, new_max: usize) {
        self.max_concurrency.store(new_max, Ordering::Relaxed);
        tracing::info!("配额上限已调整为: {}", new_max);
    }
}

/// 配额守卫（RAII）
pub struct QuotaGuard {
    manager: Arc<QuotaManager>,
}

impl QuotaGuard {
    fn new(manager: Arc<QuotaManager>) -> Self {
        Self { manager }
    }
}

impl Drop for QuotaGuard {
    fn drop(&mut self) {
        self.manager.release();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_quota_unlimited() {
        let quota = Arc::new(QuotaManager::new(0)); // 不限制

        let mut guards = Vec::new();
        for _ in 0..1000 {
            guards.push(quota.try_acquire().unwrap());
        }

        assert_eq!(quota.current_tasks(), 1000);
    }

    #[test]
    fn test_quota_limit() {
        let quota = Arc::new(QuotaManager::new(10));

        let mut guards = Vec::new();
        for _ in 0..10 {
            guards.push(quota.try_acquire().unwrap());
        }

        // 第 11 个应该失败
        assert!(quota.try_acquire().is_err());
        assert_eq!(quota.current_tasks(), 10);
    }

    #[test]
    fn test_quota_guard_drop() {
        let quota = Arc::new(QuotaManager::new(5));

        {
            let _guard1 = quota.try_acquire().unwrap();
            let _guard2 = quota.try_acquire().unwrap();
            assert_eq!(quota.current_tasks(), 2);
        } // guards drop here

        assert_eq!(quota.current_tasks(), 0);
    }

    #[test]
    fn test_quota_dynamic_adjust() {
        let quota = Arc::new(QuotaManager::new(5));
        assert_eq!(quota.max_concurrency(), 5);

        quota.set_max_concurrency(10);
        assert_eq!(quota.max_concurrency(), 10);
    }

    #[test]
    fn test_quota_concurrent_access() {
        use std::sync::Arc;
        use std::thread;

        let quota = Arc::new(QuotaManager::new(100));
        let mut handles = vec![];

        for _ in 0..10 {
            let q = Arc::clone(&quota);
            handles.push(thread::spawn(move || {
                for _ in 0..100 {
                    let _guard = q.try_acquire().unwrap();
                    thread::sleep(std::time::Duration::from_micros(1));
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(quota.current_tasks(), 0);
    }
}
