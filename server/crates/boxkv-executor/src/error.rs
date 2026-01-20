use thiserror::Error;

/// Executor 错误类型
#[derive(Error, Debug)]
pub enum ExecutorError {
    #[error("任务队列已满: {0}")]
    QueueFull(String),

    #[error("配额超限: {0}")]
    QuotaExceeded(String),

    #[error("任务执行失败: {0}")]
    TaskFailed(String),

    #[error("运行时错误: {0}")]
    RuntimeError(String),

    #[error("任务超时")]
    Timeout,

    #[error("死锁风险: {0}")]
    DeadlockRisk(String),

    #[error("内部错误: {0}")]
    Internal(String),
}

pub type Result<T> = std::result::Result<T, ExecutorError>;

impl ExecutorError {
    pub fn queue_full(msg: impl Into<String>) -> Self {
        Self::QueueFull(msg.into())
    }

    pub fn quota_exceeded(msg: impl Into<String>) -> Self {
        Self::QuotaExceeded(msg.into())
    }

    pub fn task_failed(msg: impl Into<String>) -> Self {
        Self::TaskFailed(msg.into())
    }

    pub fn runtime_error(msg: impl Into<String>) -> Self {
        Self::RuntimeError(msg.into())
    }

    pub fn internal(msg: impl Into<String>) -> Self {
        Self::Internal(msg.into())
    }
}
