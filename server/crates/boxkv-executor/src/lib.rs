//! # boxkv-executor: 全局任务调度器
//!
//! 提供统一的任务调度和执行框架，支持：
//! - 三级优先级队列（Critical / High / Medium）
//! - CPU 密集型任务（通过 TaskSpec 提交）
//! - DRR（Deficit Round Robin）公平调度

pub mod cost;
pub mod error;
pub mod executor;
pub mod metrics;
pub mod priority;
pub mod quota;
pub mod scheduler;
pub mod task;

pub use cost::{
    COST_UNIT_BYTES, CostConfig, CostModel, CostWeights, FeedbackEvent, MAX_COST, MIN_COST, Policy,
    Scope, SizeHint, WorkClass, bytes_to_cost,
};
pub use error::ExecutorError;
pub use executor::{BackgroundExecutor, TokioExecutor};
pub use metrics::EwmaEstimator;
pub use priority::{Priority, PriorityQueue};
pub use scheduler::{GlobalScheduler, SchedulerConfig};
pub use task::{Task, TaskHandle, TaskId, TaskMetadata, TaskSpec};
