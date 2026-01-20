use serde::{Deserialize, Serialize};

/// Executor 调度器配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutorConfig {
    // 线程池配置
    /// 工作线程数（默认 min(CPU 核数, 16)）
    pub max_worker_threads: usize,
    /// Medium 优先级任务最大占用线程比例（0.0~1.0，默认 0.5）
    pub medium_task_ratio: f64,
    /// 任务提交通道容量（默认 1024）
    pub task_queue_capacity: usize,

    // DRR 权重配置
    /// Critical 优先级 quantum（默认 8）
    pub quantum_critical: u64,
    /// High 优先级 quantum（默认 4）
    pub quantum_high: u64,
    /// Medium 优先级 quantum（默认 1）
    pub quantum_medium: u64,

    // Cost 计费配置
    /// Cost 计算单位（字节，默认 4096）
    pub cost_unit_bytes: u64,
    /// 最小 cost（默认 1）
    pub min_cost: u64,
    /// 最大 cost（默认 100,000）
    pub max_cost: u64,
    /// 写操作权重倍数（默认 10）
    pub cost_write_weight: u64,
    /// 读操作权重倍数（默认 1）
    pub cost_read_weight: u64,

    // EWMA 预测配置
    /// EWMA 初始值（字节，默认 4096）
    pub ewma_initial_value_bytes: u64,
    /// EWMA 限制最小值（字节，默认 4096）
    pub ewma_clamp_min_bytes: u64,
    /// EWMA 限制最大值（字节，默认 1MB）
    pub ewma_clamp_max_bytes: u64,
}

impl Default for ExecutorConfig {
    fn default() -> Self {
        // 获取 CPU 核数，至少 1 核
        let cpu = num_cpus::get().max(1);
        // 限制最大 16 个线程
        let max_worker_threads = cpu.min(16);

        Self {
            // 线程池
            max_worker_threads,
            medium_task_ratio: 0.5,
            task_queue_capacity: 1024,

            // DRR 权重
            quantum_critical: 8,
            quantum_high: 4,
            quantum_medium: 1,

            // Cost 计费
            cost_unit_bytes: 4096,
            min_cost: 1,
            max_cost: 100_000,
            cost_write_weight: 10,
            cost_read_weight: 1,

            // EWMA
            ewma_initial_value_bytes: 4096,
            ewma_clamp_min_bytes: 4096,
            ewma_clamp_max_bytes: 1024 * 1024,
        }
    }
}
