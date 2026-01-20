//! Cost 计费系统 - 基于字节数
//!
//! ## 设计原则
//! 1. **确定性**：字节数不变，cost 不变（不受 CPU/缓存影响）
//! 2. **公平性**：相同数据量 = 相同 cost
//! 3. **可配置**：单位/权重可调整
//!
//! ## Cost 单位
//! - 基准：4KB = 1 cost（对齐 OS/SSD 页大小）
//! - 计算：`cost = ceil(bytes / 4096)`
//!
//! ## 读写权重
//! - 写操作：10 倍权重（写放大、持久化开销）
//! - 读操作：1 倍权重（基准）
//! - 比例可配置

use serde::{Deserialize, Serialize};

/// Cost 单位：每 4KB 为 1 个 cost 单位
pub const COST_UNIT_BYTES: u64 = 4096;

/// 最小 cost（至少 1，即使字节数为 0）
pub const MIN_COST: u64 = 1;

/// 最大 cost（防止单任务过大）
/// 400MB = 100,000 * 4KB
pub const MAX_COST: u64 = 100_000;

/// 默认读权重
pub const DEFAULT_READ_WEIGHT: u64 = 1;

/// 默认写权重
pub const DEFAULT_WRITE_WEIGHT: u64 = 10;

/// EWMA 默认初始值（字节）
pub const DEFAULT_EWMA_INITIAL_VALUE: u64 = 4096;

/// EWMA 限制最小值（字节）
pub const EWMA_CLAMP_MIN_BYTES: u64 = 4096;

/// EWMA 限制最大值（字节）
pub const EWMA_CLAMP_MAX_BYTES: u64 = 1024 * 1024;

/// DRR quantum - Critical 优先级
pub const DRR_QUANTUM_CRITICAL: u64 = 100;

/// DRR quantum - High 优先级
pub const DRR_QUANTUM_HIGH: u64 = 500;

/// DRR quantum - Medium 优先级
pub const DRR_QUANTUM_MEDIUM: u64 = 2000;

/// 字节数转 cost（向上取整）
#[inline]
pub fn bytes_to_cost(bytes: u64) -> u64 {
    if bytes == 0 {
        return MIN_COST;
    }

    bytes.div_ceil(COST_UNIT_BYTES).clamp(MIN_COST, MAX_COST)
}

/// 读写计费权重
///
/// ## 默认配置
/// - 写：10 倍权重（写放大、持久化、Compaction 开销）
/// - 读：1 倍权重（基准，缓存命中率高）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CostWeights {
    /// 写操作权重（相对读）
    pub write_weight: u64,
    /// 读操作权重（基准）
    pub read_weight: u64,
}

impl Default for CostWeights {
    fn default() -> Self {
        Self {
            write_weight: DEFAULT_WRITE_WEIGHT,
            read_weight: DEFAULT_READ_WEIGHT,
        }
    }
}

impl CostWeights {
    /// 创建自定义权重
    pub fn new(read_weight: u64, write_weight: u64) -> Self {
        Self {
            read_weight: read_weight.max(1), // 至少为 1
            write_weight: write_weight.max(1),
        }
    }
}

/// Cost 全局配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CostConfig {
    /// Cost 单位（字节数）
    pub unit_bytes: u64,

    /// 读写权重
    pub weights: CostWeights,

    /// 最小 cost
    pub min_cost: u64,

    /// 最大 cost（防止单任务过大）
    pub max_cost: u64,
}

impl Default for CostConfig {
    fn default() -> Self {
        Self {
            unit_bytes: COST_UNIT_BYTES,
            weights: CostWeights::default(),
            min_cost: MIN_COST,
            max_cost: MAX_COST,
        }
    }
}

impl CostConfig {
    /// 创建自定义配置
    pub fn new(unit_bytes: u64, weights: CostWeights) -> Self {
        Self {
            unit_bytes: unit_bytes.max(1),
            weights,
            min_cost: MIN_COST,
            max_cost: MAX_COST,
        }
    }

    /// 验证配置合法性
    pub fn validate(&self) -> Result<(), String> {
        if self.unit_bytes == 0 {
            return Err("unit_bytes 不能为 0".to_string());
        }
        if self.min_cost == 0 {
            return Err("min_cost 不能为 0".to_string());
        }
        if self.max_cost < self.min_cost {
            return Err("max_cost 必须 >= min_cost".to_string());
        }
        Ok(())
    }
}

/// 工作负载类别
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WorkClass {
    /// 控制平面：状态查询/心跳/元数据
    ControlPlane,

    /// 前台小读：点查
    FrontendReadSmall,

    /// 前台大读：Scan
    FrontendReadLarge,

    /// 前台写：用户写请求
    FrontendWrite,

    /// 持久化：WAL/Flush
    Durability,

    /// 后台写放大：Compaction/GC
    BackgroundWriteAmp,

    /// 维护：统计/清理
    Maintenance,
}

/// 大小提示
#[derive(Debug, Clone, Copy)]
pub enum SizeHint {
    /// 已知字节数（写、Flush、Compaction）- 不需要 EWMA
    Bytes(u64),

    /// 读：只知道 key，value 用 EWMA 预测
    ReadKey { key_bytes: u64, scope: Scope },

    /// Scan：必须带 limit
    /// scope 是必需的，用于 EWMA 预测
    Scan {
        key_bytes: u64,
        limit_bytes: u64,
        scope: Scope,
    },
}

/// EWMA 统计的 scope（分桶维度）
/// 为多租户预留接口
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub enum Scope {
    /// 全局
    #[default]
    Global,
}

use crate::metrics::EwmaEstimator;
/// CostModel - 内部成本模型（调度器内部使用）
use crossbeam_skiplist::SkipMap;

pub struct CostModel {
    /// EWMA 估算器
    ewma_map: SkipMap<Scope, EwmaEstimator>,

    /// 读写权重配置
    weights: CostWeights,
}

impl Default for CostModel {
    fn default() -> Self {
        Self::new(CostWeights::default())
    }
}

impl CostModel {
    /// 创建 CostModel
    pub fn new(weights: CostWeights) -> Self {
        let map = SkipMap::new();
        // 初始化 Global scope 的 EWMA
        map.insert(
            Scope::Global,
            EwmaEstimator::default_with_initial(DEFAULT_EWMA_INITIAL_VALUE),
        );

        Self {
            ewma_map: map,
            weights,
        }
    }

    /// 估算任务成本
    pub fn estimate_cost(&self, work_class: WorkClass, size_hint: SizeHint) -> u64 {
        match size_hint {
            SizeHint::Bytes(bytes) => {
                // 已知字节数（写、Flush、Compaction）- 不需要 EWMA
                let base_cost = bytes_to_cost(bytes);
                match work_class {
                    WorkClass::FrontendWrite
                    | WorkClass::Durability
                    | WorkClass::BackgroundWriteAmp => {
                        (base_cost * self.weights.write_weight).max(MIN_COST)
                    }
                    _ => base_cost.max(MIN_COST),
                }
            }
            SizeHint::ReadKey { key_bytes, scope } => {
                // 读：使用 EWMA 预测 value_bytes（scope 已在 SizeHint 内部）
                let predicted_value_bytes = self.get_ewma_estimate(scope);
                let total_bytes = key_bytes + predicted_value_bytes;
                let base_cost = bytes_to_cost(total_bytes);
                (base_cost * self.weights.read_weight).max(MIN_COST)
            }
            SizeHint::Scan {
                key_bytes,
                limit_bytes,
                scope: _,
            } => {
                // Scan：使用 limit 限制（scope 已在 SizeHint 内部）
                let total_bytes = key_bytes + limit_bytes;
                let base_cost = bytes_to_cost(total_bytes);
                (base_cost * self.weights.read_weight).max(MIN_COST)
            }
        }
    }

    /// 观察实际读取的字节数，更新 EWMA
    pub fn observe_read(&self, value_bytes: u64, scope: Scope) {
        if let Some(entry) = self.ewma_map.get(&scope) {
            // 钳位到合理范围
            let clamped = value_bytes.clamp(EWMA_CLAMP_MIN_BYTES, EWMA_CLAMP_MAX_BYTES);
            entry.value().update(clamped);
        }
    }

    /// 获取 EWMA 估算值
    fn get_ewma_estimate(&self, scope: Scope) -> u64 {
        self.ewma_map
            .get(&scope)
            .map(|entry| entry.value().get())
            .unwrap_or(DEFAULT_EWMA_INITIAL_VALUE)
    }
}

/// Policy 配置（WorkClass → Priority + quantum）
use crate::priority::Priority;

pub struct Policy;

impl Policy {
    /// 从 WorkClass 映射到 Priority
    ///
    /// ## 映射规则
    /// - ControlPlane: Critical（必须立即响应）
    /// - FrontendReadSmall/Write: Critical（用户请求）
    /// - FrontendReadLarge: High（避免阻塞小读）
    /// - Durability: High（WAL/Flush 优先级高于后台）
    /// - BackgroundWriteAmp: Medium（Compaction）
    /// - Maintenance: Medium（清理/统计）
    pub fn priority(work_class: WorkClass) -> Priority {
        match work_class {
            WorkClass::ControlPlane => Priority::Critical,
            WorkClass::FrontendReadSmall => Priority::Critical,
            WorkClass::FrontendWrite => Priority::Critical,
            WorkClass::FrontendReadLarge => Priority::High,
            WorkClass::Durability => Priority::High,
            WorkClass::BackgroundWriteAmp => Priority::Medium,
            WorkClass::Maintenance => Priority::Medium,
        }
    }

    /// 获取 DRR quantum（每轮消耗的 cost 配额）
    ///
    /// ## 策略
    /// - Critical: 较小 quantum，避免阻塞其他任务
    /// - High: 中等 quantum
    /// - Medium: 较大 quantum，吞吐优先
    pub fn quantum(priority: Priority) -> u64 {
        match priority {
            Priority::Critical => DRR_QUANTUM_CRITICAL,
            Priority::High => DRR_QUANTUM_HIGH,
            Priority::Medium => DRR_QUANTUM_MEDIUM,
        }
    }
}

/// 反馈事件（任务完成后上报实际观测值）
#[derive(Debug, Clone)]
pub enum FeedbackEvent {
    /// 读请求完成，上报实际 value_bytes
    ReadComplete { scope: Scope, value_bytes: u64 },
}

impl FeedbackEvent {
    /// 创建读完成事件
    pub fn read_complete(value_bytes: u64) -> Self {
        Self::ReadComplete {
            scope: Scope::Global,
            value_bytes,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bytes_to_cost() {
        // 边界情况
        assert_eq!(bytes_to_cost(0), 1);
        assert_eq!(bytes_to_cost(1), 1);

        // 标准情况
        assert_eq!(bytes_to_cost(4096), 1);
        assert_eq!(bytes_to_cost(4097), 2); // 向上取整
        assert_eq!(bytes_to_cost(8192), 2);
        assert_eq!(bytes_to_cost(8193), 3);

        // 大数值
        assert_eq!(bytes_to_cost(1024 * 1024), 256); // 1MB = 256 cost
        assert_eq!(bytes_to_cost(64 * 1024 * 1024), 16384); // 64MB
    }

    #[test]
    fn test_cost_breakdown() {}

    #[test]
    fn test_cost_config_validation() {
        let mut config = CostConfig::default();
        assert!(config.validate().is_ok());

        // 非法配置
        config.unit_bytes = 0;
        assert!(config.validate().is_err());

        config.unit_bytes = 4096;
        config.max_cost = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_max_cost_clamp() {
        // 超大数据会被 clamp 到 MAX_COST
        let huge_bytes = 1024 * 1024 * 1024 * 1024; // 1TB
        let cost = bytes_to_cost(huge_bytes);
        assert_eq!(cost, MAX_COST);
    }
}
