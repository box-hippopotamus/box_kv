//! 预算控制模块

mod epoch;
mod fuel;
mod limiter;

pub use epoch::{EpochDeadline, EpochTicker, TickerStats};
pub use fuel::{FuelBudget, FuelStats};
pub use limiter::{LimiterStats, MemoryLimiter, MemoryLimiterManager};

use std::sync::Arc;
use std::time::Instant;
use wasmtime::{Engine, Store};

use crate::BudgetConfig;
use crate::context::CallContext;
use crate::error::Result;
use crate::plugin::PluginId;

/// 预算管理器
pub struct BudgetManager {
    /// Epoch ticker
    epoch_ticker: Arc<EpochTicker>,

    /// 内存限制器管理
    memory_manager: Arc<MemoryLimiterManager>,
}

impl BudgetManager {
    /// 创建新的预算管理器
    pub fn new(engine: Arc<Engine>, tick_ms: u64) -> Result<Self> {
        let epoch_ticker = Arc::new(EpochTicker::new(engine, tick_ms)?);
        let memory_manager = Arc::new(MemoryLimiterManager::new());

        tracing::info!("BudgetManager initialized: tick_ms={}", tick_ms);

        Ok(Self {
            epoch_ticker,
            memory_manager,
        })
    }

    /// 为一次执行附加预算
    pub fn attach(
        &self,
        store: &mut Store<CallContext>,
        plugin_id: PluginId,
        config: &BudgetConfig,
    ) -> Result<BudgetGuard> {
        // 1. 设置 Fuel
        let fuel_budget = FuelBudget::new(config.max_fuel);
        fuel_budget.apply(store)?;

        // 2. 设置 Epoch 截止时间
        let epoch_deadline = EpochDeadline::new(&self.epoch_ticker, config.timeout_ms);
        epoch_deadline.apply(store);

        // 3. 设置内存限制器到 context
        let mem_hard = config.max_memory_bytes;
        let mem_soft = (mem_hard as f64 * 0.7) as u64; // 70% 为软限制
        let limiter = self
            .memory_manager
            .create_limiter(plugin_id, mem_hard, mem_soft);
        store.data_mut().memory_limiter = Some(limiter);

        tracing::debug!(
            "Budget attached: plugin={:?}, fuel={}, timeout_ms={}, mem_hard={}, mem_soft={}",
            plugin_id,
            config.max_fuel,
            config.timeout_ms,
            mem_hard,
            mem_soft
        );

        Ok(BudgetGuard {
            plugin_id,
            start_time: Instant::now(),
            max_fuel: config.max_fuel,
            epoch_ticker: self.epoch_ticker.clone(),
            epoch_deadline,
        })
    }

    /// 获取 Epoch ticker
    pub fn epoch_ticker(&self) -> &Arc<EpochTicker> {
        &self.epoch_ticker
    }

    /// 获取内存管理器
    pub fn memory_manager(&self) -> &Arc<MemoryLimiterManager> {
        &self.memory_manager
    }

    /// 优雅关闭
    pub fn shutdown(&self) {
        self.epoch_ticker.shutdown();
        tracing::info!("BudgetManager shutdown");
    }
}

impl Drop for BudgetManager {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// 预算 Guard（RAII）
pub struct BudgetGuard {
    plugin_id: PluginId,
    start_time: Instant,
    max_fuel: u64,
    epoch_ticker: Arc<EpochTicker>,
    epoch_deadline: EpochDeadline,
}

impl BudgetGuard {
    /// 采集执行完成后的指标
    pub fn collect_metrics(&self, store: &Store<CallContext>) -> BudgetMetrics {
        let duration = self.start_time.elapsed();
        let fuel_stats = FuelStats::from_store(store, self.max_fuel);

        let consumed_epochs = self.epoch_deadline.consumed_epochs(&self.epoch_ticker);
        let is_timeout = self.epoch_deadline.is_timeout(&self.epoch_ticker);

        BudgetMetrics {
            plugin_id: self.plugin_id,
            duration_ms: duration.as_millis() as u64,
            fuel_consumed: fuel_stats.consumed,
            fuel_max: fuel_stats.max,
            fuel_usage_ratio: fuel_stats.usage_ratio,
            consumed_epochs,
            is_timeout,
        }
    }
}

/// 预算指标
#[derive(Debug, Clone)]
pub struct BudgetMetrics {
    /// 插件 ID
    pub plugin_id: PluginId,

    /// 执行时长（毫秒）
    pub duration_ms: u64,

    /// 消耗的 fuel
    pub fuel_consumed: u64,

    /// 最大 fuel
    pub fuel_max: u64,

    /// Fuel 使用率
    pub fuel_usage_ratio: f64,

    /// 消耗的 epoch 数
    pub consumed_epochs: u64,

    /// 是否超时
    pub is_timeout: bool,
}

impl BudgetMetrics {
    /// 记录到日志
    pub fn log(&self) {
        if self.is_timeout {
            tracing::warn!(
                "Budget metrics (TIMEOUT): plugin={:?}, duration={}ms, fuel={}/{} ({:.2}%), epochs={}",
                self.plugin_id,
                self.duration_ms,
                self.fuel_consumed,
                self.fuel_max,
                self.fuel_usage_ratio * 100.0,
                self.consumed_epochs
            );
        } else if self.fuel_usage_ratio > 0.9 {
            tracing::warn!(
                "Budget metrics (HIGH FUEL): plugin={:?}, duration={}ms, fuel={}/{} ({:.2}%)",
                self.plugin_id,
                self.duration_ms,
                self.fuel_consumed,
                self.fuel_max,
                self.fuel_usage_ratio * 100.0
            );
        } else {
            tracing::debug!(
                "Budget metrics: plugin={:?}, duration={}ms, fuel={}/{} ({:.2}%)",
                self.plugin_id,
                self.duration_ms,
                self.fuel_consumed,
                self.fuel_max,
                self.fuel_usage_ratio * 100.0
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use boxkv_core::hooks::DbView;
    use bytes::Bytes;
    use wasmtime::{Config, Linker, Module};

    pub struct MockDbView;
    impl DbView for MockDbView {
        fn kv_get(&self, _key: &[u8]) -> boxkv_core::db::error::Result<Option<Bytes>> {
            Ok(None)
        }
    }

    #[test]
    fn test_budget_manager_basic() {
        let mut config = Config::new();
        config.consume_fuel(true);
        config.epoch_interruption(true);

        let engine = Arc::new(Engine::new(&config).unwrap());
        let manager = BudgetManager::new(engine.clone(), 10).unwrap();

        // 简单测试
        let wasm = wat::parse_str(
            r#"
            (module
                (func (export "test") (result i32)
                    i32.const 42
                )
            )
        "#,
        )
        .unwrap();

        let module = Module::new(&engine, wasm).unwrap();
        let linker = Linker::<CallContext>::new(&engine);

        let budget_cfg = BudgetConfig {
            max_fuel: 100000,
            timeout_ms: 100,
            max_memory_bytes: 64 * 1024 * 1024,
            max_kv_get_count: 100,
            max_bytes_read_total: 10 * 1024 * 1024,
        };

        let ctx = CallContext::new_readonly(
            Bytes::new(),
            Bytes::new(),
            None,
            Arc::new(MockDbView),
            budget_cfg.clone(),
            0,
        );

        let mut store = Store::new(&engine, ctx);
        let plugin_id = PluginId::from_uuid(uuid::Uuid::new_v4());

        // 附加预算
        let guard = manager.attach(&mut store, plugin_id, &budget_cfg).unwrap();

        // 执行
        let instance = linker.instantiate(&mut store, &module).unwrap();
        let func = instance
            .get_typed_func::<(), i32>(&mut store, "test")
            .unwrap();
        let result = func.call(&mut store, ()).unwrap();
        assert_eq!(result, 42);

        // 采集指标
        let metrics = guard.collect_metrics(&store);
        assert!(metrics.fuel_consumed > 0);
        assert!(!metrics.is_timeout);

        metrics.log();

        manager.shutdown();
    }

    #[test]
    fn test_epoch_timeout() {
        let mut config = Config::new();
        config.consume_fuel(true);
        config.epoch_interruption(true);

        let engine = Arc::new(Engine::new(&config).unwrap());
        let manager = BudgetManager::new(engine.clone(), 10).unwrap();

        // 死循环模块
        let wasm = wat::parse_str(
            r#"
            (module
                (func (export "infinite")
                    (loop $continue
                        (br $continue)
                    )
                )
            )
        "#,
        )
        .unwrap();

        let module = Module::new(&engine, wasm).unwrap();
        let linker = Linker::<CallContext>::new(&engine);

        let budget_cfg = BudgetConfig {
            max_fuel: 1000000,
            timeout_ms: 50, // 50ms 超时
            max_memory_bytes: 64 * 1024 * 1024,
            max_kv_get_count: 100,
            max_bytes_read_total: 10 * 1024 * 1024,
        };

        let ctx = CallContext::new_readonly(
            Bytes::new(),
            Bytes::new(),
            None,
            Arc::new(MockDbView),
            budget_cfg.clone(),
            0,
        );

        let mut store = Store::new(&engine, ctx);
        let plugin_id = PluginId::from_uuid(uuid::Uuid::new_v4());

        let _guard = manager.attach(&mut store, plugin_id, &budget_cfg).unwrap();

        let instance = linker.instantiate(&mut store, &module).unwrap();
        let func = instance
            .get_typed_func::<(), ()>(&mut store, "infinite")
            .unwrap();

        // 应该因超时而失败
        let result = func.call(&mut store, ());
        assert!(result.is_err(), "Should timeout");

        manager.shutdown();
    }
}
