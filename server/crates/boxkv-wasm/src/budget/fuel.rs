//! Fuel-based CPU 预算管理
use crate::context::CallContext;
use crate::error::{Result, WasmError};
use wasmtime::Store;

/// Fuel 预算管理器
pub struct FuelBudget {
    /// 最大 fuel
    max_fuel: u64,

    /// 开始时的 fuel
    start_fuel: u64,
}

impl FuelBudget {
    /// 创建新的 Fuel 预算
    pub fn new(max_fuel: u64) -> Self {
        Self {
            max_fuel,
            start_fuel: max_fuel,
        }
    }

    /// 应用到 Store
    pub fn apply(&self, store: &mut Store<CallContext>) -> Result<()> {
        store
            .set_fuel(self.max_fuel)
            .map_err(|e| WasmError::InternalError(format!("Failed to set fuel: {}", e)))?;

        tracing::trace!("Set fuel budget: max_fuel={}", self.max_fuel);
        Ok(())
    }

    /// 获取消耗的 fuel
    pub fn consumed_fuel(&self, store: &Store<CallContext>) -> u64 {
        let remaining = store.get_fuel().unwrap_or(self.start_fuel);
        self.start_fuel.saturating_sub(remaining)
    }

    /// 检查是否耗尽
    pub fn is_exhausted(&self, store: &Store<CallContext>) -> bool {
        store.get_fuel().unwrap_or(0) == 0
    }

    /// 获取剩余 fuel
    pub fn remaining_fuel(&self, store: &Store<CallContext>) -> u64 {
        store.get_fuel().unwrap_or(0)
    }
}

/// Fuel 消耗统计
#[derive(Debug, Clone, Default)]
pub struct FuelStats {
    /// 消耗的 fuel
    pub consumed: u64,

    /// 最大 fuel
    pub max: u64,

    /// 使用率（0.0 ~ 1.0）
    pub usage_ratio: f64,
}

impl FuelStats {
    /// 从 Store 采集统计
    pub fn from_store(store: &Store<CallContext>, max_fuel: u64) -> Self {
        let remaining = store.get_fuel().unwrap_or(max_fuel);
        let consumed = max_fuel.saturating_sub(remaining);
        let usage_ratio = (consumed as f64) / (max_fuel as f64);

        Self {
            consumed,
            max: max_fuel,
            usage_ratio,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::BudgetConfig;
    use boxkv_core::hooks::DbView;
    use bytes::Bytes;
    use std::sync::Arc;
    use wasmtime::{Config, Engine, Linker, Module};

    // Mock DbView for testing
    pub struct MockDbView;
    impl DbView for MockDbView {
        fn kv_get(&self, _key: &[u8]) -> boxkv_core::db::error::Result<Option<Bytes>> {
            Ok(None)
        }
    }

    #[test]
    fn test_fuel_budget() {
        let mut config = Config::new();
        config.consume_fuel(true);
        let engine = Engine::new(&config).unwrap();

        // 简单模块
        let wasm = wat::parse_str(
            r#"
            (module
                (func (export "loop") (param i32)
                    (local i32)
                    (local.set 1 (i32.const 0))
                    (loop $continue
                        (local.set 1 (i32.add (local.get 1) (i32.const 1)))
                        (br_if $continue (i32.lt_u (local.get 1) (local.get 0)))
                    )
                )
            )
        "#,
        )
        .unwrap();

        let module = Module::new(&engine, wasm).unwrap();
        let linker = Linker::<CallContext>::new(&engine);

        let budget_cfg = BudgetConfig::default();
        let ctx = CallContext::new_readonly(
            Bytes::new(),
            Bytes::new(),
            None,
            Arc::new(MockDbView),
            budget_cfg,
            0,
        );

        let mut store = Store::new(&engine, ctx);

        // 设置 fuel
        let budget = FuelBudget::new(10000);
        budget.apply(&mut store).unwrap();

        let instance = linker.instantiate(&mut store, &module).unwrap();
        let func = instance
            .get_typed_func::<i32, ()>(&mut store, "loop")
            .unwrap();

        // 执行小循环
        func.call(&mut store, 10).unwrap();

        let consumed = budget.consumed_fuel(&store);
        assert!(consumed > 0, "Should consume some fuel");
        assert!(consumed < 10000, "Should not exhaust fuel");
    }

    #[test]
    fn test_fuel_exhaustion() {
        let mut config = Config::new();
        config.consume_fuel(true);
        let engine = Engine::new(&config).unwrap();

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

        let budget_cfg = BudgetConfig::default();
        let ctx = CallContext::new_readonly(
            Bytes::new(),
            Bytes::new(),
            None,
            Arc::new(MockDbView),
            budget_cfg,
            0,
        );

        let mut store = Store::new(&engine, ctx);

        // 设置很小的 fuel
        let budget = FuelBudget::new(1000);
        budget.apply(&mut store).unwrap();

        let instance = linker.instantiate(&mut store, &module).unwrap();
        let func = instance
            .get_typed_func::<(), ()>(&mut store, "infinite")
            .unwrap();

        // 应该因 fuel 耗尽而失败
        let result = func.call(&mut store, ());
        assert!(result.is_err(), "Should fail due to fuel exhaustion");
    }
}
