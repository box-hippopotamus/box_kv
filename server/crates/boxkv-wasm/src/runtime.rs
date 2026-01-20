//! Wasm Runtime 核心

use crate::RuntimeConfig;
use crate::budget::BudgetManager;
use crate::context::CallContext;
use crate::error::{Result, WasmError};
use crate::plugin::{PluginId, PluginService};
use crate::pool::{InstancePoolManager, ModuleCache};
use boxkv_core::hooks::PreWriteAction;
use bytes::Bytes;
use std::sync::Arc;
use wasmtime::{Engine, Store};

/// Wasm Runtime
pub struct WasmRuntime {
    /// Wasmtime 引擎
    engine: Arc<Engine>,

    /// 配置（公开给 provider）
    pub(crate) config: RuntimeConfig,

    /// Module 缓存
    module_cache: Arc<ModuleCache>,

    /// 实例池管理器
    pool_manager: Arc<InstancePoolManager>,

    /// 预算管理器
    budget_manager: Arc<BudgetManager>,

    /// 插件服务
    plugin_service: Arc<PluginService>,
}

impl WasmRuntime {
    /// 从全局配置创建 Runtime
    pub fn from_global_config(plugin_service: Arc<PluginService>) -> Result<Self> {
        let global_config = boxkv_common::config::GlobalConfig::get();
        Self::new(global_config.wasm.runtime.clone(), plugin_service)
    }

    /// 创建新的 Runtime
    pub fn new(config: RuntimeConfig, plugin_service: Arc<PluginService>) -> Result<Self> {
        let mut wasmtime_config = wasmtime::Config::new();
        wasmtime_config.consume_fuel(true);
        wasmtime_config.epoch_interruption(true);

        let engine = Arc::new(Engine::new(&wasmtime_config)?);
        let module_cache = Arc::new(ModuleCache::new(engine.clone(), &config.cache));
        let pool_manager = Arc::new(InstancePoolManager::new(
            engine.clone(),
            config.pool.clone(),
        ));
        let budget_manager = Arc::new(BudgetManager::new(engine.clone(), config.epoch_tick_ms)?);

        Ok(Self {
            engine,
            config: config.clone(),
            module_cache,
            pool_manager,
            budget_manager,
            plugin_service,
        })
    }

    /// 加载模块（使用缓存）
    pub fn load_module(&self, plugin_id: PluginId, wasm_bytes: Bytes) -> Result<()> {
        self.module_cache.get_or_compile(plugin_id, wasm_bytes)?;
        Ok(())
    }

    /// 从 PluginService 加载模块（如果尚未加载）
    pub fn load_by_id(&self, id: &PluginId) -> Result<()> {
        // 检查缓存
        if self.module_cache.get(id).is_some() {
            return Ok(());
        }

        let record = self
            .plugin_service
            .registry()
            .get_record(id)?
            .ok_or_else(|| WasmError::PluginIdNotFound(format!("{:?}", id)))?;

        // 从 BlobStore 获取 wasm 字节
        let wasm_bytes = self.plugin_service.blobs().get(&record.fingerprint)?;

        // 加载模块（Vec<u8> -> Bytes）
        self.load_module(*id, Bytes::from(wasm_bytes))?;

        Ok(())
    }

    /// 检查插件是否有更新
    pub fn check_update_available(&self, id: &PluginId) -> Result<Option<(PluginId, String)>> {
        let (is_latest, latest_id, latest_fp) = self.plugin_service.is_latest(id)?;

        if !is_latest {
            if let (Some(latest_id), Some(latest_fp)) = (latest_id, latest_fp) {
                return Ok(Some((latest_id, latest_fp.to_string())));
            }
        }

        Ok(None)
    }

    /// 执行 PreWrite Hook
    pub fn execute_pre_write(
        &self,
        plugin_id: PluginId,
        ctx: &CallContext,
    ) -> Result<PreWriteAction> {
        // 1. 从缓存获取 Module
        let module = self
            .module_cache
            .get(&plugin_id)
            .ok_or_else(|| WasmError::PluginIdNotFound(format!("{:?}", plugin_id)))?;

        // 2. 获取实例 Guard（并发控制）
        let _guard = tokio::runtime::Handle::current()
            .block_on(self.pool_manager.acquire_instance_guard(plugin_id))?;

        // 3. 获取 Linker（共享）
        let linker = self.pool_manager.get_linker(plugin_id)?;

        // 4. 创建 CallContext 和 Store
        let new_ctx = CallContext::new_writable(
            ctx.handle_table.key_bytes().clone(),
            ctx.handle_table.value_bytes().clone(),
            ctx.handle_table.expires_at_opt(),
            ctx.db_view.clone(),
            self.config.budget.clone(),
        );

        let mut store = Store::new(self.engine.as_ref(), new_ctx);

        // 5. 附加预算（Fuel + Epoch + Memory）
        let budget_guard =
            self.budget_manager
                .attach(&mut store, plugin_id, &self.config.budget)?;

        // 6. 实例化
        let instance = linker.instantiate(&mut store, &module)?;

        let pre_write = instance
            .get_typed_func::<(), i32>(&mut store, "pre_write")
            .map_err(|_| WasmError::FunctionNotFound("pre_write".to_string()))?;

        let result = pre_write
            .call(&mut store, ())
            .map_err(|e| WasmError::Trap(e.to_string()))?;

        let final_ctx = store.data();

        // 采集预算指标
        let metrics = budget_guard.collect_metrics(&store);
        metrics.log();

        // 解析返回值
        match result {
            0 => Ok(PreWriteAction::Accept),
            1 => {
                let reason = final_ctx
                    .reject_reason()
                    .unwrap_or("Plugin rejected")
                    .to_string();
                Ok(PreWriteAction::Reject(reason))
            }
            2 => {
                let commands = final_ctx.commands();
                Ok(PreWriteAction::Transform(commands))
            }
            _ => Err(WasmError::InternalError(format!(
                "Invalid return code: {}",
                result
            ))),
        }
    }

    /// 执行 PostWrite Hook
    ///
    /// 与 PreWrite 的关键区别：
    /// - 使用只读 CallContext（sequence 已分配）
    /// - 不关心返回的变更指令（只读模式下 cmd_* 会返回 -6）
    /// - 返回值仅用于检测执行成功/失败
    pub fn execute_post_write(&self, plugin_id: PluginId, ctx: &CallContext) -> Result<()> {
        let module = self
            .module_cache
            .get(&plugin_id)
            .ok_or_else(|| WasmError::PluginIdNotFound(format!("{:?}", plugin_id)))?;

        let _guard = tokio::runtime::Handle::current()
            .block_on(self.pool_manager.acquire_instance_guard(plugin_id))?;

        let linker = self.pool_manager.get_linker(plugin_id)?;

        let new_ctx = CallContext::new_readonly(
            ctx.handle_table.key_bytes().clone(),
            ctx.handle_table.value_bytes().clone(),
            ctx.handle_table.expires_at_opt(),
            ctx.db_view.clone(),
            self.config.budget.clone(),
            ctx.ctx_sequence(),
        );

        let mut store = Store::new(self.engine.as_ref(), new_ctx);
        let budget_guard =
            self.budget_manager
                .attach(&mut store, plugin_id, &self.config.budget)?;

        let instance = linker.instantiate(&mut store, &module)?;

        let post_write = instance
            .get_typed_func::<(), i32>(&mut store, "post_write")
            .map_err(|_| WasmError::FunctionNotFound("post_write".to_string()))?;

        let result = post_write
            .call(&mut store, ())
            .map_err(|e| WasmError::Trap(e.to_string()))?;

        let metrics = budget_guard.collect_metrics(&store);
        metrics.log();

        // PostWrite 返回值约定：0=Success，非 0=失败
        if result != 0 {
            return Err(WasmError::InternalError(format!(
                "PostWrite failed with code {}",
                result
            )));
        }

        tracing::debug!("PostWrite executed: plugin={:?}", plugin_id);
        Ok(())
    }

    /// 执行 OnRead Hook
    ///
    /// 返回码约定：
    /// - 0: Accept（返回原值）
    /// - 1: Reject（读取 reject_reason）
    /// - 2: Transform（读取 transformed_value）
    pub fn execute_on_read(
        &self,
        plugin_id: PluginId,
        key: Bytes,
        value: Bytes,
        ctx: &CallContext,
    ) -> Result<boxkv_core::hooks::OnReadAction> {
        let module = self
            .module_cache
            .get(&plugin_id)
            .ok_or_else(|| WasmError::PluginIdNotFound(format!("{:?}", plugin_id)))?;

        let _guard = tokio::runtime::Handle::current()
            .block_on(self.pool_manager.acquire_instance_guard(plugin_id))?;

        let linker = self.pool_manager.get_linker(plugin_id)?;

        let new_ctx = CallContext::new_readonly(
            key.clone(),
            value.clone(),
            None,
            ctx.db_view.clone(),
            self.config.budget.clone(),
            0,
        );

        let mut store = Store::new(self.engine.as_ref(), new_ctx);
        let budget_guard =
            self.budget_manager
                .attach(&mut store, plugin_id, &self.config.budget)?;

        let instance = linker.instantiate(&mut store, &module)?;

        let on_read = instance
            .get_typed_func::<(), i32>(&mut store, "on_read")
            .map_err(|_| WasmError::FunctionNotFound("on_read".to_string()))?;

        let result = on_read
            .call(&mut store, ())
            .map_err(|e| WasmError::Trap(e.to_string()))?;

        let final_ctx = store.data();

        // 采集预算指标
        let metrics = budget_guard.collect_metrics(&store);
        metrics.log();

        // 解析返回值（封装为 ValueType::Normal，wasm 层不处理 TTL）
        match result {
            0 => Ok(boxkv_core::hooks::OnReadAction::Accept(
                boxkv_common::types::ValueType::Normal(value),
            )),
            1 => {
                let reason = final_ctx
                    .read_get_reject_reason()
                    .unwrap_or("Plugin rejected")
                    .to_string();
                Ok(boxkv_core::hooks::OnReadAction::Reject(reason))
            }
            2 => {
                let transformed = final_ctx
                    .read_get_transformed_value()
                    .ok_or_else(|| {
                        WasmError::InternalError("Transform returned but no value set".to_string())
                    })?
                    .clone();
                Ok(boxkv_core::hooks::OnReadAction::Transform(
                    boxkv_common::types::ValueType::Normal(transformed),
                ))
            }
            _ => Err(WasmError::InternalError(format!(
                "Invalid return code: {}",
                result
            ))),
        }
    }

    /// 执行 ScanFilter Hook
    ///
    /// 返回码约定：
    /// - 0: Keep
    /// - 1: Drop
    pub fn execute_scan_filter(
        &self,
        plugin_id: PluginId,
        key: Bytes,
        value: Bytes,
        ctx: &CallContext,
    ) -> Result<boxkv_core::hooks::ScanFilterAction> {
        let module = self
            .module_cache
            .get(&plugin_id)
            .ok_or_else(|| WasmError::PluginIdNotFound(format!("{:?}", plugin_id)))?;

        let _guard = tokio::runtime::Handle::current()
            .block_on(self.pool_manager.acquire_instance_guard(plugin_id))?;

        let linker = self.pool_manager.get_linker(plugin_id)?;

        let new_ctx = CallContext::new_readonly(
            key.clone(),
            value.clone(),
            None,
            ctx.db_view.clone(),
            self.config.budget.clone(),
            0,
        );

        let mut store = Store::new(self.engine.as_ref(), new_ctx);
        let budget_guard =
            self.budget_manager
                .attach(&mut store, plugin_id, &self.config.budget)?;

        let instance = linker.instantiate(&mut store, &module)?;

        let scan_filter = instance
            .get_typed_func::<(), i32>(&mut store, "scan_filter")
            .map_err(|_| WasmError::FunctionNotFound("scan_filter".to_string()))?;

        let result = scan_filter
            .call(&mut store, ())
            .map_err(|e| WasmError::Trap(e.to_string()))?;

        let final_ctx = store.data();

        // 采集预算指标
        let metrics = budget_guard.collect_metrics(&store);
        metrics.log();

        // 解析返回值
        match result {
            0 => Ok(boxkv_core::hooks::ScanFilterAction::Keep),
            1 => Ok(boxkv_core::hooks::ScanFilterAction::Drop),
            _ => {
                // 检查 ctx.read_is_drop() 作为备用
                if final_ctx.read_is_drop() {
                    Ok(boxkv_core::hooks::ScanFilterAction::Drop)
                } else {
                    Ok(boxkv_core::hooks::ScanFilterAction::Keep)
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plugin::{FsBlobStore, FsRegistry, PluginService};
    use tempfile::TempDir;

    #[test]
    fn test_runtime_creation() {
        let tmp = TempDir::new().unwrap();
        let blobs = Arc::new(FsBlobStore::new(tmp.path().join("blobs")).unwrap());
        let registry = Arc::new(FsRegistry::new(tmp.path().join("registry")).unwrap());
        let service = Arc::new(PluginService::new(blobs, registry));

        let config = RuntimeConfig::default();
        let runtime = WasmRuntime::new(config, service);
        assert!(runtime.is_ok());
    }
}
