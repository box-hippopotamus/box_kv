//! WasmHookProvider - HookProvider 实现

use crate::context::CallContext;
use crate::error::Result;
use crate::runtime::WasmRuntime;
use boxkv_common::types::ValueType;
use boxkv_core::{
    HookContext, HookProvider, WasmCallPlan,
    hooks::{OnReadAction, PreWriteAction, ScanFilterAction},
};
use bytes::Bytes;
use std::sync::Arc;

/// Wasm Hook Provider
pub struct WasmHookProvider {
    /// Runtime
    runtime: Arc<WasmRuntime>,
}

impl WasmHookProvider {
    /// 创建新的 Provider
    pub fn new(runtime: Arc<WasmRuntime>) -> Self {
        Self { runtime }
    }

    /// 检查是否启用（从全局配置读取）
    fn is_enabled(&self) -> bool {
        if let Some(config) = boxkv_common::config::GlobalConfig::try_get() {
            config.wasm.enabled
        } else {
            // 如果全局配置未初始化，默认启用（用于测试）
            true
        }
    }
}

impl HookProvider for WasmHookProvider {
    fn pre_write(
        &self,
        ctx: &HookContext,
        plan: &WasmCallPlan,
        write_ctx: &boxkv_core::hooks::WriteContext,
    ) -> boxkv_core::db::error::Result<PreWriteAction> {
        if !self.is_enabled() || plan.is_empty() {
            return Ok(PreWriteAction::Accept);
        }

        let plugins = match plan.get(boxkv_core::hooks::HookType::PreWrite) {
            Some(list) => list,
            None => return Ok(PreWriteAction::Accept),
        };

        // 构造调用上下文
        let mut call_ctx = CallContext::new_writable(
            write_ctx.key().read_all(),
            write_ctx.value().read_all(),
            write_ctx.expires_at(),
            ctx.db_view.clone(),
            self.runtime.config.budget.clone(),
        );

        // 按序执行插件
        for plugin_spec in plugins {
            let plugin_id = crate::plugin::PluginId::from_uuid(plugin_spec.id);

            // 先加载插件（如果尚未加载）
            if let Err(e) = self.runtime.load_by_id(&plugin_id) {
                return Err(boxkv_core::db::error::DBError::PluginRejected(format!(
                    "Failed to load plugin {}: {}",
                    plugin_id, e
                )));
            }

            match self.runtime.execute_pre_write(plugin_id, &call_ctx) {
                Ok(action) => match action {
                    PreWriteAction::Accept => continue,
                    PreWriteAction::Reject(reason) => {
                        return Err(boxkv_core::db::error::DBError::PluginRejected(reason));
                    }
                    PreWriteAction::Transform(commands) => {
                        call_ctx.apply_commands(&commands);
                    }
                },
                Err(e) => {
                    return Err(boxkv_core::db::error::DBError::PluginRejected(
                        e.to_string(),
                    ));
                }
            }
        }

        if call_ctx.has_changes() {
            Ok(PreWriteAction::Transform(call_ctx.collect_commands()))
        } else {
            Ok(PreWriteAction::Accept)
        }
    }

    fn post_write(
        &self,
        ctx: &HookContext,
        plan: &WasmCallPlan,
        write_ctx: &boxkv_core::hooks::WriteContext,
        sequence: u64,
    ) {
        if !self.is_enabled() || plan.is_empty() {
            return;
        }

        let plugins = match plan.get(boxkv_core::hooks::HookType::PostWrite) {
            Some(list) => list,
            None => return,
        };

        let call_ctx = CallContext::new_readonly(
            write_ctx.key().read_all(),
            write_ctx.value().read_all(),
            write_ctx.expires_at(),
            ctx.db_view.clone(),
            self.runtime.config.budget.clone(),
            sequence,
        );

        for plugin_spec in plugins {
            let plugin_id = crate::plugin::PluginId::from_uuid(plugin_spec.id);

            // 先加载插件
            if let Err(e) = self.runtime.load_by_id(&plugin_id) {
                tracing::error!("Failed to load PostWrite plugin {}: {}", plugin_id, e);
                continue;
            }

            if let Err(e) = self.runtime.execute_post_write(plugin_id, &call_ctx) {
                tracing::error!("PostWrite failed: {} - {}", plugin_id, e);
            }
        }
    }

    fn on_read(
        &self,
        ctx: &HookContext,
        plan: &WasmCallPlan,
        key: Bytes,
        value: ValueType,
    ) -> boxkv_core::db::error::Result<OnReadAction> {
        if !self.is_enabled() || plan.is_empty() {
            return Ok(OnReadAction::Accept(value));
        }

        let plugins = match plan.get(boxkv_core::hooks::HookType::OnRead) {
            Some(list) => list,
            None => return Ok(OnReadAction::Accept(value)),
        };

        // 提取数据本体和过期时间（wasm 层使用 Bytes + Option<u64>）
        let (value_data, expires_at) = match &value {
            ValueType::Normal(data) => (data.clone(), None),
            ValueType::Expiring { data, expire_at } => (data.clone(), Some(*expire_at)),
            ValueType::Tombstone => (Bytes::new(), None),
        };

        // 构造只读 CallContext
        let call_ctx = CallContext::new_readonly(
            key.clone(),
            value_data.clone(),
            expires_at,
            ctx.db_view.clone(),
            self.runtime.config.budget.clone(),
            0, // OnRead 无 sequence
        );

        let mut current_value_data = value_data;
        let mut current_expires_at = expires_at;

        // 按序执行插件
        for plugin_spec in plugins {
            let plugin_id = crate::plugin::PluginId::from_uuid(plugin_spec.id);

            // 先加载插件
            if let Err(e) = self.runtime.load_by_id(&plugin_id) {
                tracing::error!("Failed to load OnRead plugin {}: {}", plugin_id, e);
                // OnRead 加载失败，返回当前值
                let final_value = match current_expires_at {
                    Some(exp) => ValueType::Expiring {
                        data: current_value_data,
                        expire_at: exp,
                    },
                    None => ValueType::Normal(current_value_data),
                };
                return Ok(OnReadAction::Accept(final_value));
            }

            match self.runtime.execute_on_read(
                plugin_id,
                key.clone(),
                current_value_data.clone(),
                &call_ctx,
            ) {
                Ok(action) => {
                    match action {
                        OnReadAction::Accept(vt) => {
                            // 提取数据
                            match vt {
                                ValueType::Normal(d) => {
                                    current_value_data = d;
                                    current_expires_at = None;
                                }
                                ValueType::Expiring { data: d, expire_at } => {
                                    current_value_data = d;
                                    current_expires_at = Some(expire_at);
                                }
                                ValueType::Tombstone => {
                                    current_value_data = Bytes::new();
                                    current_expires_at = None;
                                }
                            }
                        }
                        OnReadAction::Reject(reason) => {
                            return Err(boxkv_core::db::error::DBError::PluginRejected(reason));
                        }
                        OnReadAction::Transform(vt) => {
                            // 提取数据
                            match vt {
                                ValueType::Normal(d) => {
                                    current_value_data = d;
                                    current_expires_at = None;
                                }
                                ValueType::Expiring { data: d, expire_at } => {
                                    current_value_data = d;
                                    current_expires_at = Some(expire_at);
                                }
                                ValueType::Tombstone => {
                                    current_value_data = Bytes::new();
                                    current_expires_at = None;
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    tracing::error!("OnRead failed: {:?} - {}", plugin_spec.id, e);
                    // OnRead 错误不终止，返回当前值
                    let final_value = match current_expires_at {
                        Some(exp) => ValueType::Expiring {
                            data: current_value_data,
                            expire_at: exp,
                        },
                        None => ValueType::Normal(current_value_data),
                    };
                    return Ok(OnReadAction::Accept(final_value));
                }
            }
        }

        // 重新封装为 ValueType
        let final_value = match current_expires_at {
            Some(exp) => ValueType::Expiring {
                data: current_value_data,
                expire_at: exp,
            },
            None => ValueType::Normal(current_value_data),
        };
        Ok(OnReadAction::Accept(final_value))
    }

    fn scan_filter(
        &self,
        ctx: &HookContext,
        plan: &WasmCallPlan,
        key: Bytes,
        value: ValueType,
    ) -> boxkv_core::db::error::Result<ScanFilterAction> {
        // 提取数据本体（scan_filter 不需要 expires_at）
        let value_data = match &value {
            ValueType::Normal(data) | ValueType::Expiring { data, .. } => data.clone(),
            ValueType::Tombstone => Bytes::new(),
        };
        if !self.is_enabled() || plan.is_empty() {
            return Ok(ScanFilterAction::Keep);
        }

        let plugins = match plan.get(boxkv_core::hooks::HookType::ScanFilter) {
            Some(list) => list,
            None => return Ok(ScanFilterAction::Keep),
        };

        // 构造只读 CallContext
        let call_ctx = CallContext::new_readonly(
            key.clone(),
            value_data.clone(),
            None,
            ctx.db_view.clone(),
            self.runtime.config.budget.clone(),
            0,
        );

        // 按序执行插件，任一 Drop 则 Drop
        for plugin_spec in plugins {
            let plugin_id = crate::plugin::PluginId::from_uuid(plugin_spec.id);

            // 先加载插件
            if let Err(e) = self.runtime.load_by_id(&plugin_id) {
                tracing::error!("Failed to load ScanFilter plugin {}: {}", plugin_id, e);
                // ScanFilter 加载失败默认 Keep
                continue;
            }

            match self.runtime.execute_scan_filter(
                plugin_id,
                key.clone(),
                value_data.clone(),
                &call_ctx,
            ) {
                Ok(ScanFilterAction::Drop) => {
                    return Ok(ScanFilterAction::Drop);
                }
                Ok(ScanFilterAction::Keep) => {
                    continue;
                }
                Err(e) => {
                    tracing::error!("ScanFilter failed: {:?} - {}", plugin_spec.id, e);
                    // ScanFilter 错误默认 Keep
                    continue;
                }
            }
        }

        Ok(ScanFilterAction::Keep)
    }
}
