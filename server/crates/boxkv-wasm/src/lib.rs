//! BoxKV Wasm Runtime

pub mod abi;
pub mod budget;
pub mod context;
pub mod error;
pub mod plugin;
pub mod pool;
pub mod provider;
pub mod runtime;
pub mod validator;

// 重新导出核心类型
pub use boxkv_common::config::{
    WasmBudgetConfig as BudgetConfig, WasmCacheConfig as CacheConfig, WasmPoolConfig as PoolConfig,
    WasmRuntimeConfig as RuntimeConfig,
};
pub use context::CallContext;
pub use error::{Result, WasmError};
pub use provider::WasmHookProvider;
pub use runtime::WasmRuntime;

#[cfg(test)]
mod tests {
    use super::*;
    use plugin::{FsBlobStore, FsRegistry, PluginService};
    use std::sync::Arc;
    use tempfile::TempDir;

    fn setup() -> (TempDir, Arc<PluginService>) {
        let tmp = TempDir::new().unwrap();
        let blobs = Arc::new(FsBlobStore::new(tmp.path().join("blobs")).unwrap());
        let registry = Arc::new(FsRegistry::new(tmp.path().join("registry")).unwrap());
        let service = Arc::new(PluginService::new(blobs, registry));
        (tmp, service)
    }

    #[test]
    fn test_runtime_creation() {
        let (_tmp, service) = setup();
        let config = RuntimeConfig::default();
        let runtime = WasmRuntime::new(config, service);
        assert!(runtime.is_ok());
    }

    #[test]
    fn test_provider_creation() {
        let (_tmp, service) = setup();
        let config = RuntimeConfig::default();
        let runtime = WasmRuntime::new(config, service).unwrap();
        let _provider = WasmHookProvider::new(Arc::new(runtime));
        // 基础创建测试
        assert!(true);
    }
}
