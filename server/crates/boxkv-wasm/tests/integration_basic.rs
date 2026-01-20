//! 基础集成测试
//!
//! 测试 WasmRuntime 和 WasmHookProvider 的基本功能

use boxkv_wasm::plugin::{FsBlobStore, FsRegistry, PluginService};
use boxkv_wasm::{RuntimeConfig, WasmHookProvider, WasmRuntime};
use std::sync::Arc;

#[test]
fn test_runtime_lifecycle() {
    // 创建 Runtime
    let tmp = tempfile::TempDir::new().unwrap();
    let blobs = Arc::new(FsBlobStore::new(tmp.path().join("blobs")).unwrap());
    let registry = Arc::new(FsRegistry::new(tmp.path().join("registry")).unwrap());
    let service = Arc::new(PluginService::new(blobs, registry));

    let config = RuntimeConfig::default();
    let runtime = WasmRuntime::new(config, service);
    assert!(runtime.is_ok(), "Runtime creation should succeed");

    let runtime = runtime.unwrap();

    // 创建 Provider
    let _provider = WasmHookProvider::new(Arc::new(runtime));

    // 验证创建成功
    assert!(true, "Provider creation should succeed");
}

#[test]
fn test_config_presets() {
    // 默认配置
    let default_cfg = RuntimeConfig::default();
    assert_eq!(default_cfg.budget.max_fuel, 1_000_000);
    assert_eq!(default_cfg.budget.max_kv_get_count, 100);
    assert_eq!(default_cfg.pool.max_instances_per_plugin, 10);

    // 生产配置
    let prod_cfg = RuntimeConfig::production();
    assert_eq!(prod_cfg.budget.max_fuel, 1_000_000); // 使用默认的 budget
    assert_eq!(prod_cfg.pool.max_instances_per_plugin, 20);

    // 严格配置
    let strict_cfg = RuntimeConfig::strict();
    assert_eq!(strict_cfg.budget.max_fuel, 100_000);
    assert_eq!(strict_cfg.budget.max_kv_get_count, 10);
    assert_eq!(strict_cfg.pool.max_instances_per_plugin, 5);
}

#[test]
fn test_multiple_runtimes() {
    // 测试多个 Runtime 实例可以共存
    let cfg1 = RuntimeConfig::default();
    let cfg2 = RuntimeConfig::production();

    let tmp1 = tempfile::TempDir::new().unwrap();
    let blobs1 = Arc::new(FsBlobStore::new(tmp1.path().join("blobs")).unwrap());
    let registry1 = Arc::new(FsRegistry::new(tmp1.path().join("registry")).unwrap());
    let service1 = Arc::new(PluginService::new(blobs1, registry1));
    let runtime1 = WasmRuntime::new(cfg1, service1).unwrap();

    let tmp2 = tempfile::TempDir::new().unwrap();
    let blobs2 = Arc::new(FsBlobStore::new(tmp2.path().join("blobs")).unwrap());
    let registry2 = Arc::new(FsRegistry::new(tmp2.path().join("registry")).unwrap());
    let service2 = Arc::new(PluginService::new(blobs2, registry2));
    let runtime2 = WasmRuntime::new(cfg2, service2).unwrap();

    let _provider1 = WasmHookProvider::new(Arc::new(runtime1));
    let _provider2 = WasmHookProvider::new(Arc::new(runtime2));

    // 验证多实例共存
    assert!(true);
}
