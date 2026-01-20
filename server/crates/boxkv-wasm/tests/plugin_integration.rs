//! Plugin Management 集成测试
//!
//! 验证完整的插件管理流程：Upload → Ensure → GetLatest → Execute → Purge

use boxkv_wasm::plugin::*;
use boxkv_wasm::{RuntimeConfig, WasmRuntime};
use bytes::Bytes;
use std::sync::Arc;
use tempfile::TempDir;

#[test]
fn test_plugin_upload_and_service() {
    let tmp = TempDir::new().unwrap();
    let blob_path = tmp.path().join("blobs");
    let registry_path = tmp.path().join("registry");

    // 1. 创建 PluginService
    let blobs: SharedBlobStore = Arc::new(FsBlobStore::new(blob_path).unwrap());
    let registry: SharedRegistry = Arc::new(FsRegistry::new(registry_path).unwrap());
    let service = Arc::new(PluginService::new(blobs, registry));

    // 2. 上传插件
    let wasm_bytes = Bytes::from_static(b"fake wasm binary v1");
    let upload_resp = service
        .upload(
            "test_plugin".to_string(),
            "1.0".to_string(),
            HookType::PreWrite,
            wasm_bytes.clone(),
        )
        .unwrap();

    assert!(upload_resp.is_latest);
    let plugin_id = upload_resp.id;

    // 3. 验证可以通过 registry 获取记录
    let record = service.registry().get_record(&plugin_id).unwrap().unwrap();
    assert_eq!(record.id, plugin_id);
    assert_eq!(record.key.name, "test_plugin");
    assert_eq!(record.key.version, "1.0");

    // 4. 验证可以通过 blobs 读取二进制
    let read_bytes = service.blobs().get(&record.fingerprint).unwrap();
    assert_eq!(read_bytes, wasm_bytes.as_ref());
}

#[test]
fn test_plugin_ensure_workflow() {
    let tmp = TempDir::new().unwrap();
    let blob_path = tmp.path().join("blobs");
    let registry_path = tmp.path().join("registry");

    let blobs: SharedBlobStore = Arc::new(FsBlobStore::new(blob_path).unwrap());
    let registry: SharedRegistry = Arc::new(FsRegistry::new(registry_path).unwrap());
    let service = Arc::new(PluginService::new(blobs, registry));

    // 1. 上传第一个版本
    let wasm_v1 = Bytes::from_static(b"wasm binary v1");
    let fp_v1 = Fingerprint::compute(&wasm_v1);

    let resp1 = service
        .upload(
            "app".to_string(),
            "1.0".to_string(),
            HookType::PostWrite,
            wasm_v1.clone(),
        )
        .unwrap();

    assert!(resp1.is_latest);

    // 2. Ensure 应该找到
    let ensure_resp = service
        .ensure(
            "app".to_string(),
            "1.0".to_string(),
            HookType::PostWrite,
            fp_v1.clone(),
        )
        .unwrap();

    assert!(ensure_resp.found);
    assert!(ensure_resp.is_latest);
    assert_eq!(ensure_resp.id, Some(resp1.id));

    // 3. 上传第二个版本（二进制内容不同）
    let wasm_v2 = Bytes::from_static(b"wasm binary v2 (updated)");
    let fp_v2 = Fingerprint::compute(&wasm_v2);

    let resp2 = service
        .upload(
            "app".to_string(),
            "1.0".to_string(),
            HookType::PostWrite,
            wasm_v2.clone(),
        )
        .unwrap();

    assert!(resp2.is_latest);
    assert_ne!(resp2.id, resp1.id); // id 应该不同

    // 4. Ensure 旧版本应该找到但不是最新
    let ensure_old = service
        .ensure(
            "app".to_string(),
            "1.0".to_string(),
            HookType::PostWrite,
            fp_v1,
        )
        .unwrap();

    assert!(ensure_old.found);
    assert!(!ensure_old.is_latest); // 不是最新
    assert_eq!(ensure_old.id, Some(resp1.id));
    assert_eq!(ensure_old.latest_id, Some(resp2.id)); // 提示最新 id

    // 5. Ensure 新版本应该是最新
    let ensure_new = service
        .ensure(
            "app".to_string(),
            "1.0".to_string(),
            HookType::PostWrite,
            fp_v2,
        )
        .unwrap();

    assert!(ensure_new.found);
    assert!(ensure_new.is_latest);
}

#[test]
fn test_plugin_get_latest() {
    let tmp = TempDir::new().unwrap();
    let blob_path = tmp.path().join("blobs");
    let registry_path = tmp.path().join("registry");

    let blobs: SharedBlobStore = Arc::new(FsBlobStore::new(blob_path).unwrap());
    let registry: SharedRegistry = Arc::new(FsRegistry::new(registry_path).unwrap());
    let service = Arc::new(PluginService::new(blobs, registry));

    // 上传多个版本
    let wasm_v1 = Bytes::from_static(b"v1");
    let wasm_v2 = Bytes::from_static(b"v2");
    let wasm_v3 = Bytes::from_static(b"v3");

    service
        .upload(
            "plugin".to_string(),
            "2.0".to_string(),
            HookType::OnRead,
            wasm_v1.clone(),
        )
        .unwrap();

    service
        .upload(
            "plugin".to_string(),
            "2.0".to_string(),
            HookType::OnRead,
            wasm_v2.clone(),
        )
        .unwrap();

    service
        .upload(
            "plugin".to_string(),
            "2.0".to_string(),
            HookType::OnRead,
            wasm_v3.clone(),
        )
        .unwrap();

    // GetLatest 应该返回最后上传的版本
    let latest_resp = service
        .get_latest("plugin".to_string(), "2.0".to_string(), HookType::OnRead)
        .unwrap();

    assert_eq!(latest_resp.wasm_bytes, wasm_v3.as_ref());
    assert_eq!(
        latest_resp.latest_fingerprint,
        Fingerprint::compute(&wasm_v3)
    );
}

#[test]
fn test_plugin_update_check() {
    let tmp = TempDir::new().unwrap();
    let blob_path = tmp.path().join("blobs");
    let registry_path = tmp.path().join("registry");

    let blobs: SharedBlobStore = Arc::new(FsBlobStore::new(blob_path).unwrap());
    let registry: SharedRegistry = Arc::new(FsRegistry::new(registry_path).unwrap());
    let service = Arc::new(PluginService::new(blobs, registry));

    // 上传 v1
    let wasm_v1 = Bytes::from_static(b"version 1");
    let resp1 = service
        .upload(
            "checker".to_string(),
            "1.0".to_string(),
            HookType::ScanFilter,
            wasm_v1.clone(),
        )
        .unwrap();

    // 检查 v1 是否最新（应该是）
    let (is_latest, _, _) = service.is_latest(&resp1.id).unwrap();
    assert!(is_latest);

    // 上传 v2
    let wasm_v2 = Bytes::from_static(b"version 2");
    let resp2 = service
        .upload(
            "checker".to_string(),
            "1.0".to_string(),
            HookType::ScanFilter,
            wasm_v2.clone(),
        )
        .unwrap();

    // 检查 v1 是否最新（应该不是）
    let (is_latest, latest_id, _) = service.is_latest(&resp1.id).unwrap();
    assert!(!is_latest);
    assert_eq!(latest_id, Some(resp2.id));

    // 检查 v2 是否最新（应该是）
    let (is_latest, _, _) = service.is_latest(&resp2.id).unwrap();
    assert!(is_latest);
}

#[test]
fn test_plugin_purge() {
    let tmp = TempDir::new().unwrap();
    let blob_path = tmp.path().join("blobs");
    let registry_path = tmp.path().join("registry");

    let blobs: SharedBlobStore = Arc::new(FsBlobStore::new(blob_path).unwrap());
    let registry: SharedRegistry = Arc::new(FsRegistry::new(registry_path).unwrap());
    let service = Arc::new(PluginService::new(blobs, registry));

    // 上传多个 hook 类型
    let wasm = Bytes::from_static(b"test wasm");

    service
        .upload(
            "purge_test".to_string(),
            "0.1".to_string(),
            HookType::PreWrite,
            wasm.clone(),
        )
        .unwrap();

    service
        .upload(
            "purge_test".to_string(),
            "0.1".to_string(),
            HookType::PostWrite,
            wasm.clone(),
        )
        .unwrap();

    service
        .upload(
            "purge_test".to_string(),
            "0.1".to_string(),
            HookType::OnRead,
            wasm.clone(),
        )
        .unwrap();

    // Purge 三个 Hook（逐个）
    let purge_pre = service
        .purge(
            "purge_test".to_string(),
            "0.1".to_string(),
            HookType::PreWrite,
        )
        .unwrap();
    assert_eq!(purge_pre.deleted_plugin_count, 1);
    assert!(purge_pre.deleted_wasm_count > 0);
    let purge_post = service
        .purge(
            "purge_test".to_string(),
            "0.1".to_string(),
            HookType::PostWrite,
        )
        .unwrap();
    assert_eq!(purge_post.deleted_plugin_count, 1);
    assert!(purge_post.deleted_wasm_count > 0);
    let purge_read = service
        .purge(
            "purge_test".to_string(),
            "0.1".to_string(),
            HookType::OnRead,
        )
        .unwrap();
    assert_eq!(purge_read.deleted_plugin_count, 1);
    assert!(purge_read.deleted_wasm_count > 0);

    // 验证无法再获取
    let get_result = service.get_latest(
        "purge_test".to_string(),
        "0.1".to_string(),
        HookType::PreWrite,
    );

    assert!(get_result.is_err());
}

#[test]
fn test_runtime_with_service_integration() {
    let tmp = TempDir::new().unwrap();
    let blob_path = tmp.path().join("blobs");
    let registry_path = tmp.path().join("registry");

    let blobs: SharedBlobStore = Arc::new(FsBlobStore::new(blob_path).unwrap());
    let registry: SharedRegistry = Arc::new(FsRegistry::new(registry_path).unwrap());
    let service = Arc::new(PluginService::new(blobs, registry));

    // 上传插件
    let wasm = Bytes::from_static(b"test plugin binary");
    let upload_resp = service
        .upload(
            "runtime_test".to_string(),
            "1.0".to_string(),
            HookType::PreWrite,
            wasm.clone(),
        )
        .unwrap();

    let plugin_id = upload_resp.id;

    // 创建 Runtime
    let config = RuntimeConfig::default();
    let runtime = WasmRuntime::new(config, service.clone()).unwrap();

    // 检查更新（当前是最新）
    let update = runtime.check_update_available(&plugin_id).unwrap();
    assert!(update.is_none());

    // 上传新版本
    let wasm_v2 = Bytes::from_static(b"test plugin binary v2");
    service
        .upload(
            "runtime_test".to_string(),
            "1.0".to_string(),
            HookType::PreWrite,
            wasm_v2.clone(),
        )
        .unwrap();

    // 检查更新（应该有更新）
    let update = runtime.check_update_available(&plugin_id).unwrap();
    assert!(update.is_some());

    let (new_id, new_fp) = update.unwrap();
    assert_ne!(new_id, plugin_id);
    assert_eq!(new_fp, Fingerprint::compute(&wasm_v2).to_string());
}
