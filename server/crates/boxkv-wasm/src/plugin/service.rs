//! Plugin Service - 插件管理服务
use super::blob::SharedBlobStore;
use super::registry::SharedRegistry;
use super::types::{Fingerprint, HookType, PluginId, PluginKey, PluginRecord};
use crate::error::{Result, WasmError};
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Upload 响应
#[derive(Debug, Clone)]
pub struct UploadResponse {
    pub id: PluginId,
    pub fingerprint: Fingerprint,
    pub is_latest: bool,
}

/// Ensure 响应
#[derive(Debug, Clone)]
pub struct EnsureResponse {
    pub found: bool,
    pub id: Option<PluginId>,
    pub is_latest: bool,
    pub latest_id: Option<PluginId>,
    pub latest_fingerprint: Option<Fingerprint>,
}

/// GetLatest 响应
#[derive(Debug, Clone)]
pub struct GetLatestResponse {
    pub latest_id: PluginId,
    pub latest_fingerprint: Fingerprint,
    pub wasm_bytes: Vec<u8>,
}

/// Purge 响应
#[derive(Debug, Clone)]
pub struct PurgeResponse {
    pub deleted_plugin_count: usize,
    pub deleted_wasm_count: usize,
}

/// 插件服务
pub struct PluginService {
    blobs: SharedBlobStore,
    registry: SharedRegistry,
    /// Triple 级别锁（保证同一 triple 的操作串行化）
    triple_locks: Mutex<HashMap<PluginKey, Arc<Mutex<()>>>>,
}

impl PluginService {
    pub fn new(blobs: SharedBlobStore, registry: SharedRegistry) -> Self {
        Self {
            blobs,
            registry,
            triple_locks: Mutex::new(HashMap::new()),
        }
    }

    /// 获取 BlobStore 引用（用于 Runtime 集成）
    pub fn blobs(&self) -> &SharedBlobStore {
        &self.blobs
    }

    /// 获取 Registry 引用（用于 Runtime 集成）
    pub fn registry(&self) -> &SharedRegistry {
        &self.registry
    }

    /// 获取 triple 级别锁（保证原子性）
    fn lock_triple(&self, key: &PluginKey) -> Arc<Mutex<()>> {
        let mut locks = self.triple_locks.lock().unwrap_or_else(|e| e.into_inner());
        locks
            .entry(key.clone())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    /// 1. 上传插件
    pub fn upload(
        &self,
        name: String,
        version: String,
        hook: HookType,
        wasm_bytes: Bytes,
    ) -> Result<UploadResponse> {
        // ABI 校验（在保存前拒绝非法模块）
        let policy = crate::validator::AbiPolicy::default();
        crate::validator::validate_abi(wasm_bytes.as_ref(), &policy)?;

        // 计算指纹
        let fingerprint = Fingerprint::compute(&wasm_bytes);
        let key = PluginKey::new(name, version, hook);

        // 获取 triple 锁
        let lock_arc = self.lock_triple(&key);
        let _lock = lock_arc.lock().unwrap_or_else(|e| e.into_inner());

        // 生成 id
        let id = PluginId::generate(&key, &fingerprint);

        // 检查是否已存在（幂等）
        if self.registry.get_record(&id)?.is_some() {
            // 已存在，直接返回（revision_index 已经维护）
            return Ok(UploadResponse {
                id,
                fingerprint,
                is_latest: true,
            });
        }

        // 保存 blob（自动去重）
        self.blobs.put(&fingerprint, &wasm_bytes)?;

        // 获取当前 revision（如果存在）
        let current_revision = self
            .registry
            .list_by_key(&key)?
            .into_iter()
            .map(|r| r.revision)
            .max()
            .unwrap_or(0);

        let new_revision = current_revision + 1;

        // 创建记录
        let record = PluginRecord::new(
            id,
            key.clone(),
            fingerprint.clone(),
            wasm_bytes.len() as u64,
            new_revision,
        );

        // upsert_record 会自动维护 revision_index
        self.registry.upsert_record(&record)?;

        tracing::info!(
            "Uploaded plugin {}:{} id={} fp={} size={}",
            key.name,
            key.version,
            id,
            fingerprint,
            wasm_bytes.len()
        );

        Ok(UploadResponse {
            id,
            fingerprint,
            is_latest: true,
        })
    }

    /// 2. 确认插件存在
    pub fn ensure(
        &self,
        name: String,
        version: String,
        hook: HookType,
        fingerprint: Fingerprint,
    ) -> Result<EnsureResponse> {
        let key = PluginKey::new(name, version, hook);

        // 获取 triple 锁
        let lock_arc = self.lock_triple(&key);
        let _lock = lock_arc.lock().unwrap_or_else(|e| e.into_inner());

        // 验证 (triple, fingerprint)
        let found_id = self.registry.ensure_fingerprint(&key, &fingerprint)?;

        // 获取最新 id
        let latest_id = self.registry.get_latest_id(&key)?;

        match found_id {
            Some(id) => {
                let is_latest = Some(id) == latest_id;

                // 如果找到且是最新，直接返回
                if is_latest {
                    return Ok(EnsureResponse {
                        found: true,
                        id: Some(id),
                        is_latest: true,
                        latest_id: None,
                        latest_fingerprint: None,
                    });
                }

                // 找到但不是最新，返回最新信息
                let latest_fp = if let Some(latest_id) = latest_id {
                    self.registry.get_record(&latest_id)?.map(|r| r.fingerprint)
                } else {
                    None
                };

                Ok(EnsureResponse {
                    found: true,
                    id: Some(id),
                    is_latest: false,
                    latest_id,
                    latest_fingerprint: latest_fp,
                })
            }
            None => {
                // 找不到，返回最新信息（如果存在）
                let latest_fp = if let Some(latest_id) = latest_id {
                    self.registry.get_record(&latest_id)?.map(|r| r.fingerprint)
                } else {
                    None
                };

                Ok(EnsureResponse {
                    found: false,
                    id: None,
                    is_latest: false,
                    latest_id,
                    latest_fingerprint: latest_fp,
                })
            }
        }
    }

    /// 3. 获取最新 wasm 文件
    pub fn get_latest(
        &self,
        name: String,
        version: String,
        hook: HookType,
    ) -> Result<GetLatestResponse> {
        let key = PluginKey::new(name, version, hook);

        // 获取 latest_id
        let latest_id = self
            .registry
            .get_latest_id(&key)?
            .ok_or_else(|| WasmError::PluginKeyNotFound(format!("{:?}", key)))?;

        // 获取记录
        let record = self
            .registry
            .get_record(&latest_id)?
            .ok_or_else(|| WasmError::RecordNotFound(format!("{:?}", latest_id)))?;

        // 读取 blob
        let wasm_bytes = self.blobs.get(&record.fingerprint)?;

        Ok(GetLatestResponse {
            latest_id,
            latest_fingerprint: record.fingerprint,
            wasm_bytes,
        })
    }

    /// 4. 检查 id 是否为最新
    pub fn is_latest(
        &self,
        id: &PluginId,
    ) -> Result<(bool, Option<PluginId>, Option<Fingerprint>)> {
        // 获取记录
        let record = self
            .registry
            .get_record(id)?
            .ok_or_else(|| WasmError::PluginIdNotFound(format!("{:?}", id)))?;

        // 获取 latest_id
        let latest_id = self.registry.get_latest_id(&record.key)?;

        if Some(*id) == latest_id {
            return Ok((true, None, None));
        }

        // 不是最新，返回最新信息
        let latest_fp = if let Some(latest_id) = latest_id {
            self.registry.get_record(&latest_id)?.map(|r| r.fingerprint)
        } else {
            None
        };

        Ok((false, latest_id, latest_fp))
    }

    /// 5. 废弃一个 triple
    pub fn purge(&self, name: String, version: String, hook: HookType) -> Result<PurgeResponse> {
        let key = PluginKey::new(name.clone(), version.clone(), hook);

        // 获取 triple 锁
        let lock_arc = self.lock_triple(&key);
        let _lock = lock_arc.lock().map_err(|e| {
            WasmError::InternalError(format!("Failed to acquire triple lock: {}", e))
        })?;

        // 列举所有记录（收集 fingerprint）
        let records = self.registry.list_by_key(&key)?;
        let deleted_blobs: std::collections::HashSet<_> = records
            .into_iter()
            .map(|record| record.fingerprint)
            .collect();

        // 删除该 triple 的所有记录
        let deleted_plugin_count = self.registry.purge_version(&key)?;

        // 删除 blobs
        let mut deleted_wasm_count = 0;
        for fp in deleted_blobs {
            if self.blobs.delete(&fp).is_ok() {
                deleted_wasm_count += 1;
            }
        }

        tracing::info!(
            "Purged triple {}:{}:{:?} - {} plugins, {} wasm files",
            name,
            version,
            hook,
            deleted_plugin_count,
            deleted_wasm_count
        );

        Ok(PurgeResponse {
            deleted_plugin_count,
            deleted_wasm_count,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::super::blob::FsBlobStore;
    use super::super::registry::FsRegistry;
    use super::*;
    use bytes::Bytes;
    use tempfile::TempDir;

    fn setup() -> (TempDir, PluginService) {
        let tmp = TempDir::new().unwrap();
        let blob_path = tmp.path().join("blobs");
        let registry_path = tmp.path().join("registry");

        let blobs: SharedBlobStore = Arc::new(FsBlobStore::new(blob_path).unwrap());
        let registry: SharedRegistry = Arc::new(FsRegistry::new(registry_path).unwrap());

        let service = PluginService::new(blobs, registry);
        (tmp, service)
    }

    /// 生成一个合法的最小 wasm 模块（只导入 boxkv_host::ctx_key_handle）
    fn valid_test_wasm() -> Bytes {
        let wat = r#"
            (module
                (import "boxkv_host" "ctx_key_handle" (func (result i32)))
            )
        "#;
        Bytes::from(wat::parse_str(wat).unwrap())
    }

    /// 生成另一个不同的合法 wasm 模块（导入不同函数）
    fn valid_test_wasm2() -> Bytes {
        let wat = r#"
            (module
                (import "boxkv_host" "ctx_value_handle" (func (result i32)))
            )
        "#;
        Bytes::from(wat::parse_str(wat).unwrap())
    }

    #[test]
    fn test_upload() {
        let (_tmp, service) = setup();

        let bytes = valid_test_wasm();
        let resp = service
            .upload(
                "test".to_string(),
                "1".to_string(),
                HookType::PreWrite,
                bytes.clone(),
            )
            .unwrap();

        assert!(resp.is_latest);
        assert_eq!(resp.fingerprint, Fingerprint::compute(&bytes));
    }

    #[test]
    fn test_upload_rejects_invalid_module() {
        let (_tmp, service) = setup();

        // 非法模块：导入非法命名空间
        let wat = r#"
            (module
                (import "evil" "func" (func))
            )
        "#;
        let invalid_wasm = Bytes::from(wat::parse_str(wat).unwrap());

        let result = service.upload(
            "test".to_string(),
            "1".to_string(),
            HookType::PreWrite,
            invalid_wasm,
        );

        assert!(result.is_err());
        if let Err(WasmError::AbiViolation(msg)) = result {
            assert!(
                msg.contains("evil"),
                "Error message should mention 'evil': {}",
                msg
            );
        } else {
            panic!("Expected AbiViolation error, got: {:?}", result);
        }
    }

    #[test]
    fn test_upload_rejects_invalid_function() {
        let (_tmp, service) = setup();

        // 非法模块：导入不存在的函数
        let wat = r#"
            (module
                (import "boxkv_host" "bad_func" (func))
            )
        "#;
        let invalid_wasm = Bytes::from(wat::parse_str(wat).unwrap());

        let result = service.upload(
            "test".to_string(),
            "1".to_string(),
            HookType::PreWrite,
            invalid_wasm,
        );

        assert!(result.is_err());
        if let Err(WasmError::AbiViolation(msg)) = result {
            assert!(
                msg.contains("bad_func"),
                "Error message should mention 'bad_func': {}",
                msg
            );
        } else {
            panic!("Expected AbiViolation error, got: {:?}", result);
        }
    }

    #[test]
    fn test_upload_idempotent() {
        let (_tmp, service) = setup();

        let bytes = valid_test_wasm();

        let resp1 = service
            .upload(
                "test".to_string(),
                "1".to_string(),
                HookType::PreWrite,
                bytes.clone(),
            )
            .unwrap();

        let resp2 = service
            .upload(
                "test".to_string(),
                "1".to_string(),
                HookType::PreWrite,
                bytes.clone(),
            )
            .unwrap();

        assert_eq!(resp1.id, resp2.id);
        assert_eq!(resp1.fingerprint, resp2.fingerprint);
    }

    #[test]
    fn test_ensure_found() {
        let (_tmp, service) = setup();

        let bytes = valid_test_wasm();
        let fp = Fingerprint::compute(&bytes);

        // 先上传
        let upload_resp = service
            .upload(
                "test".to_string(),
                "1".to_string(),
                HookType::PreWrite,
                bytes.clone(),
            )
            .unwrap();

        // Ensure 应该找到
        let ensure_resp = service
            .ensure("test".to_string(), "1".to_string(), HookType::PreWrite, fp)
            .unwrap();

        assert!(ensure_resp.found);
        assert_eq!(ensure_resp.id, Some(upload_resp.id));
        assert!(ensure_resp.is_latest);
    }

    #[test]
    fn test_ensure_not_found() {
        let (_tmp, service) = setup();

        let fp = Fingerprint::compute(&Bytes::from_static(b"nonexistent"));

        let ensure_resp = service
            .ensure("test".to_string(), "1".to_string(), HookType::PreWrite, fp)
            .unwrap();

        assert!(!ensure_resp.found);
        assert!(ensure_resp.id.is_none());
    }

    #[test]
    fn test_get_latest() {
        let (_tmp, service) = setup();

        let bytes = valid_test_wasm();

        service
            .upload(
                "test".to_string(),
                "1".to_string(),
                HookType::PreWrite,
                bytes.clone(),
            )
            .unwrap();

        let latest_resp = service
            .get_latest("test".to_string(), "1".to_string(), HookType::PreWrite)
            .unwrap();

        assert_eq!(latest_resp.wasm_bytes, bytes.as_ref());
        assert_eq!(latest_resp.latest_fingerprint, Fingerprint::compute(&bytes));
    }

    #[test]
    fn test_is_latest() {
        let (_tmp, service) = setup();

        let bytes1 = valid_test_wasm();
        let bytes2 = valid_test_wasm2();

        let resp1 = service
            .upload(
                "test".to_string(),
                "1".to_string(),
                HookType::PreWrite,
                bytes1.clone(),
            )
            .unwrap();

        let resp2 = service
            .upload(
                "test".to_string(),
                "1".to_string(),
                HookType::PreWrite,
                bytes2.clone(),
            )
            .unwrap();

        // resp1 不是最新
        let (is_latest, latest_id, _) = service.is_latest(&resp1.id).unwrap();
        assert!(!is_latest);
        assert_eq!(latest_id, Some(resp2.id));

        // resp2 是最新
        let (is_latest, _, _) = service.is_latest(&resp2.id).unwrap();
        assert!(is_latest);
    }

    #[test]
    fn test_purge() {
        let (_tmp, service) = setup();

        let bytes = valid_test_wasm();

        service
            .upload(
                "test".to_string(),
                "1".to_string(),
                HookType::PreWrite,
                bytes.clone(),
            )
            .unwrap();

        service
            .upload(
                "test".to_string(),
                "1".to_string(),
                HookType::PostWrite,
                bytes.clone(),
            )
            .unwrap();

        // 删除 PreWrite triple
        let purge_resp1 = service
            .purge("test".to_string(), "1".to_string(), HookType::PreWrite)
            .unwrap();
        assert_eq!(purge_resp1.deleted_plugin_count, 1);
        assert!(purge_resp1.deleted_wasm_count > 0);

        // 删除 PostWrite triple
        let purge_resp2 = service
            .purge("test".to_string(), "1".to_string(), HookType::PostWrite)
            .unwrap();
        assert_eq!(purge_resp2.deleted_plugin_count, 1);
        assert!(purge_resp2.deleted_wasm_count > 0);
    }
}
