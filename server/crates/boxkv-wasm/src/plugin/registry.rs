//! Plugin Registry - 插件索引与元数据管理

use super::types::{Fingerprint, PluginId, PluginKey, PluginRecord};
use crate::error::{Result, WasmError};
use boxkv_storage::{FileSystem, LocalFileSystem};
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Registry 接口
pub trait Registry: Send + Sync {
    /// 保存/更新插件记录
    fn upsert_record(&self, record: &PluginRecord) -> Result<()>;

    /// 根据 ID 获取记录
    fn get_record(&self, id: &PluginId) -> Result<Option<PluginRecord>>;

    /// 获取 triple 的当前 latest_id（最新的 revision）
    fn get_latest_id(&self, key: &PluginKey) -> Result<Option<PluginId>>;

    /// 根据 (triple, fingerprint) 查找 id（用于 Ensure）
    fn ensure_fingerprint(&self, key: &PluginKey, fp: &Fingerprint) -> Result<Option<PluginId>>;

    /// 列举某个 triple 下的所有记录
    fn list_by_key(&self, key: &PluginKey) -> Result<Vec<PluginRecord>>;

    /// 删除某个 triple 下的所有记录
    fn purge_version(&self, key: &PluginKey) -> Result<usize>;
}

/// 内存状态结构（受 RwLock 保护）
#[derive(Debug, Clone, Serialize, Deserialize)]
struct RegistryState {
    /// 所有 record
    records: HashMap<PluginId, PluginRecord>,
    /// triple → 所有 revision 列表
    revision_index: HashMap<PluginKey, Vec<PluginId>>,
}

impl RegistryState {
    fn new() -> Self {
        Self {
            records: HashMap::new(),
            revision_index: HashMap::new(),
        }
    }
}

/// 文件系统实现的 Registry
pub struct FsRegistry {
    root: PathBuf,
    fs: LocalFileSystem,
    /// 内存状态
    state: RwLock<RegistryState>,
}

impl FsRegistry {
    /// 创建新的 FsRegistry
    pub fn new<P: AsRef<Path>>(root: P) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        let fs = LocalFileSystem;

        if !fs.exists(&root) {
            fs.create_dir(&root).map_err(|e| {
                WasmError::InternalError(format!("Failed to create registry dir: {}", e))
            })?;
        }

        let registry = Self {
            root,
            fs,
            state: RwLock::new(RegistryState::new()),
        };

        // 从磁盘恢复索引
        registry.recover()?;

        Ok(registry)
    }

    /// 元信息文件路径（单文件）
    fn metadata_path(&self) -> PathBuf {
        self.root.join("plugin_metadata.json")
    }

    /// 从磁盘恢复索引
    fn recover(&self) -> Result<()> {
        if !self.fs.exists(&self.metadata_path()) {
            return Ok(()); // 首次启动
        }

        let mut reader = self
            .fs
            .open_read(&self.metadata_path())
            .map_err(|e| WasmError::InternalError(format!("Failed to open metadata: {}", e)))?;

        let content = reader
            .read_all()
            .map_err(|e| WasmError::InternalError(format!("Failed to read metadata: {}", e)))?;

        // 从 Vec 反序列化
        #[derive(Deserialize)]
        struct Snapshot {
            records: Vec<(PluginId, PluginRecord)>,
            revision_index: Vec<(PluginKey, Vec<PluginId>)>,
        }

        let snapshot: Snapshot = serde_json::from_slice(&content)
            .map_err(|e| WasmError::InternalError(format!("Failed to parse metadata: {}", e)))?;

        // 填充状态（写锁）
        let mut state = self.state.write();
        state.records = snapshot.records.into_iter().collect();
        state.revision_index = snapshot.revision_index.into_iter().collect();

        tracing::info!("Recovered {} plugin records", state.records.len());
        Ok(())
    }

    /// 持久化元信息（原子写入）
    /// 在读锁保护下调用（由写锁降级而来）
    fn persist_with_read_lock(&self, state: &RwLockReadGuard<RegistryState>) -> Result<()> {
        #[derive(Serialize)]
        struct Snapshot {
            records: Vec<(PluginId, PluginRecord)>,
            revision_index: Vec<(PluginKey, Vec<PluginId>)>,
        }

        let snapshot = Snapshot {
            records: state.records.iter().map(|(k, v)| (*k, v.clone())).collect(),
            revision_index: state
                .revision_index
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        };

        let json = serde_json::to_vec(&snapshot).map_err(|e| {
            WasmError::InternalError(format!("Failed to serialize metadata: {}", e))
        })?;

        // 原子写入（tmp + rename）
        let tmp_path = self.metadata_path().with_extension("tmp");
        let mut writer = self
            .fs
            .open_write(&tmp_path)
            .map_err(|e| WasmError::InternalError(format!("Failed to open tmp: {}", e)))?;
        writer
            .write(&json)
            .map_err(|e| WasmError::InternalError(format!("Failed to write tmp: {}", e)))?;
        writer
            .flush()
            .map_err(|e| WasmError::InternalError(format!("Failed to flush tmp: {}", e)))?;
        drop(writer);
        self.fs
            .rename(&tmp_path, &self.metadata_path())
            .map_err(|e| WasmError::InternalError(format!("Failed to rename: {}", e)))?;

        Ok(())
    }
}

impl Registry for FsRegistry {
    fn upsert_record(&self, record: &PluginRecord) -> Result<()> {
        // 1. 获取写锁，更新内存状态
        let mut state = self.state.write();

        // 更新 records
        state.records.insert(record.id, record.clone());

        // 更新 revision_index
        state
            .revision_index
            .entry(record.key.clone())
            .and_modify(|revisions| {
                // 去重 + 尾插
                revisions.retain(|&existing_id| existing_id != record.id);
                revisions.push(record.id);
            })
            .or_insert_with(|| vec![record.id]);

        // 2. 降级为读锁
        let state = RwLockWriteGuard::downgrade(state);

        // 3. 在读锁保护下持久化
        self.persist_with_read_lock(&state)?;
        Ok(())
    }

    fn get_record(&self, id: &PluginId) -> Result<Option<PluginRecord>> {
        let state = self.state.read();
        Ok(state.records.get(id).cloned())
    }

    fn get_latest_id(&self, key: &PluginKey) -> Result<Option<PluginId>> {
        let state = self.state.read();
        // 尾部是最新的
        Ok(state
            .revision_index
            .get(key)
            .and_then(|revisions| revisions.last().copied()))
    }

    fn ensure_fingerprint(&self, key: &PluginKey, fp: &Fingerprint) -> Result<Option<PluginId>> {
        // 计算 id
        let id = PluginId::generate(key, fp);

        // 检查该 id 是否存在
        if self.get_record(&id)?.is_some() {
            Ok(Some(id))
        } else {
            Ok(None)
        }
    }

    fn list_by_key(&self, key: &PluginKey) -> Result<Vec<PluginRecord>> {
        let state = self.state.read();

        // 直接从 revision_index 获取所有 id
        let ids = match state.revision_index.get(key) {
            Some(ids) => ids,
            None => return Ok(Vec::new()),
        };

        // 根据 id 查找 record
        let records: Vec<_> = ids
            .iter()
            .filter_map(|id| state.records.get(id).cloned())
            .collect();

        Ok(records)
    }

    fn purge_version(&self, key: &PluginKey) -> Result<usize> {
        // 1. 获取写锁，更新内存状态
        let mut state = self.state.write();

        // 从 revision_index 直接获取所有 id
        let ids_to_delete = match state.revision_index.remove(key) {
            Some(ids) => ids,
            None => {
                // 没有这个 triple，直接返回
                return Ok(0);
            }
        };

        // 删除所有 record
        let deleted_count = ids_to_delete.len();
        for id in ids_to_delete {
            state.records.remove(&id);
        }

        // 2. 降级为读锁
        let state = RwLockWriteGuard::downgrade(state);

        // 3. 在读锁保护下持久化
        self.persist_with_read_lock(&state)?;

        Ok(deleted_count)
    }
}

pub type SharedRegistry = Arc<dyn Registry>;

#[cfg(test)]
mod tests {
    use super::super::types::HookType;
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_registry_upsert_get() {
        let tmp = TempDir::new().unwrap();
        let registry = FsRegistry::new(tmp.path()).unwrap();

        let key = PluginKey::new("test".to_string(), "1".to_string(), HookType::PreWrite);
        let fp = Fingerprint::compute(&bytes::Bytes::from_static(b"test"));
        let id = PluginId::generate(&key, &fp);

        let record = PluginRecord::new(id, key.clone(), fp.clone(), 100, 1);

        registry.upsert_record(&record).unwrap();

        let loaded = registry.get_record(&id).unwrap().unwrap();
        assert_eq!(loaded.id, id);
        assert_eq!(loaded.size, 100);
    }

    #[test]
    fn test_registry_latest_id() {
        let tmp = TempDir::new().unwrap();
        let registry = FsRegistry::new(tmp.path()).unwrap();

        let key = PluginKey::new("test".to_string(), "1".to_string(), HookType::PreWrite);
        let fp = Fingerprint::compute(&bytes::Bytes::from_static(b"test"));
        let id = PluginId::generate(&key, &fp);

        // 使用 upsert_record 代替 set_latest_id
        let record = PluginRecord::new(id, key.clone(), fp, 100, 1);
        registry.upsert_record(&record).unwrap();

        let loaded_id = registry.get_latest_id(&key).unwrap().unwrap();
        assert_eq!(loaded_id, id);
    }

    #[test]
    fn test_registry_ensure_fingerprint() {
        let tmp = TempDir::new().unwrap();
        let registry = FsRegistry::new(tmp.path()).unwrap();

        let key = PluginKey::new("test".to_string(), "1".to_string(), HookType::PreWrite);
        let fp = Fingerprint::compute(&bytes::Bytes::from_static(b"test"));
        let id = PluginId::generate(&key, &fp);

        // 记录不存在时返回 None
        assert!(registry.ensure_fingerprint(&key, &fp).unwrap().is_none());

        // 创建记录
        let record = PluginRecord::new(id, key.clone(), fp.clone(), 100, 1);
        registry.upsert_record(&record).unwrap();

        // 现在能找到
        let found_id = registry.ensure_fingerprint(&key, &fp).unwrap().unwrap();
        assert_eq!(found_id, id);
    }

    #[test]
    fn test_registry_all_revisions() {
        let tmp = TempDir::new().unwrap();
        let registry = FsRegistry::new(tmp.path()).unwrap();

        let key = PluginKey::new("test".to_string(), "1".to_string(), HookType::PreWrite);

        // 插入多个 revision（使用 upsert_record）
        let fp1 = Fingerprint::compute(&bytes::Bytes::from_static(b"v1"));
        let id1 = PluginId::generate(&key, &fp1);
        let record1 = PluginRecord::new(id1, key.clone(), fp1, 100, 1);
        registry.upsert_record(&record1).unwrap();

        let fp2 = Fingerprint::compute(&bytes::Bytes::from_static(b"v2"));
        let id2 = PluginId::generate(&key, &fp2);
        let record2 = PluginRecord::new(id2, key.clone(), fp2, 200, 2);
        registry.upsert_record(&record2).unwrap();

        let fp3 = Fingerprint::compute(&bytes::Bytes::from_static(b"v3"));
        let id3 = PluginId::generate(&key, &fp3);
        let record3 = PluginRecord::new(id3, key.clone(), fp3, 300, 3);
        registry.upsert_record(&record3).unwrap();

        // 获取最新的（尾部）
        let latest = registry.get_latest_id(&key).unwrap().unwrap();
        assert_eq!(latest, id3);

        // 验证历史顺序：[最老, ..., 最新]
        let state = registry.state.read();
        let revisions = state.revision_index.get(&key).unwrap();
        assert_eq!(revisions.len(), 3);
        assert_eq!(revisions[0], id1); // 最老
        assert_eq!(revisions[1], id2);
        assert_eq!(revisions[2], id3); // 最新（尾部）
    }

    #[test]
    fn test_upsert_record_updates_revision_index() {
        let tmp = TempDir::new().unwrap();
        let registry = FsRegistry::new(tmp.path()).unwrap();

        let key = PluginKey::new("test".to_string(), "1.0".to_string(), HookType::PreWrite);

        // 插入第一个 record
        let fp1 = Fingerprint::compute(&bytes::Bytes::from_static(b"v1"));
        let id1 = PluginId::generate(&key, &fp1);
        let record1 = PluginRecord::new(id1, key.clone(), fp1.clone(), 100, 1);
        registry.upsert_record(&record1).unwrap();

        // 验证 revision_index 已更新
        let latest = registry.get_latest_id(&key).unwrap().unwrap();
        assert_eq!(latest, id1);

        // 插入第二个 record
        let fp2 = Fingerprint::compute(&bytes::Bytes::from_static(b"v2"));
        let id2 = PluginId::generate(&key, &fp2);
        let record2 = PluginRecord::new(id2, key.clone(), fp2.clone(), 200, 2);
        registry.upsert_record(&record2).unwrap();

        // 验证 revision_index 包含两个 revision
        let latest = registry.get_latest_id(&key).unwrap().unwrap();
        assert_eq!(latest, id2);

        let state = registry.state.read();
        let revisions = state.revision_index.get(&key).unwrap();
        assert_eq!(revisions.len(), 2);
        assert_eq!(revisions[0], id1); // 最老
        assert_eq!(revisions[1], id2); // 最新
    }
}
