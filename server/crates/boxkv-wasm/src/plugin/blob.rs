//! Blob Store - 内容寻址的二进制存储

use super::types::Fingerprint;
use crate::error::{Result, WasmError};
use boxkv_storage::{FileSystem, LocalFileSystem};
use bytes::Bytes;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Blob Store 接口
pub trait BlobStore: Send + Sync {
    /// 存储 wasm 二进制
    fn put(&self, fingerprint: &Fingerprint, bytes: &Bytes) -> Result<bool>;

    /// 读取 wasm 二进制
    fn get(&self, fingerprint: &Fingerprint) -> Result<Vec<u8>>;

    /// 检查是否存在
    fn exists(&self, fingerprint: &Fingerprint) -> bool;

    /// 删除 blob（谨慎：仅在确认无引用时调用）
    fn delete(&self, fingerprint: &Fingerprint) -> Result<()>;
}

/// 文件系统实现的 Blob Store
pub struct FsBlobStore {
    root: PathBuf,
    fs: LocalFileSystem,
}

impl FsBlobStore {
    /// 创建新的 FsBlobStore
    pub fn new<P: AsRef<Path>>(root: P) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        let fs = LocalFileSystem;

        // 确保根目录存在
        if !fs.exists(&root) {
            fs.create_dir(&root).map_err(|e| {
                WasmError::InternalError(format!("Failed to create blob dir: {}", e))
            })?;
        }

        Ok(Self { root, fs })
    }

    /// 获取 blob 文件路径
    fn blob_path(&self, fingerprint: &Fingerprint) -> PathBuf {
        // 两级目录：前2字符/后2字符/完整指纹
        // 例：ab/cd/abcdef...
        let hex = fingerprint.as_str();
        let dir1 = &hex[0..2];
        let dir2 = &hex[2..4];
        self.root.join(dir1).join(dir2).join(hex)
    }

    /// 原子写入（临时文件 + rename）
    fn atomic_write(&self, path: &Path, bytes: &Bytes) -> Result<()> {
        // 确保父目录存在
        if let Some(parent) = path.parent() {
            self.fs
                .create_dir(parent)
                .map_err(|e| WasmError::InternalError(format!("Failed to create dir: {}", e)))?;
        }

        // 写临时文件
        let tmp_path = path.with_extension("tmp");
        let mut writer = self
            .fs
            .open_write(&tmp_path)
            .map_err(|e| WasmError::InternalError(format!("Failed to create tmp file: {}", e)))?;

        writer
            .write(bytes.as_ref())
            .map_err(|e| WasmError::InternalError(format!("Failed to write: {}", e)))?;

        writer
            .sync()
            .map_err(|e| WasmError::InternalError(format!("Failed to sync: {}", e)))?;

        writer
            .close()
            .map_err(|e| WasmError::InternalError(format!("Failed to close: {}", e)))?;

        // 原子 rename
        std::fs::rename(&tmp_path, path)
            .map_err(|e| WasmError::InternalError(format!("Failed to rename: {}", e)))?;

        Ok(())
    }
}

impl BlobStore for FsBlobStore {
    fn put(&self, fingerprint: &Fingerprint, bytes: &Bytes) -> Result<bool> {
        let path = self.blob_path(fingerprint);

        // 已存在，直接返回
        if self.fs.exists(&path) {
            return Ok(true);
        }

        // 原子写入
        self.atomic_write(&path, bytes)?;

        Ok(false)
    }

    fn get(&self, fingerprint: &Fingerprint) -> Result<Vec<u8>> {
        let path = self.blob_path(fingerprint);

        let reader = self.fs.open_read(&path).map_err(|e| {
            WasmError::InternalError(format!("Failed to open blob {}: {}", fingerprint, e))
        })?;

        let data = reader.read_all().map_err(|e| {
            WasmError::InternalError(format!("Failed to read blob {}: {}", fingerprint, e))
        })?;

        Ok(data.to_vec())
    }

    fn exists(&self, fingerprint: &Fingerprint) -> bool {
        self.fs.exists(&self.blob_path(fingerprint))
    }

    fn delete(&self, fingerprint: &Fingerprint) -> Result<()> {
        let path = self.blob_path(fingerprint);

        if self.fs.exists(&path) {
            self.fs
                .delete(&path)
                .map_err(|e| WasmError::InternalError(format!("Failed to delete blob: {}", e)))?;
        }

        Ok(())
    }
}

pub type SharedBlobStore = Arc<dyn BlobStore>;

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use tempfile::TempDir;

    #[test]
    fn test_blob_store_put_get() {
        let tmp = TempDir::new().unwrap();
        let store = FsBlobStore::new(tmp.path()).unwrap();

        let bytes = Bytes::from_static(b"test wasm binary");
        let fp = Fingerprint::compute(&bytes);

        // 首次存储
        let existed = store.put(&fp, &bytes).unwrap();
        assert!(!existed);

        // 读取
        let read_bytes = store.get(&fp).unwrap();
        assert_eq!(read_bytes, bytes.as_ref());

        // 重复存储（幂等）
        let existed = store.put(&fp, &bytes).unwrap();
        assert!(existed);
    }

    #[test]
    fn test_blob_store_exists() {
        let tmp = TempDir::new().unwrap();
        let store = FsBlobStore::new(tmp.path()).unwrap();

        let bytes = Bytes::from_static(b"test");
        let fp = Fingerprint::compute(&bytes);

        assert!(!store.exists(&fp));

        store.put(&fp, &bytes).unwrap();

        assert!(store.exists(&fp));
    }

    #[test]
    fn test_blob_store_delete() {
        let tmp = TempDir::new().unwrap();
        let store = FsBlobStore::new(tmp.path()).unwrap();

        let bytes = Bytes::from_static(b"test");
        let fp = Fingerprint::compute(&bytes);

        store.put(&fp, &bytes).unwrap();
        assert!(store.exists(&fp));

        store.delete(&fp).unwrap();
        assert!(!store.exists(&fp));
    }

    #[test]
    fn test_blob_path_sharding() {
        let tmp = TempDir::new().unwrap();
        let store = FsBlobStore::new(tmp.path()).unwrap();

        let fp = Fingerprint::from_hex(
            "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789".to_string(),
        )
        .unwrap();
        let path = store.blob_path(&fp);

        let path_str = path.to_str().unwrap();
        assert!(path_str.contains("ab"));
        assert!(path_str.contains("cd"));
    }
}
