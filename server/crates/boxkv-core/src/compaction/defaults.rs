use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use boxkv_storage::FileSystem;

use crate::manifest::Manifest;
use crate::version::{Version, VersionEdit, VersionSet};

use super::types::{CompactionError, TablePathProvider, VersionCommit};

/// 默认 SSTable 路径提供器
///
/// 按 {file_number:06}.sst 格式生成文件名
pub struct DefaultTablePathProvider {
    /// 数据目录，SSTable 将写入该目录
    pub dir: PathBuf,
}

impl TablePathProvider for DefaultTablePathProvider {
    /// 生成 SSTable 文件路径
    fn sst_path(&self, file_number: u64) -> PathBuf {
        self.dir.join(format!("{file_number:06}.sst"))
    }
}

/// 默认版本提交器
///
/// 先持久化到 Manifest，再应用到 VersionSet
pub struct DefaultVersionCommit<FS: FileSystem> {
    /// 版本集合
    pub vs: Arc<VersionSet>,
    /// Manifest 写入器
    pub manifest: Arc<Mutex<Manifest<FS>>>,
}

impl<FS: FileSystem> VersionCommit for DefaultVersionCommit<FS> {
    /// 提交版本变更
    fn commit(&self, edit: &VersionEdit) -> Result<Version, CompactionError> {
        let mut m = self.manifest.lock().map_err(|e| {
            CompactionError::Io(std::io::Error::other(format!(
                "manifest mutex poisoned: {e}"
            )))
        })?;
        m.add_record(&self.vs, edit.clone())?;
        let v = self.vs.apply_edit(edit)?;
        Ok(v)
    }
}
