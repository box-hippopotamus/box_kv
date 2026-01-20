use bytes::Bytes;
use std::path::PathBuf;
use thiserror::Error;

use crate::manifest::ManifestError;
use crate::sstable::SSTableError;
use crate::version::{FileMeta, Version, VersionEdit, VersionError};

/// 提供 SSTable 文件路径的策略接口。
///
/// - 输入：文件编号 `file_number`
/// - 输出：该编号对应的完整 SSTable 路径（含目录与标准命名）
pub trait TablePathProvider: Send + Sync {
    fn sst_path(&self, file_number: u64) -> PathBuf;
}

/// 持久化并提交一次版本变更（VersionEdit）的接口。
///
/// - 典型实现会先将 `VersionEdit` 写入 Manifest，再原子地应用到 `VersionSet`
pub trait VersionCommit: Send + Sync {
    fn commit(&self, edit: &VersionEdit) -> Result<Version, CompactionError>;
}

/// 压缩流程可能产生的统一错误类型。
#[derive(Debug, Error)]
pub enum CompactionError {
    #[error("version error: {0}")]
    Version(#[from] VersionError),

    #[error("sstable error: {0}")]
    SsTable(#[from] SSTableError),

    #[error("manifest error: {0}")]
    Manifest(#[from] ManifestError),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

/// 一次压缩计划，描述从某一层（level）到下一层（target_level）的输入范围与原因。
#[derive(Clone, Debug)]
pub struct CompactionPlan {
    /// 源层级（Lk）
    pub level: u32,
    /// 目标层级（Lk+1）
    pub target_level: u32,
    /// 源层输入文件集合（同层 clean-cut 扩展后稳定的文件集）
    pub inputs_level: Vec<FileMeta>,
    /// 下层重叠文件集合（与 `inputs_level` 的 key-range 在 Lk+1 层的重叠集）
    pub inputs_next_level: Vec<FileMeta>,
    /// 该次压缩的最小 user key（包含）
    pub smallest: Bytes,
    /// 该次压缩的最大 user key（包含）
    pub largest: Bytes,
    /// 触发原因（如 L0 文件数触发、层级得分超限）
    pub reason: CompactionReason,
}

/// 压缩触发原因。
#[derive(Clone, Debug)]
pub enum CompactionReason {
    L0Files(usize),
    LevelScore { level: u32, score: f64 },
}
