use crate::compaction::CompactionError;
use crate::manifest::ManifestError;
use crate::sstable::SSTableError;
use crate::version::VersionError;
use crate::wal::WalError;
/// DB 层统一错误类型。
///
/// 该枚举用于在 DB 边界汇总各子模块错误，并补充少量由 DB 层直接判定的状态/约束错误。
use thiserror::Error;

#[derive(Debug, Error)]
pub enum DBError {
    #[error("Database not found at {0}")]
    NotFound(String),

    #[error("Database already exists at {0}")]
    AlreadyExists(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("WAL error: {0}")]
    Wal(#[from] WalError),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Key too large: {0} bytes (max: {1} bytes)")]
    KeyTooLarge(usize, usize),

    #[error("Value too large: {0} bytes (max: {1} bytes)")]
    ValueTooLarge(usize, usize),

    #[error("Batch too large: {0} entries (max: {1} entries)")]
    BatchTooLarge(usize, usize),

    #[error("Database is closed")]
    Closed,

    #[error("Snapshot not found")]
    SnapshotNotFound,

    #[error("Version error: {0}")]
    Version(#[from] VersionError),

    #[error("Manifest error: {0}")]
    Manifest(#[from] ManifestError),

    #[error("SSTable error: {0}")]
    Sst(#[from] SSTableError),

    #[error("Compaction error: {0}")]
    Compact(#[from] CompactionError),

    #[error("Write stalled: too many L0 files or immutable memtables")]
    WriteStalled,

    #[error("Plugin rejected: {0}")]
    PluginRejected(String),

    #[error("Internal error: {0}")]
    Internal(String),

    #[error("Executor error: {0}")]
    Executor(String),
}

/// DB 层 Result 别名。
pub type Result<T> = std::result::Result<T, DBError>;

// 执行器错误在 DB 边界统一映射为字符串化的 Executor 错误
impl From<boxkv_executor::ExecutorError> for DBError {
    fn from(e: boxkv_executor::ExecutorError) -> Self {
        DBError::Executor(e.to_string())
    }
}
