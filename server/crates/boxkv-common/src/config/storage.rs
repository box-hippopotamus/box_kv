use serde::{Deserialize, Serialize};

/// 存储（引擎）相关配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    pub data_dir: String,
    pub wal_dir: String,
    pub memtable_size_mb: usize,
    pub max_write_buffer_number: usize,
    pub wal_sync_mode: WalSyncMode,
    pub block_cache_size_mb: usize,
    pub create_if_missing: bool,
    pub error_if_exists: bool,
    // Manifest
    pub manifest_block_size_bytes: usize,
    pub manifest_max_file_size_bytes: u64,
    // WAL 安全上限
    pub wal_max_key_size_bytes: u64,
    pub wal_max_value_size_bytes: u64,
    // TableCache 容量（按表个数）
    pub table_cache_capacity_tables: u64,
    // 文件写缓冲区大小（字节）
    pub file_write_buffer_size_bytes: usize,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_dir: "./data".to_string(),
            wal_dir: "./data/wal".to_string(),
            memtable_size_mb: 64,
            max_write_buffer_number: 3,
            wal_sync_mode: WalSyncMode::Sync,
            block_cache_size_mb: 256,
            create_if_missing: true,
            error_if_exists: false,
            manifest_block_size_bytes: 32 * 1024,
            manifest_max_file_size_bytes: 128 * 1024 * 1024,
            wal_max_key_size_bytes: 1024 * 1024,
            wal_max_value_size_bytes: 64 * 1024 * 1024,
            table_cache_capacity_tables: 1024,
            file_write_buffer_size_bytes: 64 * 1024,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WalSyncMode {
    /// 不主动调用 fsync，由操作系统负责刷新
    None,
    /// 每次写入后都同步落盘（最安全，性能较差）
    Sync,
    /// 每隔固定时间批量同步（需与调度器配合）
    Periodic,
}
