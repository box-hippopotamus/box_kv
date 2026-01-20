mod compaction;
mod error;
mod executor;
mod limits;
mod server;
mod sstable;
mod storage;
mod wasm;

pub use compaction::CompactionConfig;
pub use error::ConfigError;
pub use executor::ExecutorConfig;
pub use limits::LimitsConfig;
pub use server::ServerConfig;
pub use sstable::{CompressionType, FilterBlockType, FilterPolicyType, SSTableConfig};
pub use storage::{StorageConfig, WalSyncMode};
pub use wasm::{
    PluginServiceConfig, WasmBudgetConfig, WasmCacheConfig, WasmConfig, WasmPoolConfig,
    WasmRuntimeConfig,
};

use once_cell::sync::OnceCell;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

static GLOBAL_CONFIG: OnceCell<Arc<GlobalConfig>> = OnceCell::new();

/// BoxKV 全局配置
/// 线程安全，初始化后只读
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct GlobalConfig {
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub storage: StorageConfig,
    #[serde(default)]
    pub compaction: CompactionConfig,
    #[serde(default)]
    pub sstable: SSTableConfig,
    #[serde(default)]
    pub wasm: WasmConfig,
    #[serde(default)]
    pub limits: LimitsConfig,
    #[serde(default)]
    pub executor: ExecutorConfig,
}

impl GlobalConfig {
    /// 从 TOML 文件加载配置
    pub fn load_from_file(path: &str) -> Result<Self, ConfigError> {
        let content = std::fs::read_to_string(path).map_err(|e| {
            ConfigError::LoadFailed(format!("Failed to read config file '{}': {}", path, e))
        })?;

        let config: Self = toml::from_str(&content)
            .map_err(|e| ConfigError::TomlParseError(format!("Failed to parse TOML: {}", e)))?;

        config.validate()?;
        Ok(config)
    }

    /// 将配置保存为 TOML 文件
    pub fn save_to_file(&self, path: &str) -> Result<(), ConfigError> {
        let content = toml::to_string_pretty(self).map_err(|e| {
            ConfigError::TomlSerializeError(format!("Failed to serialize config: {}", e))
        })?;

        std::fs::write(path, content).map_err(ConfigError::Io)?;

        Ok(())
    }

    /// 按优先级加载配置：环境变量 > TOML 文件 > 默认值
    pub fn load() -> Result<Self, ConfigError> {
        // 1. Check for config file path from environment variable
        let config_path =
            std::env::var("BOXKV_CONFIG").unwrap_or_else(|_| "boxkv.toml".to_string());

        // 2. Load from file if exists, otherwise use defaults
        let mut config = if std::path::Path::new(&config_path).exists() {
            tracing::info!("Loading configuration from: {}", config_path);
            Self::load_from_file(&config_path)?
        } else {
            tracing::info!("Config file '{}' not found, using defaults", config_path);
            Self::default()
        };

        // 3. Apply environment variable overrides
        config.apply_env_overrides();

        // 4. Validate final configuration
        config.validate()?;

        Ok(config)
    }

    /// 应用环境变量对配置的覆盖
    fn apply_env_overrides(&mut self) {
        // Server configuration
        if let Ok(host) = std::env::var("BOXKV_SERVER_HOST") {
            tracing::info!("Override server.host from env: {}", host);
            self.server.host = host;
        }
        if let Ok(port) = std::env::var("BOXKV_SERVER_PORT")
            && let Ok(p) = port.parse() {
                tracing::info!("Override server.port from env: {}", p);
                self.server.port = p;
            }
        if let Ok(workers) = std::env::var("BOXKV_SERVER_WORKERS")
            && let Ok(w) = workers.parse() {
                tracing::info!("Override server.workers from env: {}", w);
                self.server.workers = w;
            }

        // Storage configuration
        if let Ok(data_dir) = std::env::var("BOXKV_STORAGE_DATA_DIR") {
            tracing::info!("Override storage.data_dir from env: {}", data_dir);
            self.storage.data_dir = data_dir;
        }
        if let Ok(wal_dir) = std::env::var("BOXKV_STORAGE_WAL_DIR") {
            tracing::info!("Override storage.wal_dir from env: {}", wal_dir);
            self.storage.wal_dir = wal_dir;
        }
        if let Ok(memtable_size) = std::env::var("BOXKV_STORAGE_MEMTABLE_SIZE_MB")
            && let Ok(s) = memtable_size.parse() {
                tracing::info!("Override storage.memtable_size_mb from env: {}", s);
                self.storage.memtable_size_mb = s;
            }
        if let Ok(cache_size) = std::env::var("BOXKV_STORAGE_BLOCK_CACHE_SIZE_MB")
            && let Ok(s) = cache_size.parse() {
                tracing::info!("Override storage.block_cache_size_mb from env: {}", s);
                self.storage.block_cache_size_mb = s;
            }

        // Compaction configuration
        if let Ok(max_levels) = std::env::var("BOXKV_COMPACTION_MAX_LEVELS")
            && let Ok(l) = max_levels.parse() {
                tracing::info!("Override compaction.max_levels from env: {}", l);
                self.compaction.max_levels = l;
            }
        if let Ok(level0_trigger) = std::env::var("BOXKV_COMPACTION_LEVEL0_TRIGGER")
            && let Ok(t) = level0_trigger.parse() {
                tracing::info!("Override compaction.level0_trigger from env: {}", t);
                self.compaction.level0_trigger = t;
            }
        if let Ok(max_jobs) = std::env::var("BOXKV_COMPACTION_MAX_BACKGROUND_JOBS")
            && let Ok(j) = max_jobs.parse() {
                tracing::info!("Override compaction.max_background_jobs from env: {}", j);
                self.compaction.max_background_jobs = j;
            }

        // Wasm configuration
        if let Ok(enabled) = std::env::var("BOXKV_WASM_ENABLED")
            && let Ok(e) = enabled.parse::<bool>() {
                tracing::info!("Override wasm.enabled from env: {}", e);
                self.wasm.enabled = e;
            }
        if let Ok(max_fuel) = std::env::var("BOXKV_WASM_MAX_FUEL")
            && let Ok(f) = max_fuel.parse() {
                tracing::info!("Override wasm.runtime.budget.max_fuel from env: {}", f);
                self.wasm.runtime.budget.max_fuel = f;
            }
        if let Ok(timeout) = std::env::var("BOXKV_WASM_TIMEOUT_MS")
            && let Ok(t) = timeout.parse() {
                tracing::info!("Override wasm.runtime.budget.timeout_ms from env: {}", t);
                self.wasm.runtime.budget.timeout_ms = t;
            }
    }

    /// 校验配置项的取值有效性
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.compaction.max_levels < 2 {
            return Err(ConfigError::InvalidValue(
                "max_levels must be at least 2".to_string(),
            ));
        }
        if self.compaction.level0_trigger == 0 {
            return Err(ConfigError::InvalidValue(
                "level0_trigger must be greater than 0".to_string(),
            ));
        }
        if self.storage.memtable_size_mb == 0 {
            return Err(ConfigError::InvalidValue(
                "memtable_size_mb must be greater than 0".to_string(),
            ));
        }
        Ok(())
    }

    /// 初始化全局配置单例（仅允许调用一次）
    pub fn init(config: GlobalConfig) -> Result<(), ConfigError> {
        config.validate()?;
        GLOBAL_CONFIG
            .set(Arc::new(config))
            .map_err(|_| ConfigError::AlreadyInitialized)
    }

    /// 获取全局配置的引用（未初始化会 panic）
    pub fn get() -> &'static Arc<GlobalConfig> {
        GLOBAL_CONFIG
            .get()
            .expect("GlobalConfig not initialized. Call GlobalConfig::init() first.")
    }

    /// 尝试获取全局配置的引用（未初始化返回 None）
    pub fn try_get() -> Option<&'static Arc<GlobalConfig>> {
        GLOBAL_CONFIG.get()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_default_values() {
        let cfg = GlobalConfig::default();
        assert_eq!(cfg.compaction.max_levels, 7);
        assert_eq!(cfg.compaction.level0_trigger, 4);
        assert_eq!(cfg.storage.memtable_size_mb, 64);
    }

    #[test]
    fn test_config_validation() {
        let mut cfg = GlobalConfig::default();
        assert!(cfg.validate().is_ok());

        cfg.compaction.max_levels = 1;
        assert!(cfg.validate().is_err());

        cfg.compaction.max_levels = 7;
        cfg.compaction.level0_trigger = 0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_compaction_target_level_bytes() {
        let cfg = CompactionConfig::default();
        assert_eq!(cfg.target_level_bytes(0), u64::MAX);
        assert_eq!(cfg.target_level_bytes(1), 256 * 1024 * 1024);
        assert_eq!(cfg.target_level_bytes(2), 2560 * 1024 * 1024);
    }

    #[test]
    fn test_compaction_target_file_size() {
        let cfg = CompactionConfig::default();
        assert_eq!(cfg.target_file_size_bytes(0), 64 * 1024 * 1024);
        assert_eq!(cfg.target_file_size_bytes(1), 64 * 1024 * 1024);
    }
}
