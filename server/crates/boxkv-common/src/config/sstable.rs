use super::error::ConfigError;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SSTableConfig {
    // Block 配置
    pub block_size: usize,
    pub restart_interval: usize,
    pub metadata_block_size: usize,

    // 压缩
    pub compression: CompressionType,
    pub enable_index_compression: bool,

    // 过滤器配置
    pub filter_enabled: bool,
    pub filter_block_type: FilterBlockType,
    pub filter_policy: FilterPolicyType,
    pub bloom_false_positive_rate: f64,
    pub filter_bits_per_key: usize,
    pub filter_bloom_before_level: usize,
    pub filter_index_restart_interval: usize,

    // 验证
    pub verify_checksums: bool,

    // 大小限制
    pub max_key_size_bytes: u64,
    pub max_value_size_bytes: u64,
    pub max_block_size_bytes: u64,

    // 预取
    pub enable_prefetch: bool,
    pub prefetch_index_and_filter: bool,
}

impl Default for SSTableConfig {
    fn default() -> Self {
        Self {
            // Block 配置
            block_size: 4096,
            restart_interval: 16,
            metadata_block_size: 4096,

            // 压缩
            compression: CompressionType::None,
            enable_index_compression: false,

            // 过滤器配置
            filter_enabled: true,
            filter_block_type: FilterBlockType::Full,
            filter_policy: FilterPolicyType::FixedBloom,
            bloom_false_positive_rate: 0.01,
            filter_bits_per_key: 10,
            filter_bloom_before_level: 3,
            filter_index_restart_interval: 16,

            // 验证
            verify_checksums: true,

            // 大小限制
            max_key_size_bytes: 1024 * 1024,        // 1MB
            max_value_size_bytes: 64 * 1024 * 1024, // 64MB
            max_block_size_bytes: 4 * 1024 * 1024,  // 4MB

            // 预取
            enable_prefetch: true,
            prefetch_index_and_filter: true,
        }
    }
}

impl SSTableConfig {
    /// 验证配置有效性
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.block_size == 0 || self.block_size > self.max_block_size_bytes as usize {
            return Err(ConfigError::InvalidValue(format!(
                "block_size must be in [1, {}], got {}",
                self.max_block_size_bytes, self.block_size
            )));
        }

        if self.restart_interval == 0 || self.restart_interval > 1024 {
            return Err(ConfigError::InvalidValue(format!(
                "restart_interval must be in [1, 1024], got {}",
                self.restart_interval
            )));
        }

        if self.metadata_block_size == 0
            || self.metadata_block_size > self.max_block_size_bytes as usize
        {
            return Err(ConfigError::InvalidValue(format!(
                "metadata_block_size must be in [1, {}], got {}",
                self.max_block_size_bytes, self.metadata_block_size
            )));
        }

        if self.bloom_false_positive_rate <= 0.0 || self.bloom_false_positive_rate >= 1.0 {
            return Err(ConfigError::InvalidValue(format!(
                "bloom_false_positive_rate must be in (0, 1), got {}",
                self.bloom_false_positive_rate
            )));
        }

        if self.filter_index_restart_interval == 0 || self.filter_index_restart_interval > 1024 {
            return Err(ConfigError::InvalidValue(format!(
                "filter_index_restart_interval must be in [1, 1024], got {}",
                self.filter_index_restart_interval
            )));
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompressionType {
    None,
    Snappy,
    Lz4,
    Zstd,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FilterBlockType {
    Full,
    Partitioned,
}

/// Filter 策略类型
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FilterPolicyType {
    /// 固定 Bloom（FastLocalBloom）
    FixedBloom,
    /// 固定 Ribbon（Standard128Ribbon）
    FixedRibbon,
    /// 按层级选择（低层 Bloom，高层 Ribbon）
    LevelBased,
}
