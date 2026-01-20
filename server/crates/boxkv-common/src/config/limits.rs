//! 请求/对象大小等通用限制配置
//! 说明：用于约束 key/value、批量与扫描上限，防止资源滥用。
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LimitsConfig {
    pub max_key_size_kb: usize,
    pub max_value_size_mb: usize,
    pub max_batch_size: usize,
    pub max_scan_limit: usize,
}

impl Default for LimitsConfig {
    fn default() -> Self {
        Self {
            max_key_size_kb: 64,
            max_value_size_mb: 64,
            max_batch_size: 10000,
            max_scan_limit: 10000,
        }
    }
}
