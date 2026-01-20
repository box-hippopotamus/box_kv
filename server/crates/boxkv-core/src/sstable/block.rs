//! Block 模块 - 通用的 Block 存储层
//!
//! 通用的 Block 实现，通过 BlockCodec trait 支持不同类型的 Block

pub mod builder;
pub mod reader;
pub mod types;

pub use builder::BlockBuilder;
pub use reader::{BlockIterator, BlockReader};

/// 重启点长度
pub const RESTART_POINT_LEN: usize = 4;

/// 最大 Key 长度
pub const MAX_KEY_LEN: usize = 1024 * 1024; // 1MB

/// 最大 Value 长度
pub const MAX_VALUE_LEN: usize = 64 * 1024 * 1024; // 64MB

use boxkv_common::config::GlobalConfig;

#[inline]
pub fn max_key_len() -> usize {
    if let Some(cfg) = GlobalConfig::try_get() {
        cfg.sstable.max_key_size_bytes as usize
    } else {
        MAX_KEY_LEN
    }
}

#[inline]
pub fn max_value_len() -> usize {
    if let Some(cfg) = GlobalConfig::try_get() {
        cfg.sstable.max_value_size_bytes as usize
    } else {
        MAX_VALUE_LEN
    }
}
