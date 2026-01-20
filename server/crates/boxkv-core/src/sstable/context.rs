//! SSTable 运行时上下文
//!
//! 用于注入运行时依赖（BlockCache、FilterPolicy 等），与全局配置（SSTableConfig）分离

use crate::cache::BlockCache;
use crate::sstable::filter::policy::FilterPolicy;
use std::sync::Arc;

/// SSTable 运行时上下文
///
/// 承载无法序列化的运行时依赖，由 DB 引擎创建并注入到 builder/reader
#[derive(Clone)]
pub struct SSTableContext {
    /// Block 缓存（可选）
    pub block_cache: Option<Arc<BlockCache>>,

    /// Filter Policy（可选，根据全局配置动态创建）
    pub filter_policy: Option<Arc<dyn FilterPolicy>>,
}

impl SSTableContext {
    /// 创建新的 SSTableContext
    pub fn new(
        block_cache: Option<Arc<BlockCache>>,
        filter_policy: Option<Arc<dyn FilterPolicy>>,
    ) -> Self {
        Self {
            block_cache,
            filter_policy,
        }
    }

    /// 创建最小化的 SSTableContext
    pub fn minimal() -> Self {
        Self {
            block_cache: None,
            filter_policy: None,
        }
    }
}

impl Default for SSTableContext {
    fn default() -> Self {
        Self::minimal()
    }
}
