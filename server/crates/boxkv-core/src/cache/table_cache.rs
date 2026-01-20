use std::path::Path;
use std::sync::Arc;

use moka::sync::Cache;

use crate::compaction::types::TablePathProvider;
use crate::sstable::{SSTableContext, SSTableError, SSTableReader};

const TABLE_CACHE_INITIAL_CAPACITY_LIMIT: u64 = 1024;

/// SSTable 文件缓存
///
/// 缓存已打开的 SSTableReader，避免重复打开文件和解析元数据。
/// 使用 LRU 策略，按表个数限制容量。
pub struct TableCache {
    cache: Cache<u64, Arc<SSTableReader>>,
    ctx: SSTableContext,
    path: Arc<dyn TablePathProvider>,
}

impl TableCache {
    /// 创建 TableCache
    ///
    /// # 参数
    /// - `capacity_tables`: 最大缓存表数
    /// - `ctx`: SSTable 上下文
    /// - `path`: 文件路径提供器
    pub fn new(
        capacity_tables: u64,
        ctx: SSTableContext,
        path: Arc<dyn TablePathProvider>,
    ) -> Self {
        let cache = Cache::builder()
            .weigher(|_k: &u64, _v: &Arc<SSTableReader>| -> u32 { 1 })
            .max_capacity(capacity_tables)
            .initial_capacity(capacity_tables.min(TABLE_CACHE_INITIAL_CAPACITY_LIMIT) as usize)
            .build();
        Self { cache, ctx, path }
    }

    /// 获取 SSTableReader，缓存未命中时自动打开文件
    pub fn get_reader(&self, file_number: u64) -> Result<Arc<SSTableReader>, SSTableError> {
        if let Some(r) = self.cache.get(&file_number) {
            return Ok(r);
        }
        let path = self.path.sst_path(file_number);
        let reader = Arc::new(SSTableReader::open(Path::new(&path), &self.ctx)?);
        self.cache.insert(file_number, reader.clone());
        Ok(reader)
    }

    /// 使指定文件的缓存失效
    pub fn invalidate(&self, file_number: u64) {
        self.cache.invalidate(&file_number);
    }

    /// 清空缓存
    pub fn clear(&self) {
        self.cache.invalidate_all();
    }

    /// 当前缓存的表数
    pub fn len(&self) -> u64 {
        self.cache.entry_count()
    }

    /// 容量（表数）
    pub fn capacity(&self) -> u64 {
        self.cache.policy().max_capacity().unwrap_or(0)
    }
}
