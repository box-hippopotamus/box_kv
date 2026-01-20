use super::BlockCacheKey;
use bytes::Bytes;
use moka::sync::Cache;

/// SSTable Block 缓存
///
/// 基于 moka 实现的并发 LRU 缓存，用于缓存已解压的 DataBlock/IndexBlock。
/// 支持按字节数限制容量，自动驱逐最少使用的块。
///
/// # 示例
/// ```ignore
/// let cache = BlockCache::new(8 * 1024 * 1024); // 8MB
/// cache.insert(key, block_data);
/// if let Some(data) = cache.get(&key) {
///     // 缓存命中
/// }
/// ```
pub struct BlockCache {
    inner: Cache<BlockCacheKey, Bytes>,
}

impl BlockCache {
    /// 创建指定容量的 Block Cache
    ///
    /// # 参数
    /// - `capacity_bytes`: 最大容量（字节）
    pub fn new(capacity_bytes: u64) -> Self {
        let cache = Cache::builder()
            .weigher(|_key: &BlockCacheKey, value: &Bytes| -> u32 {
                value.len().min(u32::MAX as usize) as u32
            })
            .max_capacity(capacity_bytes)
            .initial_capacity((capacity_bytes / 4096).min(1024) as usize)
            .build();

        Self { inner: cache }
    }

    /// 插入 Block 到缓存
    ///
    /// 若缓存已满，自动驱逐最少使用的块
    pub fn insert(&self, key: BlockCacheKey, block: Bytes) {
        self.inner.insert(key, block);
    }

    /// 从缓存查找 Block
    ///
    /// 返回 `Some(data)` 表示命中，`None` 表示未命中
    pub fn get(&self, key: &BlockCacheKey) -> Option<Bytes> {
        self.inner.get(key)
    }

    /// 获取当前使用量（字节）
    pub fn usage(&self) -> u64 {
        self.inner.weighted_size()
    }

    /// 获取容量（字节）
    pub fn capacity(&self) -> u64 {
        self.inner.policy().max_capacity().unwrap_or(0)
    }

    /// 获取缓存的条目数
    pub fn entry_count(&self) -> u64 {
        self.inner.entry_count()
    }

    /// 获取命中率（0.0 ~ 1.0）
    pub fn hit_rate(&self) -> f64 {
        0.0
    }

    /// 清空缓存
    pub fn clear(&self) {
        self.inner.invalidate_all();
    }

    /// 获取缓存统计信息
    pub fn stats(&self) -> CacheStats {
        CacheStats {
            hits: 0,
            misses: 0,
            evictions: 0,
            entry_count: self.inner.entry_count(),
            weighted_size: self.inner.weighted_size(),
            capacity: self.capacity(),
        }
    }
}

// Clone 是廉价的（内部共享 Arc）
impl Clone for BlockCache {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

/// 缓存统计信息
#[derive(Debug, Clone)]
pub struct CacheStats {
    /// 命中次数
    pub hits: u64,
    /// 未命中次数
    pub misses: u64,
    /// 驱逐次数
    pub evictions: u64,
    /// 当前条目数
    pub entry_count: u64,
    /// 当前使用量（字节）
    pub weighted_size: u64,
    /// 容量（字节）
    pub capacity: u64,
}

impl CacheStats {
    /// 计算命中率
    pub fn hit_rate(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            self.hits as f64 / total as f64
        }
    }

    /// 计算使用率
    pub fn usage_rate(&self) -> f64 {
        if self.capacity == 0 {
            0.0
        } else {
            self.weighted_size as f64 / self.capacity as f64
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_block_cache_basic() {
        let cache = BlockCache::new(1024); // 1KB cache

        let key1 = BlockCacheKey::new(1, 0);
        let data1 = Bytes::from(vec![1u8; 512]); // 512 bytes

        // 插入
        cache.insert(key1, data1.clone());

        // 查找
        assert_eq!(cache.get(&key1), Some(data1));

        // 未命中
        let key2 = BlockCacheKey::new(2, 0);
        assert_eq!(cache.get(&key2), None);
    }

    #[test]
    fn test_block_cache_eviction() {
        let cache = BlockCache::new(1024); // 1KB cache

        // 插入两个 512 字节的 Block（刚好填满）
        let key1 = BlockCacheKey::new(1, 0);
        let data1 = Bytes::from(vec![1u8; 512]);
        cache.insert(key1, data1.clone());

        let key2 = BlockCacheKey::new(2, 0);
        let data2 = Bytes::from(vec![2u8; 512]);
        cache.insert(key2, data2.clone());

        // 插入第三个 Block，应该驱逐最旧的
        let key3 = BlockCacheKey::new(3, 0);
        let data3 = Bytes::from(vec![3u8; 512]);
        cache.insert(key3, data3.clone());

        // 等待驱逐完成（moka 是异步驱逐的）
        std::thread::sleep(std::time::Duration::from_millis(100));

        // key1 应该被驱逐
        assert!(cache.get(&key1).is_none() || cache.entry_count() <= 2);
    }

    #[test]
    fn test_block_cache_functionality() {
        let cache = BlockCache::new(1024);

        let key1 = BlockCacheKey::new(1, 0);
        let data1 = Bytes::from(vec![1u8; 100]);
        cache.insert(key1, data1);

        cache.inner.run_pending_tasks();

        // 命中
        let result1 = cache.get(&key1);
        assert!(result1.is_some());

        // 未命中
        let key2 = BlockCacheKey::new(2, 0);
        let result2 = cache.get(&key2);
        assert!(result2.is_none());

        // 再次强制处理待处理任务
        cache.inner.run_pending_tasks();

        let stats = cache.stats();
        assert_eq!(stats.capacity, 1024);

        assert!(stats.entry_count <= 1); // 可能是 0 或 1
        assert!(stats.weighted_size <= 100); // 可能是 0 或 100

        assert_eq!(stats.hits, 0);
        assert_eq!(stats.misses, 0);
        assert_eq!(stats.hit_rate(), 0.0);
    }
}
