//! 内存限制器（ResourceLimiter）

use std::sync::atomic::{AtomicU64, Ordering};
use wasmtime::ResourceLimiter;

use crate::plugin::PluginId;

/// 内存限制器（per-instance）
pub struct MemoryLimiter {
    /// 单实例硬限制（字节）
    instance_hard_bytes: u64,

    /// 单实例软限制（字节，仅告警）
    instance_soft_bytes: u64,

    /// 当前实例占用（字节）
    instance_usage: AtomicU64,

    /// 当前实例所属插件（仅用于日志）
    plugin_id: PluginId,

    /// 统计
    stats: LimiterStats,
}

/// 限制器统计
#[derive(Debug, Clone, Default)]
pub struct LimiterStats {
    /// 拒绝次数（硬限制）
    pub denied_hard: u64,

    /// 告警次数（软限制）
    pub warned_soft: u64,

    /// 峰值内存（字节）
    pub peak_bytes: u64,
}

impl MemoryLimiter {
    /// 创建新的内存限制器
    pub fn new(plugin_id: PluginId, instance_hard_bytes: u64, instance_soft_bytes: u64) -> Self {
        Self {
            instance_hard_bytes,
            instance_soft_bytes,
            instance_usage: AtomicU64::new(0),
            plugin_id,
            stats: LimiterStats::default(),
        }
    }

    /// 获取当前实例使用量
    pub fn instance_usage(&self) -> u64 {
        self.instance_usage.load(Ordering::Relaxed)
    }

    /// 获取统计信息
    pub fn stats(&self) -> &LimiterStats {
        &self.stats
    }

    /// 记录拒绝（硬限制）
    fn record_denied_hard(&mut self) {
        self.stats.denied_hard += 1;
    }

    /// 记录告警（软限制）
    fn record_warned_soft(&mut self) {
        self.stats.warned_soft += 1;
    }

    /// 更新峰值
    fn update_peak(&mut self, bytes: u64) {
        self.stats.peak_bytes = self.stats.peak_bytes.max(bytes);
    }
}

impl ResourceLimiter for MemoryLimiter {
    fn memory_growing(
        &mut self,
        current: usize,
        desired: usize,
        _maximum: Option<usize>,
    ) -> Result<bool, wasmtime::Error> {
        let desired_bytes = desired as u64;

        // 1. 检查单实例硬限制
        if desired_bytes > self.instance_hard_bytes {
            tracing::warn!(
                "Memory growth denied (hard limit): plugin={:?}, current={}B, desired={}B, limit={}B",
                self.plugin_id,
                current,
                desired,
                self.instance_hard_bytes
            );
            self.record_denied_hard();
            return Ok(false);
        }

        // 2. 检查软限制（仅告警）
        if desired_bytes > self.instance_soft_bytes {
            tracing::warn!(
                "Memory soft limit exceeded (allowed): plugin={:?}, desired={}B, soft_limit={}B",
                self.plugin_id,
                desired,
                self.instance_soft_bytes
            );
            self.record_warned_soft();
        }

        // 3. 允许增长，更新计数器
        self.instance_usage.store(desired_bytes, Ordering::Relaxed);
        self.update_peak(desired_bytes);

        tracing::trace!(
            "Memory growth allowed: plugin={:?}, {} -> {} bytes",
            self.plugin_id,
            current,
            desired
        );

        Ok(true)
    }

    fn table_growing(
        &mut self,
        _current: usize,
        desired: usize,
        _maximum: Option<usize>,
    ) -> Result<bool, wasmtime::Error> {
        // 最多 10000 个表元素
        const MAX_TABLE_ELEMENTS: usize = 10000;

        if desired > MAX_TABLE_ELEMENTS {
            tracing::warn!(
                "Table growth denied: plugin={:?}, desired={}, limit={}",
                self.plugin_id,
                desired,
                MAX_TABLE_ELEMENTS
            );
            return Ok(false);
        }

        Ok(true)
    }
}

impl Drop for MemoryLimiter {
    fn drop(&mut self) {
        let instance_usage = self.instance_usage.load(Ordering::Relaxed);
        tracing::trace!(
            "MemoryLimiter dropped: plugin={:?}, used={}B, peak={}B",
            self.plugin_id,
            instance_usage,
            self.stats.peak_bytes
        );
    }
}

/// 内存限制器管理（简化版）
pub struct MemoryLimiterManager;

impl MemoryLimiterManager {
    /// 创建新的管理器
    pub fn new() -> Self {
        Self
    }

    /// 创建针对特定插件的限制器
    pub fn create_limiter(
        &self,
        plugin_id: PluginId,
        instance_hard_bytes: u64,
        instance_soft_bytes: u64,
    ) -> MemoryLimiter {
        MemoryLimiter::new(plugin_id, instance_hard_bytes, instance_soft_bytes)
    }
}

impl Default for MemoryLimiterManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn test_memory_limiter_basic() {
        let manager = MemoryLimiterManager::new();
        let plugin_id = PluginId::from_uuid(Uuid::new_v4());

        let mut limiter = manager.create_limiter(
            plugin_id,
            1024 * 1024, // 1MB hard
            512 * 1024,  // 512KB soft
        );

        // 正常增长
        assert!(limiter.memory_growing(0, 256 * 1024, None).unwrap());
        assert_eq!(limiter.instance_usage(), 256 * 1024);

        // 超过软限制但允许
        assert!(
            limiter
                .memory_growing(256 * 1024, 768 * 1024, None)
                .unwrap()
        );
        assert_eq!(limiter.stats().warned_soft, 1);

        // 超过硬限制，拒绝
        assert!(
            !limiter
                .memory_growing(768 * 1024, 2 * 1024 * 1024, None)
                .unwrap()
        );
        assert_eq!(limiter.stats().denied_hard, 1);
    }

    #[test]
    fn test_memory_limiter_peak() {
        let manager = MemoryLimiterManager::new();
        let plugin_id = PluginId::from_uuid(Uuid::new_v4());

        let mut limiter = manager.create_limiter(plugin_id, 1024 * 1024, 512 * 1024);

        // 多次增长
        assert!(limiter.memory_growing(0, 256 * 1024, None).unwrap());
        assert!(
            limiter
                .memory_growing(256 * 1024, 512 * 1024, None)
                .unwrap()
        );
        assert!(
            limiter
                .memory_growing(512 * 1024, 768 * 1024, None)
                .unwrap()
        );

        // 验证峰值
        assert_eq!(limiter.stats().peak_bytes, 768 * 1024);
    }
}
