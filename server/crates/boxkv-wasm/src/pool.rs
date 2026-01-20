//! 实例池管理
use bytes::Bytes;
use dashmap::DashMap;
use moka::sync::Cache;
use parking_lot::RwLock;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use wasmtime::{Engine, Linker, Module}; // 添加 Bytes 导入

use crate::abi::HostAbi;
use crate::error::{Result, WasmError};
use crate::plugin::PluginId;
use crate::{CacheConfig, PoolConfig};

/// Module 缓存管理器
pub struct ModuleCache {
    /// moka 缓存：PluginId -> Module
    cache: Cache<PluginId, Arc<Module>>,

    /// Wasmtime 引擎（编译用）
    engine: Arc<Engine>,
}

impl ModuleCache {
    /// 创建新的 Module 缓存
    pub fn new(engine: Arc<Engine>, config: &CacheConfig) -> Self {
        let cache = Cache::builder()
            .max_capacity(config.max_modules as u64)
            .time_to_live(Duration::from_secs(3600)) // 1 小时 TTL
            .time_to_idle(Duration::from_secs(1800)) // 30 分钟空闲超时
            .build();

        Self { cache, engine }
    }

    /// 获取或编译 Module
    pub fn get_or_compile(&self, plugin_id: PluginId, wasm_bytes: Bytes) -> Result<Arc<Module>> {
        // 缓存命中
        if let Some(module) = self.cache.get(&plugin_id) {
            tracing::debug!("Module cache hit: {:?}", plugin_id);
            return Ok(module);
        }

        // 缓存未命中，编译
        tracing::info!("Compiling module: {:?}", plugin_id);
        let module = Module::new(&self.engine, wasm_bytes.as_ref())?;
        let module_arc = Arc::new(module);

        // 插入缓存
        self.cache.insert(plugin_id, module_arc.clone());

        Ok(module_arc)
    }

    /// 获取缓存的 Module（不编译）
    pub fn get(&self, plugin_id: &PluginId) -> Option<Arc<Module>> {
        self.cache.get(plugin_id)
    }

    /// 移除缓存
    pub fn remove(&self, plugin_id: &PluginId) {
        self.cache.invalidate(plugin_id);
        tracing::info!("Module cache invalidated: {:?}", plugin_id);
    }

    /// 缓存统计
    pub fn stats(&self) -> CacheStats {
        CacheStats {
            entry_count: self.cache.entry_count(),
            // moka 0.12 不暴露 hit/miss count
            hit_count: 0,
            miss_count: 0,
        }
    }
}

/// 缓存统计
#[derive(Debug, Clone)]
pub struct CacheStats {
    pub entry_count: u64,
    pub hit_count: u64,
    pub miss_count: u64,
}

/// 每个插件的池状态
struct PluginPool {
    /// 并发控制信号量
    semaphore: Arc<Semaphore>,

    /// 共享 Linker（已注册 HostAbi）
    linker: Arc<Linker<crate::context::CallContext>>,

    /// 统计信息
    stats: RwLock<PoolStats>,
}

impl PluginPool {
    fn new(engine: &Engine, max_instances: usize) -> Result<Self> {
        let mut linker = Linker::new(engine);
        HostAbi::register_all(&mut linker)?;

        Ok(Self {
            semaphore: Arc::new(Semaphore::new(max_instances)),
            linker: Arc::new(linker),
            stats: RwLock::new(PoolStats::default()),
        })
    }

    /// 获取 Linker
    fn linker(&self) -> &Linker<crate::context::CallContext> {
        &self.linker
    }

    /// 记录实例创建
    fn record_create(&self) {
        let mut stats = self.stats.write();
        stats.total_created += 1;
        stats.active_count += 1;
    }

    /// 记录实例销毁
    fn record_destroy(&self) {
        let mut stats = self.stats.write();
        stats.active_count = stats.active_count.saturating_sub(1);
    }

    /// 获取统计
    fn get_stats(&self) -> PoolStats {
        self.stats.read().clone()
    }
}

/// 池统计
#[derive(Debug, Clone, Default)]
pub struct PoolStats {
    /// 总创建数
    pub total_created: u64,

    /// 当前活跃数
    pub active_count: u64,
}

/// 实例池管理器
pub struct InstancePoolManager {
    /// 每个插件的池
    pools: DashMap<PluginId, Arc<PluginPool>>,

    /// Wasmtime 引擎
    engine: Arc<Engine>,

    /// 池配置
    config: PoolConfig,
}

impl InstancePoolManager {
    /// 创建新的池管理器
    pub fn new(engine: Arc<Engine>, config: PoolConfig) -> Self {
        Self {
            pools: DashMap::new(),
            engine,
            config,
        }
    }

    /// 获取或创建插件池
    fn get_or_create_pool(&self, plugin_id: PluginId) -> Result<Arc<PluginPool>> {
        if let Some(pool) = self.pools.get(&plugin_id) {
            return Ok(pool.clone());
        }

        // 创建新池
        let pool = Arc::new(PluginPool::new(
            &self.engine,
            self.config.max_instances_per_plugin,
        )?);
        self.pools.insert(plugin_id, pool.clone());

        tracing::info!("Created pool for plugin: {:?}", plugin_id);
        Ok(pool)
    }

    /// 获取 Linker
    pub fn get_linker(
        &self,
        plugin_id: PluginId,
    ) -> Result<Arc<Linker<crate::context::CallContext>>> {
        let pool = self.get_or_create_pool(plugin_id)?;
        Ok(pool.linker.clone())
    }

    /// 获取并发许可（RAII guard）
    pub async fn acquire_instance_guard(&self, plugin_id: PluginId) -> Result<InstanceGuard> {
        let pool = self.get_or_create_pool(plugin_id)?;
        let permit = pool
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| WasmError::InternalError("Semaphore closed".to_string()))?;
        pool.record_create();

        Ok(InstanceGuard {
            _permit: permit,
            pool: pool.clone(),
        })
    }

    /// 移除插件池
    pub fn remove_pool(&self, plugin_id: &PluginId) {
        self.pools.remove(plugin_id);
        tracing::info!("Removed pool for plugin: {:?}", plugin_id);
    }

    /// 获取所有池的统计
    pub fn all_stats(&self) -> Vec<(PluginId, PoolStats)> {
        self.pools
            .iter()
            .map(|entry| (*entry.key(), entry.value().get_stats()))
            .collect()
    }
}

/// 实例 Guard（RAII）
pub struct InstanceGuard {
    _permit: tokio::sync::OwnedSemaphorePermit,
    pool: Arc<PluginPool>,
}

impl Drop for InstanceGuard {
    fn drop(&mut self) {
        self.pool.record_destroy();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wasmtime::Config;

    #[test]
    fn test_module_cache() {
        let mut config = Config::new();
        config.consume_fuel(true);
        let engine = Arc::new(Engine::new(&config).unwrap());

        let cache_config = CacheConfig::default();
        let cache = ModuleCache::new(engine.clone(), &cache_config);

        // 简单 WAT 模块
        let wasm = wat::parse_str(
            r#"
            (module
                (func (export "test") (result i32)
                    i32.const 42
                )
            )
        "#,
        )
        .unwrap();

        let plugin_id = PluginId::from_uuid(uuid::Uuid::new_v4());

        // 第一次：编译
        let module1 = cache
            .get_or_compile(plugin_id, Bytes::from(wasm.clone()))
            .unwrap();

        // 第二次：从缓存获取（应该返回同一个 Module）
        let module2 = cache.get_or_compile(plugin_id, Bytes::from(wasm)).unwrap();
        assert!(
            Arc::ptr_eq(&module1, &module2),
            "Should reuse cached module"
        );

        // 直接 get 也能拿到
        let module3 = cache.get(&plugin_id).expect("Should find cached module");
        assert!(
            Arc::ptr_eq(&module1, &module3),
            "Direct get should return same module"
        );
    }

    #[tokio::test]
    async fn test_pool_manager() {
        let mut config = Config::new();
        config.consume_fuel(true);
        let engine = Arc::new(Engine::new(&config).unwrap());

        let pool_config = PoolConfig {
            max_instances_per_plugin: 2,
            idle_timeout_secs: 60,
        };

        let manager = InstancePoolManager::new(engine, pool_config);
        let plugin_id = PluginId::from_uuid(uuid::Uuid::new_v4());

        // 获取 2 个许可
        let guard1 = manager.acquire_instance_guard(plugin_id).await.unwrap();
        let guard2 = manager.acquire_instance_guard(plugin_id).await.unwrap();

        // 第 3 个应该超时
        let result = tokio::time::timeout(
            Duration::from_millis(100),
            manager.acquire_instance_guard(plugin_id),
        )
        .await;
        assert!(result.is_err());

        // 释放一个
        drop(guard1);

        // 现在应该可以获取
        let _guard3 = manager.acquire_instance_guard(plugin_id).await.unwrap();

        drop(guard2);
        drop(_guard3);
    }
}
