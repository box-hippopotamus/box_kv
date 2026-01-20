use serde::{Deserialize, Serialize};

/// WASM 全局配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WasmConfig {
    /// 是否启用 WASM Hook Provider（全局开关）
    pub enabled: bool,

    /// Plugin Service 配置
    pub plugin: PluginServiceConfig,

    /// Runtime 配置
    pub runtime: WasmRuntimeConfig,
}

impl Default for WasmConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            plugin: PluginServiceConfig::default(),
            runtime: WasmRuntimeConfig::default(),
        }
    }
}

impl WasmConfig {
    /// 生产环境配置
    pub fn production() -> Self {
        Self {
            enabled: true,
            plugin: PluginServiceConfig::production(),
            runtime: WasmRuntimeConfig::production(),
        }
    }

    /// 严格模式配置（不受信任的插件）
    pub fn strict() -> Self {
        Self {
            enabled: true,
            plugin: PluginServiceConfig::default(),
            runtime: WasmRuntimeConfig::strict(),
        }
    }
}

/// Plugin Service 配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginServiceConfig {
    /// Blob 存储路径
    pub blobs_path: String,

    /// Registry 存储路径
    pub registry_path: String,

    /// 启动时预热插件（按 ID 列表）
    pub prewarm_on_start: Vec<String>,

    /// 最大模块大小（MB）
    pub max_module_size_mb: usize,

    /// 上传时执行预检验证
    pub upload_validation_enabled: bool,

    /// 允许的导入命名空间白名单
    pub allowed_import_namespaces: Vec<String>,
}

impl Default for PluginServiceConfig {
    fn default() -> Self {
        Self {
            blobs_path: "./data/wasm/blobs".to_string(),
            registry_path: "./data/wasm/registry".to_string(),
            prewarm_on_start: Vec::new(),
            max_module_size_mb: 16,
            upload_validation_enabled: true,
            allowed_import_namespaces: vec!["boxkv_host".to_string()],
        }
    }
}

impl PluginServiceConfig {
    pub fn production() -> Self {
        Self {
            max_module_size_mb: 32,
            upload_validation_enabled: true,
            ..Default::default()
        }
    }
}

/// WASM Runtime 配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WasmRuntimeConfig {
    /// 预算配置
    pub budget: WasmBudgetConfig,

    /// 实例池配置
    pub pool: WasmPoolConfig,

    /// 模块缓存配置
    pub cache: WasmCacheConfig,

    /// Epoch ticker 间隔（毫秒）
    pub epoch_tick_ms: u64,
}

impl Default for WasmRuntimeConfig {
    fn default() -> Self {
        Self {
            budget: WasmBudgetConfig::default(),
            pool: WasmPoolConfig::default(),
            cache: WasmCacheConfig::default(),
            epoch_tick_ms: 10,
        }
    }
}

impl WasmRuntimeConfig {
    pub fn production() -> Self {
        Self {
            budget: WasmBudgetConfig::default(),
            pool: WasmPoolConfig {
                max_instances_per_plugin: 20,
                idle_timeout_secs: 300,
            },
            cache: WasmCacheConfig {
                max_modules: 100,
                enable_compilation_cache: true,
            },
            epoch_tick_ms: 10,
        }
    }

    pub fn strict() -> Self {
        Self {
            budget: WasmBudgetConfig {
                max_fuel: 100_000,
                timeout_ms: 50,
                max_memory_bytes: 16 * 1024 * 1024,
                max_kv_get_count: 10,
                max_bytes_read_total: 1024 * 1024,
            },
            pool: WasmPoolConfig {
                max_instances_per_plugin: 5,
                idle_timeout_secs: 60,
            },
            cache: WasmCacheConfig {
                max_modules: 50,
                enable_compilation_cache: true,
            },
            epoch_tick_ms: 5,
        }
    }
}

/// WASM 预算配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WasmBudgetConfig {
    /// 最大 Fuel（CPU 配额）
    pub max_fuel: u64,

    /// 单次调用超时（毫秒）
    pub timeout_ms: u64,

    /// 最大内存字节数
    pub max_memory_bytes: u64,

    /// 最大 kv_get 次数
    pub max_kv_get_count: u32,

    /// 最大字节读取总量
    pub max_bytes_read_total: usize,
}

impl Default for WasmBudgetConfig {
    fn default() -> Self {
        Self {
            max_fuel: 1_000_000,
            timeout_ms: 100,
            max_memory_bytes: 64 * 1024 * 1024,
            max_kv_get_count: 100,
            max_bytes_read_total: 10 * 1024 * 1024,
        }
    }
}

/// WASM 实例池配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WasmPoolConfig {
    /// 每个插件的最大实例数
    pub max_instances_per_plugin: usize,

    /// 空闲超时（秒）
    pub idle_timeout_secs: u64,
}

impl Default for WasmPoolConfig {
    fn default() -> Self {
        Self {
            max_instances_per_plugin: 10,
            idle_timeout_secs: 180,
        }
    }
}

/// WASM 模块缓存配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WasmCacheConfig {
    /// 最大缓存模块数
    pub max_modules: usize,

    /// 启用编译缓存
    pub enable_compilation_cache: bool,
}

impl Default for WasmCacheConfig {
    fn default() -> Self {
        Self {
            max_modules: 50,
            enable_compilation_cache: true,
        }
    }
}
