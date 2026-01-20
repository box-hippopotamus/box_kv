//! 错误类型定义

use thiserror::Error;

/// Wasm 错误类型
#[derive(Error, Debug)]
pub enum WasmError {
    /// 模块编译失败
    #[error("Failed to compile module: {0}")]
    CompilationFailed(String),

    /// 模块实例化失败
    #[error("Failed to instantiate module: {0}")]
    InstantiationFailed(String),

    /// 插件未找到
    #[error("Plugin not found: id={0}, version={1}")]
    PluginNotFound(u64, u64),

    /// 记录未找到
    #[error("Record not found for id: {0}")]
    RecordNotFound(String),

    /// 插件 ID 未找到
    #[error("Plugin ID not found: {0}")]
    PluginIdNotFound(String),

    /// 插件 Key 未找到
    #[error("Plugin key not found: {0}")]
    PluginKeyNotFound(String),

    /// 实例池耗尽
    #[error("Instance pool exhausted for plugin {0}:{1}")]
    InstancePoolExhausted(u64, u64),

    /// 超时
    #[error("Execution timeout: {0}ms")]
    Timeout(u64),

    /// Fuel 耗尽
    #[error("Fuel exhausted: consumed {0}")]
    FuelExhausted(u64),

    /// 内存越界
    #[error("Memory out of bounds: {0}..{1}, size={2}")]
    MemoryOutOfBounds(usize, usize, usize),

    /// 无效句柄
    #[error("Invalid handle: {0}")]
    InvalidHandle(u32),

    /// 函数未找到
    #[error("Function not found: {0}")]
    FunctionNotFound(String),

    /// Trap（WASM 运行时错误）
    #[error("WASM trap: {0}")]
    Trap(String),

    /// 配置错误
    #[error("Configuration error: {0}")]
    ConfigError(String),

    /// ABI 违规
    #[error("ABI violation: {0}")]
    AbiViolation(String),

    /// IO 错误
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    /// Wasmtime 错误
    #[error("Wasmtime error: {0}")]
    WasmtimeError(#[from] wasmtime::Error),

    /// 内部错误
    #[error("Internal error: {0}")]
    InternalError(String),
}

/// Result 类型别名
pub type Result<T> = std::result::Result<T, WasmError>;

impl WasmError {
    /// 创建配置错误
    pub fn config_error(msg: impl Into<String>) -> Self {
        Self::ConfigError(msg.into())
    }

    /// 创建内部错误
    pub fn internal_error(msg: impl Into<String>) -> Self {
        Self::InternalError(msg.into())
    }

    /// 转换为 core 的 DB 错误
    pub fn to_db_error(self) -> boxkv_core::db::error::DBError {
        boxkv_core::db::error::DBError::PluginRejected(self.to_string())
    }
}
