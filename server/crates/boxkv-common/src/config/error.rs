//! 配置加载与校验的错误类型定义
//!
//! 说明：用于标识配置文件解析、校验与初始化过程中的问题。
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("Failed to load configuration: {0}")]
    LoadFailed(String),

    #[error("Invalid configuration value: {0}")]
    InvalidValue(String),

    #[error("Missing required field: {0}")]
    MissingField(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Configuration already initialized")]
    AlreadyInitialized,

    #[error("TOML parse error: {0}")]
    TomlParseError(String),

    #[error("TOML serialize error: {0}")]
    TomlSerializeError(String),
}
