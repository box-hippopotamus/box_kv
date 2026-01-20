use serde::{Deserialize, Serialize};

/// 服务器配置（仅网络和服务相关，路径配置在各自的模块中）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    /// 监听主机地址
    pub host: String,
    /// 监听端口（默认 21542）
    pub port: u16,
    /// 工作线程数
    pub workers: usize,
    /// 最大连接数
    pub max_connections: usize,
    /// 请求超时时间（毫秒）
    pub request_timeout_ms: u64,
    /// 日志文件目录
    pub log_dir: String,
    /// 日志文件名前缀
    pub log_file_prefix: String,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: "0.0.0.0".to_string(),
            port: 21542,
            workers: num_cpus::get(),
            max_connections: 10000,
            request_timeout_ms: 5000,
            log_dir: "./logs".to_string(),
            log_file_prefix: "boxkv".to_string(),
        }
    }
}
