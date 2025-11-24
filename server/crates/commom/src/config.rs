mod storage;
pub use storage::StorageConfig;

mod server;
pub use server::ServerConfig;

use serde::Deserialize;
use std::env;
use std::path::PathBuf;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("Config file not found: {path:?}")]
    FileNotFound { path: PathBuf },

    #[error("Failed to parse config")]
    ParseError(#[from] config::ConfigError),

    #[error(transparent)]
    Server(#[from] server::ServerConfigError),

    #[error(transparent)]
    Storage(#[from] storage::StorageConfigError),
}

#[derive(Debug, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub storage: StorageConfig,
    
    #[serde(default)]
    pub server: ServerConfig,
}

impl Config {
    pub fn load() -> Result<Self, ConfigError> {
        let mut builder = config::Config::builder();

        // 1. 尝试加载配置文件
        if let Some(config_file) = find_config_file()? {
            builder = builder.add_source(
                config::File::from(config_file).required(true)
            );
        }

        // 2. 环境变量覆盖
        builder = builder.add_source(
            config::Environment::with_prefix("BOXKV")
                .separator("__")
        );

        // 3. 构建并反序列化
        let config: Self = builder
            .build()
            .map_err(ConfigError::ParseError)?
            .try_deserialize()
            .map_err(ConfigError::ParseError)?;

        // 4. 验证
        config.validate()?;

        Ok(config)
    }

    fn validate(&self) -> Result<(), ConfigError> {
        self.storage.validate()?;
        self.server.validate()?;
        Ok(())
    }
}

fn find_config_file() -> Result<Option<PathBuf>, ConfigError> {
    // env
    if let Ok(path) = env::var("BOXKV_CONFIG") {
        let path = PathBuf::from(path);
        return if !path.exists() {
            Err(ConfigError::FileNotFound {path})
        } else {
            Ok(Some(path))
        };
    }

    // work dir
    let default_path = PathBuf::from("./config.toml");
    if default_path.exists() {
        return Ok(Some(default_path));
    }

    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::io::Write;

    fn create_test_config_file(path: &str, content: &str) -> PathBuf {
        let path = PathBuf::from(path);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).ok();
        }
        let mut file = fs::File::create(&path).unwrap();
        file.write_all(content.as_bytes()).unwrap();
        path
    }

    fn cleanup_test_files(paths: &[&str]) {
        for path in paths {
            fs::remove_file(path).ok();
            // 只删除 test_configs 目录下的内容，不要删除其他目录
            let path_buf = PathBuf::from(path);
            if let Some(parent) = path_buf.parent() {
                if parent.to_string_lossy().contains("test_configs") {
                    fs::remove_dir_all(parent).ok();
                }
            }
        }
    }

    #[test]
    fn test_find_config_file_none() {
        // 确保环境变量未设置
        unsafe { env::remove_var("BOXKV_CONFIG"); }
        
        // 确保默认文件不存在
        let default_path = "./config.toml";
        fs::remove_file(default_path).ok();

        let result = find_config_file();
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_find_config_file_default_exists() {
        unsafe { env::remove_var("BOXKV_CONFIG"); }
        
        let config_path = "./config.toml";
        create_test_config_file(config_path, "[storage]\ndata_dir = \"./data\"\n");

        let result = find_config_file();
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Some(PathBuf::from(config_path)));

        cleanup_test_files(&[config_path]);
    }

    #[test]
    fn test_find_config_file_env_exists() {
        let test_config = "./test_configs/env_test.toml";
        create_test_config_file(test_config, "[storage]\ndata_dir = \"./data\"\n");
        
        unsafe { env::set_var("BOXKV_CONFIG", test_config); }

        let result = find_config_file();
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Some(PathBuf::from(test_config)));

        unsafe { env::remove_var("BOXKV_CONFIG"); }
        cleanup_test_files(&[test_config]);
    }

    #[test]
    fn test_find_config_file_env_not_exists() {
        let non_existent = "./test_configs/non_existent.toml";
        unsafe { env::set_var("BOXKV_CONFIG", non_existent); }

        let result = find_config_file();
        assert!(result.is_err());
        
        match result.unwrap_err() {
            ConfigError::FileNotFound { path } => {
                assert_eq!(path, PathBuf::from(non_existent));
            }
            _ => panic!("Expected FileNotFound error"),
        }

        unsafe { env::remove_var("BOXKV_CONFIG"); }
    }

    #[test]
    fn test_find_config_file_env_priority() {
        // 创建两个配置文件
        let env_config = "./test_configs/env_priority.toml";
        let default_config = "./config.toml";
        
        create_test_config_file(env_config, "[storage]\nmemtable_size_mb = 128\n");
        create_test_config_file(default_config, "[storage]\nmemtable_size_mb = 64\n");

        unsafe { env::set_var("BOXKV_CONFIG", env_config); }

        let result = find_config_file();
        assert!(result.is_ok());
        // 应该返回环境变量指定的路径
        assert_eq!(result.unwrap(), Some(PathBuf::from(env_config)));

        unsafe { env::remove_var("BOXKV_CONFIG"); }
        cleanup_test_files(&[env_config, default_config]);
    }

    #[test]
    fn test_config_error_display() {
        let err = ConfigError::FileNotFound {
            path: PathBuf::from("/path/to/config.toml"),
        };
        let msg = format!("{}", err);
        assert!(msg.contains("Config file not found"));
        assert!(msg.contains("/path/to/config.toml"));

        let err = ConfigError::ParseError(
            config::ConfigError::Message("test error".to_string())
        );
        let msg = format!("{}", err);
        assert!(msg.contains("Failed to parse config"));
    }

    #[test]
    fn test_config_default_values() {
        let config_content = r#"
        # 空配置，使用默认值
        "#;
        
        let test_config = "./test_configs/default_test.toml";
        create_test_config_file(test_config, config_content);
        unsafe { env::set_var("BOXKV_CONFIG", test_config); }

        let result = Config::load();
        
        unsafe { env::remove_var("BOXKV_CONFIG"); }
        cleanup_test_files(&[test_config]);
        
        // 清理可能创建的数据目录
        fs::remove_dir_all("./data").ok();

        assert!(result.is_ok());
        let config = result.unwrap();
        assert_eq!(config.storage.data_dir, PathBuf::from("./data"));
        assert_eq!(config.server.host, "127.0.0.1");
        assert_eq!(config.server.port, 21524);
    }

    #[test]
    fn test_config_custom_values() {
        let config_content = r#"
[storage]
data_dir = "./custom_data"
memtable_size_mb = 128

[server]
host = "0.0.0.0"
port = 8080
        "#;
        
        let test_config = "./test_configs/custom_test.toml";
        create_test_config_file(test_config, config_content);
        unsafe { env::set_var("BOXKV_CONFIG", test_config); }

        let result = Config::load();
        
        unsafe { env::remove_var("BOXKV_CONFIG"); }
        cleanup_test_files(&[test_config]);
        
        // 清理创建的目录
        fs::remove_dir_all("./custom_data").ok();

        assert!(result.is_ok());
        let config = result.unwrap();
        assert_eq!(config.storage.data_dir, PathBuf::from("./custom_data"));
        assert_eq!(config.storage.memtable_size_mb, 128);
        assert_eq!(config.server.host, "0.0.0.0");
        assert_eq!(config.server.port, 8080);
    }

    #[test]
    fn test_config_validation_fail_storage() {
        let config_content = r#"
[storage]
memtable_size_mb = 0
        "#;
        
        let test_config = "./test_configs/validation_fail_storage.toml";
        create_test_config_file(test_config, config_content);
        unsafe { env::set_var("BOXKV_CONFIG", test_config); }

        let result = Config::load();
        
        unsafe { env::remove_var("BOXKV_CONFIG"); }
        cleanup_test_files(&[test_config]);
        
        fs::remove_dir_all("./data").ok();

        assert!(result.is_err());
        // 验证错误类型
        match result.unwrap_err() {
            ConfigError::Storage(_) => {}, // 正确
            _ => panic!("Expected Storage error"),
        }
    }

    #[test]
    fn test_config_validation_fail_server() {
        let config_content = r#"
[server]
host = "invalid-host"
port = 8080
        "#;
        
        let test_config = "./test_configs/validation_fail_server.toml";
        create_test_config_file(test_config, config_content);
        unsafe { env::set_var("BOXKV_CONFIG", test_config); }

        let result = Config::load();
        
        unsafe { env::remove_var("BOXKV_CONFIG"); }
        cleanup_test_files(&[test_config]);
        
        fs::remove_dir_all("./data").ok();

        assert!(result.is_err());
        match result.unwrap_err() {
            ConfigError::Server(_) => {}, // 正确
            _ => panic!("Expected Server error"),
        }
    }

    #[test]
    fn test_config_parse_error() {
        let config_content = r#"
[storage]
memtable_size_mb = "not_a_number"
        "#;
        
        let test_config = "./test_configs/parse_error_test.toml";
        create_test_config_file(test_config, config_content);
        unsafe { env::set_var("BOXKV_CONFIG", test_config); }

        let result = Config::load();
        
        unsafe { env::remove_var("BOXKV_CONFIG"); }
        cleanup_test_files(&[test_config]);

        assert!(result.is_err());
        match result.unwrap_err() {
            ConfigError::ParseError(_) => {}, // 正确
            _ => panic!("Expected ParseError"),
        }
    }
}

