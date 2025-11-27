mod storage;
pub use storage::StorageConfig;

mod server;
pub use server::ServerConfig;

use serde::Deserialize;
use std::env;
use std::path::PathBuf;
use std::sync::OnceLock;
use thiserror::Error;
use tracing::{debug, info};

/// Errors that can occur during configuration loading or validation.
#[derive(Debug, Error)]
pub enum ConfigError {
    /// The configuration file specified by `BOXKV_CONFIG` was not found.
    #[error("Config file not found: {path:?}")]
    FileNotFound { path: PathBuf },

    /// Failed to parse the configuration file or environment variables.
    #[error("Failed to parse config")]
    ParseError(#[from] config::ConfigError),

    /// Error in server configuration validation.
    #[error(transparent)]
    Server(#[from] server::ServerConfigError),

    /// Error in storage configuration validation.
    #[error(transparent)]
    Storage(#[from] storage::StorageConfigError),
}

/// The global configuration for the BoxKV server.
///
/// This struct aggregates configurations for various subsystems (storage, server, etc.).
/// It is designed to be loaded once at startup and accessed globally via `Config::global()`.
#[derive(Debug, Deserialize)]
pub struct Config {
    /// Configuration for the storage engine.
    #[serde(default)]
    pub storage: StorageConfig,

    /// Configuration for the network server.
    #[serde(default)]
    pub server: ServerConfig,
}

static CONFIG: OnceLock<Config> = OnceLock::new();

impl Config {
    /// Returns a reference to the global configuration singleton.
    ///
    /// # Panics
    ///
    /// Panics if `Config::init()` has not been called successfully before calling this method.
    pub fn global() -> &'static Self {
        CONFIG
            .get()
            .expect("Config is not initialized! Call Config::init() first.")
    }

    /// Initializes the global configuration.
    ///
    /// This method should be called once at the start of the application. It loads the configuration
    /// from files and environment variables, validates it, and sets the global singleton.
    ///
    /// If the configuration is already initialized, this method does nothing and returns `Ok(())`.
    ///
    /// # Errors
    ///
    /// Returns `ConfigError` if loading or validation fails.
    pub fn init() -> Result<(), ConfigError> {
        if CONFIG.get().is_none() {
            info!("Initializing BoxKV configuration");
            let config = Config::load()?;
            let _ = CONFIG.set(config);
        }

        Ok(())
    }

    fn load() -> Result<Self, ConfigError> {
        let mut builder = config::Config::builder();

        // 1. Try to load the configuration file
        if let Some(config_file) = Self::find_config_file()? {
            info!(?config_file, "Loading configuration file");
            builder = builder.add_source(config::File::from(config_file).required(true));
        } else {
            info!("No config file found, using defaults and environment variables");
        }

        // 2. Environment variable override
        builder = builder
            .add_source(config::Environment::with_prefix(ENV_PREFIX).separator(ENV_SEPARATOR));

        // 3. Build and deserialize
        let config: Self = builder
            .build()
            .map_err(ConfigError::ParseError)?
            .try_deserialize()
            .map_err(ConfigError::ParseError)?;

        // 4. Validate
        config.validate()?;

        debug!(
            data_dir = ?config.storage.data_dir,
            memtable_size_mb = config.storage.memtable_size_mb,
            host = %config.server.host,
            port = config.server.port,
            "Configuration loaded and validated"
        );

        Ok(config)
    }

    fn validate(&self) -> Result<(), ConfigError> {
        self.storage.validate()?;
        self.server.validate()?;
        Ok(())
    }

    fn find_config_file() -> Result<Option<PathBuf>, ConfigError> {
        // Check environment variable
        if let Ok(path) = env::var(ENV_VAR_CONFIG_FILE) {
            let path = PathBuf::from(path);
            return if !path.exists() {
                Err(ConfigError::FileNotFound { path })
            } else {
                Ok(Some(path))
            };
        }

        // Check working directory
        let default_path = PathBuf::from(DEFAULT_CONFIG_PATH);
        if default_path.exists() {
            return Ok(Some(default_path));
        }

        Ok(None)
    }
}

const ENV_PREFIX: &str = "BOXKV";
const ENV_SEPARATOR: &str = "__";
const ENV_VAR_CONFIG_FILE: &str = "BOXKV_CONFIG";
const DEFAULT_CONFIG_PATH: &str = "./config.toml";

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_find_config_file_none() {
        unsafe {
            env::remove_var(ENV_VAR_CONFIG_FILE);
        }

        // Save and remove default config if exists
        let default_path = PathBuf::from(DEFAULT_CONFIG_PATH);
        let backup = if default_path.exists() {
            let content = fs::read(&default_path).ok();
            fs::remove_file(&default_path).ok();
            content
        } else {
            None
        };

        let result = Config::find_config_file();

        // Restore default config if it existed
        if let Some(content) = backup {
            fs::write(&default_path, content).ok();
        }

        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_find_config_file_default_exists() {
        unsafe {
            env::remove_var(ENV_VAR_CONFIG_FILE);
        }

        let config_path = PathBuf::from(DEFAULT_CONFIG_PATH);

        // Backup existing config
        let backup = if config_path.exists() {
            let content = fs::read(&config_path).ok();
            fs::remove_file(&config_path).ok();
            content
        } else {
            None
        };

        // Create test config
        fs::write(&config_path, "[storage]\ndata_dir = \"./data\"\n").unwrap();

        let result = Config::find_config_file();

        // Cleanup and restore
        fs::remove_file(&config_path).ok();
        if let Some(content) = backup {
            fs::write(&config_path, content).ok();
        }

        assert!(
            result.is_ok(),
            "find_config_file failed: {:?}",
            result.err()
        );
        assert_eq!(result.unwrap(), Some(config_path));
    }

    #[test]
    fn test_find_config_file_env_exists() {
        unsafe {
            env::remove_var(ENV_VAR_CONFIG_FILE);
        }

        let temp_dir = tempfile::tempdir().unwrap();
        let test_config = temp_dir.path().join("env_test.toml");

        fs::write(&test_config, "[storage]\ndata_dir = \"./data\"\n").unwrap();

        unsafe {
            env::set_var(ENV_VAR_CONFIG_FILE, &test_config);
        }

        let result = Config::find_config_file();

        unsafe {
            env::remove_var(ENV_VAR_CONFIG_FILE);
        }

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Some(test_config));
    }

    #[test]
    fn test_find_config_file_env_not_exists() {
        unsafe {
            env::remove_var(ENV_VAR_CONFIG_FILE);
        }

        let temp_dir = tempfile::tempdir().unwrap();
        let non_existent = temp_dir.path().join("non_existent.toml");

        unsafe {
            env::set_var(ENV_VAR_CONFIG_FILE, &non_existent);
        }

        let result = Config::find_config_file();

        unsafe {
            env::remove_var(ENV_VAR_CONFIG_FILE);
        }

        assert!(result.is_err());

        match result.unwrap_err() {
            ConfigError::FileNotFound { path } => {
                assert_eq!(path, non_existent);
            }
            _ => panic!("Expected FileNotFound error"),
        }
    }

    #[test]
    fn test_find_config_file_env_priority() {
        unsafe {
            env::remove_var(ENV_VAR_CONFIG_FILE);
        }

        let temp_dir = tempfile::tempdir().unwrap();
        let env_config = temp_dir.path().join("env_priority.toml");
        let default_config = PathBuf::from(DEFAULT_CONFIG_PATH);

        // Backup default config
        let backup = if default_config.exists() {
            let content = fs::read(&default_config).ok();
            fs::remove_file(&default_config).ok();
            content
        } else {
            None
        };

        fs::write(&env_config, "[storage]\nmemtable_size_mb = 128\n").unwrap();
        fs::write(&default_config, "[storage]\nmemtable_size_mb = 64\n").unwrap();

        unsafe {
            env::set_var(ENV_VAR_CONFIG_FILE, &env_config);
        }

        let result = Config::find_config_file();

        unsafe {
            env::remove_var(ENV_VAR_CONFIG_FILE);
        }

        // Cleanup default config and restore
        fs::remove_file(&default_config).ok();
        if let Some(content) = backup {
            fs::write(&default_config, content).ok();
        }

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Some(env_config));
    }

    #[test]
    fn test_config_error_display() {
        let err = ConfigError::FileNotFound {
            path: PathBuf::from("/path/to/config.toml"),
        };
        let msg = format!("{}", err);
        assert!(msg.contains("Config file not found"));
        assert!(msg.contains("/path/to/config.toml"));

        let err = ConfigError::ParseError(config::ConfigError::Message("test error".to_string()));
        let msg = format!("{}", err);
        assert!(msg.contains("Failed to parse config"));
    }

    #[test]
    fn test_config_default_values() {
        unsafe {
            env::remove_var(ENV_VAR_CONFIG_FILE);
        }

        let temp_dir = tempfile::tempdir().unwrap();
        let test_data_dir = temp_dir.path().join("data");
        let test_config = temp_dir.path().join("default_test.toml");

        // Use forward slashes for TOML compatibility (works on both Windows and Unix)
        let data_dir_str = test_data_dir.display().to_string().replace('\\', "/");

        let config_content = format!(
            r#"
[storage]
data_dir = "{}"

[server]
host = "127.0.0.1"
port = 21524
"#,
            data_dir_str
        );

        fs::write(&test_config, config_content).unwrap();

        unsafe {
            env::set_var(ENV_VAR_CONFIG_FILE, &test_config);
        }

        let result = Config::load();

        unsafe {
            env::remove_var(ENV_VAR_CONFIG_FILE);
        }

        assert!(result.is_ok(), "Config load failed: {:?}", result.err());
        let config = result.unwrap();
        assert_eq!(config.storage.data_dir, test_data_dir);
        assert_eq!(config.server.host, "127.0.0.1");
        assert_eq!(config.server.port, 21524);
    }

    #[test]
    fn test_config_custom_values() {
        unsafe {
            env::remove_var(ENV_VAR_CONFIG_FILE);
        }

        let temp_dir = tempfile::tempdir().unwrap();
        let data_dir = temp_dir.path().join("data");
        let test_config = temp_dir.path().join("custom_test.toml");

        // Use forward slashes for TOML compatibility (works on both Windows and Unix)
        let data_dir_str = data_dir.display().to_string().replace('\\', "/");

        let config_content = format!(
            r#"
[storage]
data_dir = "{}"
memtable_size_mb = 128

[server]
host = "0.0.0.0"
port = 8080
"#,
            data_dir_str
        );

        fs::write(&test_config, config_content).unwrap();

        unsafe {
            env::set_var(ENV_VAR_CONFIG_FILE, &test_config);
        }

        let result = Config::load();

        unsafe {
            env::remove_var(ENV_VAR_CONFIG_FILE);
        }

        assert!(result.is_ok(), "Config load failed: {:?}", result.err());
        let config = result.unwrap();
        assert_eq!(config.storage.data_dir, data_dir);
        assert_eq!(config.storage.memtable_size_mb, 128);
        assert_eq!(config.server.host, "0.0.0.0");
        assert_eq!(config.server.port, 8080);
    }

    #[test]
    fn test_config_validation_fail_storage() {
        unsafe {
            env::remove_var(ENV_VAR_CONFIG_FILE);
        }

        let temp_dir = tempfile::tempdir().unwrap();
        let test_config = temp_dir.path().join("validation_fail_storage.toml");

        let config_content = r#"
[storage]
memtable_size_mb = 0

[server]
host = "127.0.0.1"
port = 8080
"#;

        fs::write(&test_config, config_content).unwrap();

        unsafe {
            env::set_var(ENV_VAR_CONFIG_FILE, &test_config);
        }

        let result = Config::load();

        unsafe {
            env::remove_var(ENV_VAR_CONFIG_FILE);
        }

        assert!(result.is_err(), "Expected error but got Ok");
        match result.unwrap_err() {
            ConfigError::Storage(_) => {} // Expected
            e => panic!("Expected Storage error, got: {:?}", e),
        }
    }

    #[test]
    fn test_config_validation_fail_server() {
        unsafe {
            env::remove_var(ENV_VAR_CONFIG_FILE);
        }

        let temp_dir = tempfile::tempdir().unwrap();
        let data_dir = temp_dir.path().join("data");
        let test_config = temp_dir.path().join("validation_fail_server.toml");

        // Use forward slashes for TOML compatibility (works on both Windows and Unix)
        let data_dir_str = data_dir.display().to_string().replace('\\', "/");

        let config_content = format!(
            r#"
[storage]
data_dir = "{}"
memtable_size_mb = 64

[server]
host = "invalid-host"
port = 8080
"#,
            data_dir_str
        );

        fs::write(&test_config, config_content).unwrap();

        unsafe {
            env::set_var(ENV_VAR_CONFIG_FILE, &test_config);
        }

        let result = Config::load();

        unsafe {
            env::remove_var(ENV_VAR_CONFIG_FILE);
        }

        assert!(result.is_err(), "Expected error but got Ok");
        match result.unwrap_err() {
            ConfigError::Server(_) => {} // Expected
            e => panic!("Expected Server error, but got: {:?}", e),
        }
    }

    #[test]
    fn test_config_parse_error() {
        unsafe {
            env::remove_var(ENV_VAR_CONFIG_FILE);
        }

        let temp_dir = tempfile::tempdir().unwrap();
        let test_config = temp_dir.path().join("parse_error_test.toml");

        let config_content = r#"
[storage]
memtable_size_mb = "not_a_number"
"#;

        fs::write(&test_config, config_content).unwrap();

        unsafe {
            env::set_var(ENV_VAR_CONFIG_FILE, &test_config);
        }

        let result = Config::load();

        unsafe {
            env::remove_var(ENV_VAR_CONFIG_FILE);
        }

        assert!(result.is_err());
        match result.unwrap_err() {
            ConfigError::ParseError(_) => {} // Expected
            _ => panic!("Expected ParseError"),
        }
    }
}
