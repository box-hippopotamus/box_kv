use serde::Deserialize;
use std::path::PathBuf;
use thiserror::Error;
use tracing::info;

/// Errors that can occur during storage configuration validation.
#[derive(Debug, Error)]
pub enum StorageConfigError {
    /// The memtable size is outside the allowed range (1-1024 MB).
    #[error("Invalid memtable size: {size} MB, must between 1 and 1024")]
    InvalidMemtableSize { size: usize },

    /// The data directory is not writable or cannot be created.
    #[error("Directory not writable: {path:?}")]
    DirNotWritable {
        path: PathBuf,
        #[source]
        error: std::io::Error,
    },
}

/// Configuration for the storage engine.
#[derive(Debug, Deserialize)]
pub struct StorageConfig {
    /// The path to the directory where data files will be stored.
    /// Defaults to "./data".
    #[serde(default = "default_data_dir")]
    pub data_dir: PathBuf,

    /// The size of the MemTable in megabytes.
    /// Must be between 1 and 1024.
    /// Defaults to 4 MB.
    #[serde(default = "default_memtable_size")]
    pub memtable_size_mb: usize,
}

const DEFAULT_DATA_DIR: &str = "./data";
const DEFAULT_MEMTABLE_SIZE_MB: usize = 4;
const MIN_MEMTABLE_SIZE_MB: usize = 1;
const MAX_MEMTABLE_SIZE_MB: usize = 1024;

fn default_data_dir() -> PathBuf {
    PathBuf::from(DEFAULT_DATA_DIR)
}
fn default_memtable_size() -> usize {
    DEFAULT_MEMTABLE_SIZE_MB
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_dir: default_data_dir(),
            memtable_size_mb: default_memtable_size(),
        }
    }
}

impl StorageConfig {
    /// Validates the storage configuration.
    ///
    /// Checks:
    /// 1. `memtable_size_mb` is within the valid range (1-1024).
    /// 2. `data_dir` is writable (creates the directory if it doesn't exist).
    pub(crate) fn validate(&self) -> Result<(), StorageConfigError> {
        self.check_memtable_size()?;
        self.check_data_dir()?;

        Ok(())
    }

    fn check_memtable_size(&self) -> Result<(), StorageConfigError> {
        if (MIN_MEMTABLE_SIZE_MB..=MAX_MEMTABLE_SIZE_MB).contains(&self.memtable_size_mb) {
            Ok(())
        } else {
            Err(StorageConfigError::InvalidMemtableSize {
                size: self.memtable_size_mb,
            })
        }
    }

    fn check_data_dir(&self) -> Result<(), StorageConfigError> {
        if !self.data_dir.exists() {
            info!(?self.data_dir, "Creating data directory");
            std::fs::create_dir_all(&self.data_dir).map_err(|e| {
                StorageConfigError::DirNotWritable {
                    path: self.data_dir.clone(),
                    error: e,
                }
            })?;
        }

        let test_file = self.data_dir.join(".write_test");
        std::fs::write(&test_file, b"test").map_err(|error| {
            StorageConfigError::DirNotWritable {
                path: self.data_dir.clone(),
                error,
            }
        })?;
        std::fs::remove_file(test_file).ok();

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_values() {
        let config = StorageConfig::default();
        assert_eq!(config.data_dir, PathBuf::from("./data"));
        assert_eq!(config.memtable_size_mb, 4);
    }

    #[test]
    fn test_valid_memtable_size() {
        let temp_dir = tempfile::tempdir().unwrap();

        let config = StorageConfig {
            data_dir: temp_dir.path().to_path_buf(),
            memtable_size_mb: 64,
        };

        let result = config.validate();
        assert!(result.is_ok(), "Validation failed: {:?}", result.err());
    }

    #[test]
    fn test_invalid_memtable_size_zero() {
        let temp_dir = tempfile::tempdir().unwrap();

        let config = StorageConfig {
            data_dir: temp_dir.path().to_path_buf(),
            memtable_size_mb: 0,
        };

        let result = config.validate();
        assert!(result.is_err());
        match result.unwrap_err() {
            StorageConfigError::InvalidMemtableSize { size } => {
                assert_eq!(size, 0);
            }
            _ => panic!("Expected InvalidMemtableSize error"),
        }
    }

    #[test]
    fn test_invalid_memtable_size_too_large() {
        let temp_dir = tempfile::tempdir().unwrap();

        let config = StorageConfig {
            data_dir: temp_dir.path().to_path_buf(),
            memtable_size_mb: 2048,
        };

        let result = config.validate();
        assert!(result.is_err(), "Expected error for size 2048, but got Ok");
        match result.unwrap_err() {
            StorageConfigError::InvalidMemtableSize { size } => {
                assert_eq!(size, 2048);
            }
            e => panic!("Expected InvalidMemtableSize error, got: {:?}", e),
        }
    }

    #[test]
    fn test_memtable_size_boundary_values() {
        // min valid value
        let temp_dir_1 = tempfile::tempdir().unwrap();
        let config = StorageConfig {
            data_dir: temp_dir_1.path().to_path_buf(),
            memtable_size_mb: 1,
        };
        let result = config.validate();
        assert!(result.is_ok(), "Size 1 should be valid");

        // max valid value
        let temp_dir_1024 = tempfile::tempdir().unwrap();
        let config = StorageConfig {
            data_dir: temp_dir_1024.path().to_path_buf(),
            memtable_size_mb: 1024,
        };
        let result = config.validate();
        assert!(result.is_ok(), "Size 1024 should be valid");

        // below min
        let temp_dir_0 = tempfile::tempdir().unwrap();
        let config = StorageConfig {
            data_dir: temp_dir_0.path().to_path_buf(),
            memtable_size_mb: 0,
        };
        let result = config.validate();
        assert!(result.is_err(), "Size 0 should be invalid");

        // above max
        let temp_dir_1025 = tempfile::tempdir().unwrap();
        let config = StorageConfig {
            data_dir: temp_dir_1025.path().to_path_buf(),
            memtable_size_mb: 1025,
        };
        let result = config.validate();
        assert!(result.is_err(), "Size 1025 should be invalid");
    }

    #[test]
    fn test_data_dir_creation() {
        let temp_dir = tempfile::tempdir().unwrap();
        let test_path = temp_dir.path().join("subdir");

        let config = StorageConfig {
            data_dir: test_path.clone(),
            memtable_size_mb: 64,
        };

        // Should succeed and create directory
        let result = config.validate();
        assert!(result.is_ok(), "Validation failed: {:?}", result.err());
        assert!(test_path.exists(), "Directory was not created");
    }

    #[test]
    fn test_data_dir_writable() {
        let temp_dir = tempfile::tempdir().unwrap();

        let config = StorageConfig {
            data_dir: temp_dir.path().to_path_buf(),
            memtable_size_mb: 64,
        };

        let result = config.validate();
        assert!(result.is_ok(), "Validation failed: {:?}", result.err());
    }

    #[test]
    fn test_error_display() {
        let err = StorageConfigError::InvalidMemtableSize { size: 0 };
        let msg = format!("{}", err);
        assert!(msg.contains("Invalid memtable size"));
        assert!(msg.contains("0"));

        let err = StorageConfigError::DirNotWritable {
            path: PathBuf::from("/invalid/path"),
            error: std::io::Error::new(std::io::ErrorKind::PermissionDenied, "test"),
        };
        let msg = format!("{}", err);
        assert!(msg.contains("Directory not writable"));
        assert!(msg.contains("/invalid/path"));
    }
}
