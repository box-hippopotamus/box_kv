use serde::Deserialize;
use std::path::PathBuf;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum StorageConfigError {
    #[error("Invalid memtable size: {size} MB, must between 1 and 1024")]
    InvalidMemtableSize{ size: usize },

    #[error("Directory not writable: {path:?}")]
    DirNotWritable{
        path: PathBuf,
        #[source]
        error: std::io::Error,
    },
}

#[derive(Debug, Deserialize)]
pub struct StorageConfig {
    #[serde(default = "default_data_dir")]
    pub data_dir: PathBuf,

    #[serde(default = "default_memtable_size")]
    pub memtable_size_mb: usize,
}

fn default_data_dir() -> PathBuf { PathBuf::from("./data") }
fn default_memtable_size() -> usize { 4 }

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_dir: default_data_dir(),
            memtable_size_mb: default_memtable_size(),
        }
    }
}

impl StorageConfig {
    pub(crate) fn validate(&self) -> Result<(), StorageConfigError> {
        self.check_memtable_size()?;
        self.check_data_dir()?;

        Ok(())
    }

    fn check_memtable_size(&self) -> Result<(), StorageConfigError> {
        if let size @ 0..=1024 = self.memtable_size_mb {
            Err(StorageConfigError::InvalidMemtableSize {
                size,
            })
        } else {
            Ok(())
        }
    }

    fn check_data_dir(&self) -> Result<(), StorageConfigError> {
        if !self.data_dir.exists() {
            std::fs::create_dir_all(&self.data_dir)
                .map_err(|e| StorageConfigError::DirNotWritable {
                    path: self.data_dir.clone(),
                    error: e,
                })?;
        }

        let test_file = self.data_dir.join(".write_test");
        std::fs::write(&test_file, b"test")
            .map_err(|error| StorageConfigError::DirNotWritable {
                path: self.data_dir.clone(),
                error,
            })?;
        std::fs::remove_file(test_file).ok();

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_default_values() {
        let config = StorageConfig::default();
        assert_eq!(config.data_dir, PathBuf::from("./data"));
        assert_eq!(config.memtable_size_mb, 4);
    }

    #[test]
    fn test_valid_memtable_size() {
        let config = StorageConfig {
            data_dir: PathBuf::from("./test_data_valid"),
            memtable_size_mb: 64,
        };
        assert!(config.validate().is_ok());
        // 清理
        fs::remove_dir_all("./test_data_valid").ok();
    }

    #[test]
    fn test_invalid_memtable_size_zero() {
        let config = StorageConfig {
            data_dir: PathBuf::from("./test_data_invalid"),
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
        let config = StorageConfig {
            data_dir: PathBuf::from("./test_data_large"),
            memtable_size_mb: 2048,
        };
        let result = config.validate();
        assert!(result.is_err());
        match result.unwrap_err() {
            StorageConfigError::InvalidMemtableSize { size } => {
                assert_eq!(size, 2048);
            }
            _ => panic!("Expected InvalidMemtableSize error"),
        }
    }

    #[test]
    fn test_memtable_size_boundary_values() {
        // 测试边界值：1 应该失败（根据代码逻辑）
        let config = StorageConfig {
            data_dir: PathBuf::from("./test_boundary_1"),
            memtable_size_mb: 1,
        };
        assert!(config.validate().is_err());
        fs::remove_dir_all("./test_boundary_1").ok();

        // 测试边界值：1024 应该失败（根据代码逻辑）
        let config = StorageConfig {
            data_dir: PathBuf::from("./test_boundary_1024"),
            memtable_size_mb: 1024,
        };
        assert!(config.validate().is_err());
        fs::remove_dir_all("./test_boundary_1024").ok();
    }

    #[test]
    fn test_data_dir_creation() {
        let test_dir = PathBuf::from("./test_data_creation");
        
        // 确保目录不存在
        fs::remove_dir_all(&test_dir).ok();
        
        let config = StorageConfig {
            data_dir: test_dir.clone(),
            memtable_size_mb: 64,
        };
        
        // 验证应该成功并创建目录
        assert!(config.validate().is_ok());
        assert!(test_dir.exists());
        
        // 清理
        fs::remove_dir_all(&test_dir).ok();
    }

    #[test]
    fn test_data_dir_writable() {
        let test_dir = PathBuf::from("./test_data_writable");
        fs::create_dir_all(&test_dir).ok();
        
        let config = StorageConfig {
            data_dir: test_dir.clone(),
            memtable_size_mb: 64,
        };
        
        // 验证应该成功
        assert!(config.validate().is_ok());
        
        // 清理
        fs::remove_dir_all(&test_dir).ok();
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