//! 同步文件系统 Trait 定义

use crate::error::StorageError;
use bytes::Bytes;
use std::path::Path;

/// 文件系统抽象接口
pub trait FileSystem: Send + Sync {
    /// 打开文件用于读取
    fn open_read(&self, path: &Path) -> Result<Box<dyn ReadableFile>, StorageError>;

    /// 打开文件用于写入（创建新文件，如果存在则截断）
    fn open_write(&self, path: &Path) -> Result<Box<dyn WritableFile>, StorageError>;

    /// 删除文件
    fn delete(&self, path: &Path) -> Result<(), StorageError>;

    /// 列出目录内容
    fn list_dir(&self, path: &Path) -> Result<Vec<String>, StorageError>;

    /// 创建目录（递归创建）
    fn create_dir(&self, path: &Path) -> Result<(), StorageError>;

    /// 检查文件或目录是否存在
    fn exists(&self, path: &Path) -> bool;

    /// 获取文件大小
    fn file_size(&self, path: &Path) -> Result<u64, StorageError>;

    /// 原子重命名文件或目录
    fn rename(&self, from: &Path, to: &Path) -> Result<(), StorageError>;
}

/// 可写文件接口
pub trait WritableFile: Send {
    /// 追加数据到文件末尾
    fn write(&mut self, data: &[u8]) -> Result<(), StorageError>;

    /// 刷新缓冲区到操作系统
    fn flush(&mut self) -> Result<(), StorageError>;

    /// 同步数据到磁盘
    fn sync(&mut self) -> Result<(), StorageError>;

    /// 关闭文件
    fn close(self: Box<Self>) -> Result<(), StorageError>;

    /// 获取当前文件大小
    fn get_file_size(&self) -> Result<u64, StorageError>;

    /// 获取所需的缓冲区对齐大小
    fn get_required_buffer_alignment(&self) -> usize {
        4096
    }
}

/// 可读文件接口
pub trait ReadableFile: Send + Sync {
    /// 从指定偏移量读取指定大小的数据
    fn read_at(&self, offset: u64, size: usize) -> Result<Bytes, StorageError>;

    /// 读取整个文件
    fn read_all(&self) -> Result<Bytes, StorageError>;

    /// 预读数据
    fn prefetch(&self, _offset: u64, _size: usize) -> Result<(), StorageError> {
        Ok(())
    }
}
