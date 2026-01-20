//! 同步 RandomAccessFileReader
//!
use crate::error::StorageError;
use crate::sync_fs::traits::ReadableFile;
use bytes::Bytes;

/// 随机访问文件读取器
pub struct RandomAccessFileReader {
    /// 底层文件
    file: Box<dyn ReadableFile>,

    /// 读取缓冲区
    _buffer: Option<Bytes>,
}

impl RandomAccessFileReader {
    /// 创建新的 RandomAccessFileReader
    pub fn new(file: Box<dyn ReadableFile>) -> Self {
        Self {
            file,
            _buffer: None,
        }
    }

    /// 从指定偏移量读取指定大小的数据
    pub fn read(&self, offset: u64, size: usize) -> Result<Bytes, StorageError> {
        self.file.read_at(offset, size)
    }

    /// 读取整个文件
    pub fn read_all(&self) -> Result<Bytes, StorageError> {
        self.file.read_all()
    }

    /// 预读数据（用于性能优化）
    pub fn prefetch(&self, offset: u64, size: usize) -> Result<(), StorageError> {
        self.file.prefetch(offset, size)
    }
}
