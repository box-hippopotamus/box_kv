//! 异步 WritableFileWriter

use bytes::BytesMut;
use std::cmp;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::async_fs::traits::AsyncWritableFile;
use crate::error::StorageError;

/// 默认写缓冲区大小（64KB）
pub const DEFAULT_WRITE_BUFFER_SIZE: usize = 64 * 1024;

/// 初始缓冲区容量（64KB）
const INITIAL_BUFFER_CAPACITY: usize = 64 * 1024;

/// 异步写入文件包装器
pub struct AsyncWritableFileWriter {
    /// 底层文件
    file: Box<dyn AsyncWritableFile>,

    /// 文件大小
    file_size: AtomicU64,

    /// 已 flush 到文件的大小
    flushed_size: AtomicU64,

    /// 写入缓冲区
    buffer: BytesMut,

    /// 最大缓冲区大小
    max_buffer_size: usize,

    /// 是否已发生错误
    seen_error: std::sync::atomic::AtomicBool,

    /// 是否有待同步的数据
    pending_sync: std::sync::atomic::AtomicBool,
}

impl AsyncWritableFileWriter {
    /// 创建新的 AsyncWritableFileWriter
    pub fn new(file: Box<dyn AsyncWritableFile>, max_buffer_size: Option<usize>) -> Self {
        let max_buffer_size = max_buffer_size.unwrap_or(DEFAULT_WRITE_BUFFER_SIZE);
        Self {
            file,
            file_size: AtomicU64::new(0),
            flushed_size: AtomicU64::new(0),
            buffer: BytesMut::with_capacity(cmp::min(INITIAL_BUFFER_CAPACITY, max_buffer_size)),
            max_buffer_size,
            seen_error: std::sync::atomic::AtomicBool::new(false),
            pending_sync: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// 追加数据到文件（异步）
    pub async fn append(&mut self, data: &[u8]) -> Result<(), StorageError> {
        if self.seen_error.load(Ordering::Acquire) {
            return Err(StorageError::Internal("Writer has previous error".into()));
        }

        self.pending_sync.store(true, Ordering::Release);

        let data_len = data.len();

        // 如果缓冲区空间不足，先 flush
        if self.buffer.len() + data_len > self.buffer.capacity() && !self.buffer.is_empty() {
            self.flush_internal().await?;
        }

        // 如果数据太大，直接写入（绕过缓冲区）
        if data_len > self.max_buffer_size {
            // 先 flush 缓冲区
            if !self.buffer.is_empty() {
                self.flush_internal().await?;
            }
            // 直接写入大块数据
            self.file.write(data).await?;
            self.flushed_size.store(
                self.flushed_size.load(Ordering::Acquire) + data_len as u64,
                Ordering::Release,
            );
        } else {
            // 添加到缓冲区
            self.buffer.extend_from_slice(data);
        }

        // 更新文件大小
        let cur_size = self.file_size.load(Ordering::Acquire);
        self.file_size
            .store(cur_size + data_len as u64, Ordering::Release);

        Ok(())
    }

    /// 刷新缓冲区到文件（但不保证持久化，异步）
    pub async fn flush(&mut self) -> Result<(), StorageError> {
        if self.seen_error.load(Ordering::Acquire) {
            return Err(StorageError::Internal("Writer has previous error".into()));
        }

        self.flush_internal().await
    }

    /// 内部 flush 实现
    async fn flush_internal(&mut self) -> Result<(), StorageError> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        let data = self.buffer.split().freeze();
        let data_len = data.len();

        match self.file.write(&data).await {
            Ok(()) => {
                self.flushed_size.store(
                    self.flushed_size.load(Ordering::Acquire) + data_len as u64,
                    Ordering::Release,
                );
                Ok(())
            }
            Err(e) => {
                self.seen_error.store(true, Ordering::Release);
                Err(e)
            }
        }
    }

    /// 同步数据到磁盘（保证持久化，异步）
    pub async fn sync(&mut self) -> Result<(), StorageError> {
        if self.seen_error.load(Ordering::Acquire) {
            return Err(StorageError::Internal("Writer has previous error".into()));
        }

        // 先 flush 缓冲区
        self.flush_internal().await?;

        // 然后同步到磁盘
        match self.file.sync().await {
            Ok(()) => {
                self.pending_sync.store(false, Ordering::Release);
                Ok(())
            }
            Err(e) => {
                self.seen_error.store(true, Ordering::Release);
                Err(e)
            }
        }
    }

    /// 关闭文件（异步）
    pub async fn close(mut self) -> Result<(), StorageError> {
        // 先 flush 缓冲区
        self.flush_internal().await?;

        // 然后关闭文件
        match self.file.close().await {
            Ok(()) => Ok(()),
            Err(e) => {
                self.seen_error.store(true, Ordering::Release);
                Err(e)
            }
        }
    }

    /// 获取当前文件大小（包括未 flush 的数据）
    pub fn get_file_size(&self) -> u64 {
        self.file_size.load(Ordering::Acquire)
    }

    /// 获取已 flush 的大小
    pub fn get_flushed_size(&self) -> u64 {
        self.flushed_size.load(Ordering::Acquire)
    }

    /// 检查是否已发生错误
    pub fn seen_error(&self) -> bool {
        self.seen_error.load(Ordering::Acquire)
    }

    /// 检查缓冲区是否为空
    pub fn buffer_is_empty(&self) -> bool {
        self.buffer.is_empty()
    }
}
