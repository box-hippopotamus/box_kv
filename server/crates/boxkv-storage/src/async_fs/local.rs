//! 基于 tokio::fs 的异步本地文件系统实现

use async_trait::async_trait;
use bytes::Bytes;
use std::path::Path;
use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncWriteExt;

use crate::async_fs::traits::{AsyncFileSystem, AsyncReadableFile, AsyncWritableFile};
use crate::error::StorageError;

/// 异步本地文件系统
/// 基于 tokio::fs，用于高并发场景
#[derive(Clone, Copy)]
pub struct AsyncLocalFileSystem;

#[async_trait]
impl AsyncFileSystem for AsyncLocalFileSystem {
    async fn open_read(&self, path: &Path) -> Result<Box<dyn AsyncReadableFile>, StorageError> {
        let file = File::open(path).await?;
        Ok(Box::new(AsyncLocalReadableFile { file }))
    }

    async fn open_write(&self, path: &Path) -> Result<Box<dyn AsyncWritableFile>, StorageError> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)
            .await?;
        Ok(Box::new(AsyncLocalWritableFile { file }))
    }

    async fn delete(&self, path: &Path) -> Result<(), StorageError> {
        tokio::fs::remove_file(path).await?;
        Ok(())
    }

    async fn list_dir(&self, path: &Path) -> Result<Vec<String>, StorageError> {
        let mut entries = Vec::new();
        let mut read_dir = tokio::fs::read_dir(path).await?;

        while let Some(entry) = read_dir.next_entry().await? {
            entries.push(entry.file_name().to_string_lossy().to_string());
        }

        Ok(entries)
    }

    async fn create_dir(&self, path: &Path) -> Result<(), StorageError> {
        tokio::fs::create_dir_all(path).await?;
        Ok(())
    }

    async fn exists(&self, path: &Path) -> bool {
        tokio::fs::metadata(path).await.is_ok()
    }

    async fn file_size(&self, path: &Path) -> Result<u64, StorageError> {
        let metadata = tokio::fs::metadata(path).await?;
        Ok(metadata.len())
    }

    async fn rename(&self, from: &Path, to: &Path) -> Result<(), StorageError> {
        tokio::fs::rename(from, to).await?;
        Ok(())
    }
}

/// 异步本地可读文件
struct AsyncLocalReadableFile {
    file: File,
}

#[async_trait]
impl AsyncReadableFile for AsyncLocalReadableFile {
    async fn read_at(&self, offset: u64, size: usize) -> Result<Bytes, StorageError> {
        let mut buffer = vec![0u8; size];

        let file_clone = self.file.try_clone().await?;
        let std_file = file_clone.into_std().await;

        let result = tokio::task::spawn_blocking(move || {
            #[cfg(unix)]
            {
                use std::os::unix::fs::FileExt;
                // Unix: 使用 pread() 系统调用
                std_file.read_exact_at(&mut buffer, offset)?;
            }
            #[cfg(windows)]
            {
                use std::os::windows::fs::FileExt;
                // Windows: 使用 OVERLAPPED I/O
                std_file.seek_read(&mut buffer, offset)?;
            }

            Ok::<_, std::io::Error>(buffer)
        })
        .await
        .map_err(|e| StorageError::Internal(format!("Task join error: {}", e)))??;

        Ok(Bytes::from(result))
    }

    async fn read_all(&self) -> Result<Bytes, StorageError> {
        // read_all 需要获取文件大小
        let metadata = self.file.metadata().await?;
        let file_size = metadata.len() as usize;

        if file_size == 0 {
            return Ok(Bytes::new());
        }

        self.read_at(0, file_size).await
    }

    async fn prefetch(&self, _offset: u64, _size: usize) -> Result<(), StorageError> {
        // 本地文件系统暂不支持预读，使用默认实现
        Ok(())
    }
}

struct AsyncLocalWritableFile {
    file: File,
}

#[async_trait]
impl AsyncWritableFile for AsyncLocalWritableFile {
    async fn write(&mut self, data: &[u8]) -> Result<(), StorageError> {
        self.file.write_all(data).await?;
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), StorageError> {
        self.file.flush().await?;
        Ok(())
    }

    async fn sync(&mut self) -> Result<(), StorageError> {
        self.file.sync_all().await?;
        Ok(())
    }

    async fn close(self: Box<Self>) -> Result<(), StorageError> {
        Ok(())
    }

    async fn get_file_size(&self) -> Result<u64, StorageError> {
        let metadata = self.file.metadata().await?;
        Ok(metadata.len())
    }

    fn get_required_buffer_alignment(&self) -> usize {
        4096
    }
}
