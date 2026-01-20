//! 基于 std::fs 的同步本地文件系统实现

use crate::error::StorageError;
use crate::sync_fs::traits::{FileSystem, ReadableFile, WritableFile};
use bytes::Bytes;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;

// 平台特定的 FileExt trait
#[cfg(unix)]
use std::os::unix::fs::FileExt;
#[cfg(windows)]
use std::os::windows::fs::FileExt;

/// 同步本地文件系统
#[derive(Clone, Copy)]
pub struct LocalFileSystem;

impl FileSystem for LocalFileSystem {
    fn open_read(&self, path: &Path) -> Result<Box<dyn ReadableFile>, StorageError> {
        let file = File::open(path)?;
        Ok(Box::new(LocalReadableFile { file }))
    }

    fn open_write(&self, path: &Path) -> Result<Box<dyn WritableFile>, StorageError> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)?;
        Ok(Box::new(LocalWritableFile { file }))
    }

    fn delete(&self, path: &Path) -> Result<(), StorageError> {
        std::fs::remove_file(path)?;
        Ok(())
    }

    fn list_dir(&self, path: &Path) -> Result<Vec<String>, StorageError> {
        let entries = std::fs::read_dir(path)?
            .map(|entry| entry.map(|e| e.file_name().to_string_lossy().to_string()))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(entries)
    }

    fn create_dir(&self, path: &Path) -> Result<(), StorageError> {
        std::fs::create_dir_all(path)?;
        Ok(())
    }

    fn exists(&self, path: &Path) -> bool {
        path.exists()
    }

    fn file_size(&self, path: &Path) -> Result<u64, StorageError> {
        let metadata = std::fs::metadata(path)?;
        Ok(metadata.len())
    }

    fn rename(&self, from: &Path, to: &Path) -> Result<(), StorageError> {
        std::fs::rename(from, to)?;
        Ok(())
    }
}

/// 本地可读文件
struct LocalReadableFile {
    file: File,
}

impl ReadableFile for LocalReadableFile {
    #[cfg(unix)]
    fn read_at(&self, offset: u64, size: usize) -> Result<Bytes, StorageError> {
        let mut buffer = vec![0u8; size];
        self.file.read_exact_at(&mut buffer, offset)?;
        Ok(Bytes::from(buffer))
    }

    #[cfg(windows)]
    fn read_at(&self, offset: u64, size: usize) -> Result<Bytes, StorageError> {
        let mut buffer = vec![0u8; size];
        let n = self.file.seek_read(&mut buffer, offset)?;
        if n != size {
            return Err(StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!("partial read: expected {} bytes, got {}", size, n),
            )));
        }
        Ok(Bytes::from(buffer))
    }

    fn read_all(&self) -> Result<Bytes, StorageError> {
        // read_all 需要从头读取整个文件
        let metadata = self.file.metadata()?;
        let file_size = metadata.len() as usize;

        if file_size == 0 {
            return Ok(Bytes::new());
        }

        self.read_at(0, file_size)
    }

    fn prefetch(&self, _offset: u64, _size: usize) -> Result<(), StorageError> {
        // 本地文件系统暂不支持预读，使用默认实现
        Ok(())
    }
}

struct LocalWritableFile {
    file: File,
}

impl WritableFile for LocalWritableFile {
    fn write(&mut self, data: &[u8]) -> Result<(), StorageError> {
        self.file.write_all(data)?;
        Ok(())
    }

    fn flush(&mut self) -> Result<(), StorageError> {
        self.file.flush()?;
        Ok(())
    }

    fn sync(&mut self) -> Result<(), StorageError> {
        self.file.sync_all()?;
        Ok(())
    }

    fn close(self: Box<Self>) -> Result<(), StorageError> {
        // File 会在 drop 时自动关闭
        Ok(())
    }

    fn get_file_size(&self) -> Result<u64, StorageError> {
        let metadata = self.file.metadata()?;
        Ok(metadata.len())
    }

    fn get_required_buffer_alignment(&self) -> usize {
        4096 // 默认 4KB 对齐（大多数文件系统）
    }
}
