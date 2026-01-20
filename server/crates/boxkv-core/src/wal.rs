mod reader;
mod writer;

use crate::wal::reader::{ReadError, WalIterator};
use crate::wal::writer::{WalWriter, WriteError};

use std::path::{Path, PathBuf};

use bytes::Bytes;
use thiserror::Error;
use tracing::{debug, info, warn};

use boxkv_common::types::Entry;
use boxkv_storage::{FileSystem, StorageError};

#[derive(Debug, Error)]
pub enum WalError {
    #[error("Read error at {path}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: ReadError,
    },

    #[error("Write error at {path}: {source}")]
    Write {
        path: PathBuf,
        #[source]
        source: WriteError,
    },
}
trait WalContext<T, E> {
    fn with_context(self, path: &Path) -> Result<T, WalError>;
}

impl<T> WalContext<T, ReadError> for Result<T, ReadError> {
    fn with_context(self, path: &Path) -> Result<T, WalError> {
        self.map_err(|e| WalError::Read {
            path: path.to_path_buf(),
            source: e,
        })
    }
}

impl<T> WalContext<T, WriteError> for Result<T, WriteError> {
    fn with_context(self, path: &Path) -> Result<T, WalError> {
        self.map_err(|e| WalError::Write {
            path: path.to_path_buf(),
            source: e,
        })
    }
}

impl<T> WalContext<T, StorageError> for Result<T, StorageError> {
    fn with_context(self, path: &Path) -> Result<T, WalError> {
        self.map_err(|e| WalError::Read {
            path: path.to_path_buf(),
            source: ReadError::Storage(e),
        })
    }
}

/// WAL 二进制格式规范
///
/// ## Header（固定 21 字节）
/// ```text
/// +----------+----------------+--------------+----------------+
/// | CRC (4B) | PayloadLen (8B)| ValueTag(1B) | Seq (8B)       |
/// +----------+----------------+--------------+----------------+
/// ```
///
/// ## Payload（变长）
/// ```text
/// +-------------+----------+----------------------+
/// | KeyLen (8B) | Key Data | Value Section        |
/// +-------------+----------+----------------------+
/// ```
///
/// ## Value Section（根据 ValueTag 决定格式）
///
/// **[ValueTag = 0] Normal（普通值）：**
/// ```text
/// +------------+
/// | Value Data |
/// +------------+
/// ```
///
/// **[ValueTag = 1] Tombstone（删除标记）：**
/// ```text
/// (空，无数据)
/// ```
///
/// **[ValueTag = 2] Expiring（带过期时间）：**
/// ```text
/// +-------------+------------+
/// | ExpireAt(8B)| Value Data |
/// +-------------+------------+
/// ```
///
/// ## CRC32 校验范围
/// 校验和覆盖除自身外的所有字段：
/// - PayloadLen (8 字节)
/// - ValueTag (1 字节)
/// - Seq (8 字节)
/// - KeyLen (8 字节)
/// - Key Data (变长)
/// - Value Section (变长)
const WAL_CRC_SIZE: usize = 4;
const WAL_PAYLOAD_LEN_SIZE: usize = 8;
const WAL_TYPE_SIZE: usize = 1;
const WAL_SEQ_SIZE: usize = 8;
const WAL_HEADER_SIZE: usize = WAL_CRC_SIZE + WAL_PAYLOAD_LEN_SIZE + WAL_TYPE_SIZE + WAL_SEQ_SIZE;

const WAL_KEY_LEN_SIZE: usize = 8;
const WAL_EXPIRE_LEN_SIZE: usize = 8;

/// WAL 管理器
pub struct Wal<FS: FileSystem> {
    writer: WalWriter<FS>,
    path: PathBuf,
}

impl<FS: FileSystem> Wal<FS> {
    /// 创建新的 WAL 文件（文件名格式：{:09}.wal）
    pub fn create(fs: &FS, dir: PathBuf, file_id: u64) -> Result<Self, WalError> {
        let path = dir.join(format!("{:09}.wal", file_id));

        info!(file_id, ?path, "Creating WAL file");

        Ok(Self {
            writer: WalWriter::new(fs, path.clone()).with_context(&path)?,
            path,
        })
    }

    /// 从目录中恢复所有 WAL 条目
    pub fn read_all_entries(
        fs: &FS,
        dir: PathBuf,
        min_sequence: u64,
    ) -> Result<(Vec<Entry>, u64), WalError> {
        info!(min_sequence, ?dir, "Starting WAL recovery");
        let start = std::time::Instant::now();

        let file_list = fs.list_dir(&dir).with_context(&dir)?;

        let mut wal_files: Vec<(u64, PathBuf)> = Vec::new();

        // 扫描目录中的 WAL 文件
        for filename in file_list {
            let path = dir.join(&filename);

            if !filename.ends_with(".wal") {
                continue;
            }

            // 解析文件 ID
            if let Some(stem) = filename.strip_suffix(".wal")
                && let Ok(id) = stem.parse::<u64>()
            {
                wal_files.push((id, path));
            }
        }

        // 按 ID 排序确保时间顺序
        wal_files.sort_unstable_by_key(|&(id, _)| id);

        debug!(file_count = wal_files.len(), "Scanned WAL files");

        let mut max_seq = u64::MIN;
        let mut all_entries = Vec::new();

        // 遍历文件并读取记录
        for (file_id, path) in &wal_files {
            // 检查文件大小
            let file_size = fs.file_size(path).with_context(path)?;
            debug!(
                file_id,
                ?path,
                file_size,
                "WAL Recovery: Opening file for reading"
            );

            if file_size == 0 {
                warn!(file_id, ?path, "WAL Recovery: File is empty, skipping");
                continue;
            }

            let file = fs.open_read(path).with_context(path)?;
            let read_it = WalIterator::new(file);

            let mut entry_count = 0;
            for res in read_it {
                match res {
                    Ok(entry) => {
                        if entry.sequence >= min_sequence {
                            max_seq = max_seq.max(entry.sequence);
                            all_entries.push(entry);
                            entry_count += 1;
                        }
                    }
                    Err(ReadError::Io(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                        warn!(
                            file_id,
                            ?path,
                            file_size,
                            entry_count,
                            error = ?e,
                            "WAL file truncated, skipping partial record"
                        );
                        break;
                    }
                    Err(e) => {
                        warn!(
                            file_id,
                            ?path,
                            file_size,
                            entry_count,
                            error = ?e,
                            "WAL Recovery: Error reading entry"
                        );
                        return Err(WalError::Read {
                            path: path.clone(),
                            source: e,
                        });
                    }
                }
            }

            debug!(file_id, entry_count, ?path, "Completed reading WAL file");
        }

        // 最终按 sequence 排序
        all_entries.sort_by_key(|r| r.sequence);

        let elapsed = start.elapsed();
        info!(
            record_count = all_entries.len(),
            max_seq,
            elapsed_ms = elapsed.as_millis(),
            "WAL recovery completed"
        );

        Ok((all_entries, max_seq))
    }

    /// 追加普通值
    pub fn append_normal(
        &mut self,
        sequence: u64,
        key: Bytes,
        value: Bytes,
    ) -> Result<(), WalError> {
        self.writer
            .append(&Entry::new_normal(key, value, sequence))
            .with_context(&self.path)?;

        Ok(())
    }

    /// 批量追加条目
    pub fn append_batch(&mut self, entries: &[Entry]) -> Result<(), WalError> {
        self.writer.append_batch(entries).with_context(&self.path)
    }

    /// 追加删除标记
    pub fn append_tombstone(&mut self, sequence: u64, key: Bytes) -> Result<(), WalError> {
        self.writer
            .append(&Entry::new_tombstone(key, sequence))
            .with_context(&self.path)?;

        Ok(())
    }

    /// 追加带过期时间的值
    pub fn append_expire(
        &mut self,
        sequence: u64,
        key: Bytes,
        value: Bytes,
        expire_at: u64,
    ) -> Result<(), WalError> {
        self.writer
            .append(&Entry::new_expiring(key, value, sequence, expire_at))
            .with_context(&self.path)?;

        Ok(())
    }

    /// 删除 WAL 文件（通常在 Flush 到 SSTable 后调用）
    pub fn delete(fs: &FS, dir: PathBuf, file_id: u64) -> Result<(), WalError> {
        let path = dir.join(format!("{:09}.wal", file_id));

        info!(file_id, ?path, "Deleting WAL file");

        fs.delete(&path).with_context(&path)
    }

    /// 同步到磁盘（fsync）
    pub fn sync(&mut self) -> Result<(), WalError> {
        debug!(?self.path, "Syncing WAL to disk");

        self.writer.sync().with_context(&self.path)?;
        Ok(())
    }

    /// 切换到新的 WAL 文件
    pub fn rotate(
        &mut self,
        fs: &FS,
        dir: &std::path::Path,
        new_file_id: u64,
    ) -> Result<(), WalError> {
        self.sync()?;

        let new_path = dir.join(format!("{:09}.wal", new_file_id));
        self.writer = WalWriter::new(fs, new_path.clone()).with_context(&new_path)?;
        self.path = new_path;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use boxkv_common::types::ValueType;
    use boxkv_storage::LocalFileSystem;
    use tempfile::TempDir;

    #[test]
    fn test_wal_create_and_file_naming() {
        let temp_dir = TempDir::new().unwrap();
        let dir_path = temp_dir.path().to_path_buf();
        let fs = LocalFileSystem;

        let _wal = Wal::create(&fs, dir_path.clone(), 1).unwrap();
        assert!(fs.exists(&dir_path.join("000000001.wal")));

        let _wal2 = Wal::create(&fs, dir_path.clone(), 42).unwrap();
        assert!(fs.exists(&dir_path.join("000000042.wal")));

        let _wal3 = Wal::create(&fs, dir_path.clone(), 123456789).unwrap();
        assert!(fs.exists(&dir_path.join("123456789.wal")));
    }

    #[test]
    fn test_wal_append_normal_value() {
        let temp_dir = TempDir::new().unwrap();
        let dir_path = temp_dir.path().to_path_buf();
        let fs = LocalFileSystem;

        {
            let mut wal = Wal::create(&fs, dir_path.clone(), 1).unwrap();
            wal.append_normal(100, Bytes::from("key1"), Bytes::from("value1"))
                .unwrap();
            wal.append_normal(101, Bytes::from("key2"), Bytes::from("value2"))
                .unwrap();
            wal.sync().unwrap();
        }

        let (entries, max_seq) = Wal::read_all_entries(&fs, dir_path, 0).unwrap();
        assert_eq!(max_seq, 101);
        assert_eq!(entries.len(), 2);

        assert_eq!(entries[0].sequence, 100);
        assert_eq!(entries[0].key.as_ref(), b"key1");
        match &entries[0].value {
            ValueType::Normal(data) => assert_eq!(data.as_ref(), b"value1"),
            _ => panic!("Expected Normal value"),
        }

        assert_eq!(entries[1].sequence, 101);
        assert_eq!(entries[1].key.as_ref(), b"key2");
        match &entries[1].value {
            ValueType::Normal(data) => assert_eq!(data.as_ref(), b"value2"),
            _ => panic!("Expected Normal value"),
        }
    }

    #[test]
    fn test_wal_append_tombstone() {
        let temp_dir = TempDir::new().unwrap();
        let dir_path = temp_dir.path().to_path_buf();
        let fs = LocalFileSystem;

        {
            let mut wal = Wal::create(&fs, dir_path.clone(), 1).unwrap();
            wal.append_tombstone(200, Bytes::from("deleted_key"))
                .unwrap();
            wal.sync().unwrap();
        }

        let (entries, max_seq) = Wal::read_all_entries(&fs, dir_path, 0).unwrap();
        assert_eq!(max_seq, 200);
        assert_eq!(entries.len(), 1);

        assert_eq!(entries[0].sequence, 200);
        assert_eq!(entries[0].key.as_ref(), b"deleted_key");
        assert!(entries[0].is_tombstone());
        assert!(matches!(entries[0].value, ValueType::Tombstone));
    }

    #[test]
    fn test_wal_append_expiring_value() {
        let temp_dir = TempDir::new().unwrap();
        let dir_path = temp_dir.path().to_path_buf();
        let fs = LocalFileSystem;

        let expire_at = 1234567890u64;

        {
            let mut wal = Wal::create(&fs, dir_path.clone(), 1).unwrap();
            wal.append_expire(
                300,
                Bytes::from("expire_key"),
                Bytes::from("expire_value"),
                expire_at,
            )
            .unwrap();
            wal.sync().unwrap();
        }

        let (entries, max_seq) = Wal::read_all_entries(&fs, dir_path, 0).unwrap();
        assert_eq!(max_seq, 300);
        assert_eq!(entries.len(), 1);

        assert_eq!(entries[0].sequence, 300);
        assert_eq!(entries[0].key.as_ref(), b"expire_key");
        match &entries[0].value {
            ValueType::Expiring {
                data,
                expire_at: exp,
            } => {
                assert_eq!(data.as_ref(), b"expire_value");
                assert_eq!(*exp, expire_at);
            }
            _ => panic!("Expected Expiring value"),
        }
    }

    #[test]
    fn test_wal_mixed_value_types() {
        let temp_dir = TempDir::new().unwrap();
        let dir_path = temp_dir.path().to_path_buf();
        let fs = LocalFileSystem;

        {
            let mut wal = Wal::create(&fs, dir_path.clone(), 1).unwrap();
            wal.append_normal(1, Bytes::from("k1"), Bytes::from("v1"))
                .unwrap();
            wal.append_tombstone(2, Bytes::from("k2")).unwrap();
            wal.append_expire(3, Bytes::from("k3"), Bytes::from("v3"), 9999)
                .unwrap();
            wal.append_normal(4, Bytes::from("k4"), Bytes::from("v4"))
                .unwrap();
            wal.sync().unwrap();
        }

        let (entries, max_seq) = Wal::read_all_entries(&fs, dir_path, 0).unwrap();
        assert_eq!(max_seq, 4);
        assert_eq!(entries.len(), 4);

        assert!(matches!(entries[0].value, ValueType::Normal(_)));
        assert!(matches!(entries[1].value, ValueType::Tombstone));
        assert!(matches!(entries[2].value, ValueType::Expiring { .. }));
        assert!(matches!(entries[3].value, ValueType::Normal(_)));
    }

    #[test]
    fn test_wal_multiple_files_recovery() {
        let temp_dir = TempDir::new().unwrap();
        let dir_path = temp_dir.path().to_path_buf();
        let fs = LocalFileSystem;

        {
            let mut wal1 = Wal::create(&fs, dir_path.clone(), 1).unwrap();
            wal1.append_normal(10, Bytes::from("k1"), Bytes::from("v1"))
                .unwrap();
            wal1.append_normal(20, Bytes::from("k2"), Bytes::from("v2"))
                .unwrap();
            wal1.sync().unwrap();
        }

        {
            let mut wal2 = Wal::create(&fs, dir_path.clone(), 2).unwrap();
            wal2.append_normal(30, Bytes::from("k3"), Bytes::from("v3"))
                .unwrap();
            wal2.append_tombstone(40, Bytes::from("k1")).unwrap();
            wal2.sync().unwrap();
        }

        {
            let mut wal3 = Wal::create(&fs, dir_path.clone(), 3).unwrap();
            wal3.append_expire(50, Bytes::from("k4"), Bytes::from("v4"), 8888)
                .unwrap();
            wal3.sync().unwrap();
        }

        let (entries, max_seq) = Wal::read_all_entries(&fs, dir_path.clone(), 0).unwrap();
        assert_eq!(max_seq, 50);
        assert_eq!(entries.len(), 5);

        assert_eq!(entries[0].sequence, 10);
        assert_eq!(entries[1].sequence, 20);
        assert_eq!(entries[2].sequence, 30);
        assert_eq!(entries[3].sequence, 40);
        assert_eq!(entries[4].sequence, 50);
    }

    #[test]
    fn test_wal_min_sequence_filtering() {
        let temp_dir = TempDir::new().unwrap();
        let dir_path = temp_dir.path().to_path_buf();
        let fs = LocalFileSystem;

        {
            let mut wal = Wal::create(&fs, dir_path.clone(), 1).unwrap();
            wal.append_normal(10, Bytes::from("k1"), Bytes::from("v1"))
                .unwrap();
            wal.append_normal(20, Bytes::from("k2"), Bytes::from("v2"))
                .unwrap();
            wal.append_normal(30, Bytes::from("k3"), Bytes::from("v3"))
                .unwrap();
            wal.append_normal(40, Bytes::from("k4"), Bytes::from("v4"))
                .unwrap();
            wal.sync().unwrap();
        }

        let (entries, max_seq) = Wal::read_all_entries(&fs, dir_path.clone(), 25).unwrap();
        assert_eq!(max_seq, 40);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].sequence, 30);
        assert_eq!(entries[1].sequence, 40);

        let (entries, max_seq) = Wal::read_all_entries(&fs, dir_path.clone(), 40).unwrap();
        assert_eq!(max_seq, 40);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].sequence, 40);

        let (entries, _) = Wal::read_all_entries(&fs, dir_path, 100).unwrap();
        assert_eq!(entries.len(), 0);
    }

    #[test]
    fn test_wal_delete_file() {
        let temp_dir = TempDir::new().unwrap();
        let dir_path = temp_dir.path().to_path_buf();
        let fs = LocalFileSystem;

        let _wal = Wal::create(&fs, dir_path.clone(), 1).unwrap();
        let file_path = dir_path.join("000000001.wal");
        assert!(fs.exists(&file_path));

        Wal::delete(&fs, dir_path.clone(), 1).unwrap();
        assert!(!fs.exists(&file_path));

        // Deleting non-existent file should return error
        assert!(Wal::delete(&fs, dir_path, 999).is_err());
    }

    #[test]
    fn test_wal_empty_recovery() {
        let temp_dir = TempDir::new().unwrap();
        let dir_path = temp_dir.path().to_path_buf();
        let fs = LocalFileSystem;

        let (entries, max_seq) = Wal::read_all_entries(&fs, dir_path, 0).unwrap();
        assert_eq!(entries.len(), 0);
        assert_eq!(max_seq, u64::MIN);
    }

    #[test]
    fn test_wal_large_values() {
        let temp_dir = TempDir::new().unwrap();
        let dir_path = temp_dir.path().to_path_buf();
        let fs = LocalFileSystem;

        let large_key = vec![b'k'; 1024];
        let large_value = vec![b'v'; 1024 * 1024];

        {
            let mut wal = Wal::create(&fs, dir_path.clone(), 1).unwrap();
            wal.append_normal(
                1,
                Bytes::from(large_key.clone()),
                Bytes::from(large_value.clone()),
            )
            .unwrap();
            wal.sync().unwrap();
        }

        let (entries, _) = Wal::read_all_entries(&fs, dir_path, 0).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].key.len(), 1024);
        match &entries[0].value {
            ValueType::Normal(data) => assert_eq!(data.len(), 1024 * 1024),
            _ => panic!("Expected Normal value"),
        }
    }

    #[test]
    fn test_wal_empty_key_and_value() {
        let temp_dir = TempDir::new().unwrap();
        let dir_path = temp_dir.path().to_path_buf();
        let fs = LocalFileSystem;

        {
            let mut wal = Wal::create(&fs, dir_path.clone(), 1).unwrap();
            wal.append_normal(1, Bytes::from(""), Bytes::from(""))
                .unwrap();
            wal.append_tombstone(2, Bytes::from("")).unwrap();
            wal.sync().unwrap();
        }

        let (entries, _) = Wal::read_all_entries(&fs, dir_path, 0).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].key.len(), 0);
        assert_eq!(entries[1].key.len(), 0);
    }

    #[test]
    fn test_wal_binary_key_and_value() {
        let temp_dir = TempDir::new().unwrap();
        let dir_path = temp_dir.path().to_path_buf();
        let fs = LocalFileSystem;

        let binary_key: Vec<u8> = (0..=255).collect();
        let binary_value: Vec<u8> = (0..=255).rev().collect();

        {
            let mut wal = Wal::create(&fs, dir_path.clone(), 1).unwrap();
            wal.append_normal(
                1,
                Bytes::from(binary_key.clone()),
                Bytes::from(binary_value.clone()),
            )
            .unwrap();
            wal.sync().unwrap();
        }

        let (entries, _) = Wal::read_all_entries(&fs, dir_path, 0).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].key.as_ref(), binary_key.as_slice());
        match &entries[0].value {
            ValueType::Normal(data) => assert_eq!(data.as_ref(), binary_value.as_slice()),
            _ => panic!("Expected Normal value"),
        }
    }

    #[test]
    fn test_wal_sequence_number_ordering() {
        let temp_dir = TempDir::new().unwrap();
        let dir_path = temp_dir.path().to_path_buf();
        let fs = LocalFileSystem;

        {
            let mut wal = Wal::create(&fs, dir_path.clone(), 1).unwrap();
            wal.append_normal(100, Bytes::from("k100"), Bytes::from("v100"))
                .unwrap();
            wal.append_normal(50, Bytes::from("k50"), Bytes::from("v50"))
                .unwrap();
            wal.append_normal(200, Bytes::from("k200"), Bytes::from("v200"))
                .unwrap();
            wal.append_normal(75, Bytes::from("k75"), Bytes::from("v75"))
                .unwrap();
            wal.sync().unwrap();
        }

        let (entries, max_seq) = Wal::read_all_entries(&fs, dir_path, 0).unwrap();
        assert_eq!(max_seq, 200);
        assert_eq!(entries.len(), 4);

        assert_eq!(entries[0].sequence, 50);
        assert_eq!(entries[1].sequence, 75);
        assert_eq!(entries[2].sequence, 100);
        assert_eq!(entries[3].sequence, 200);
    }

    #[test]
    fn test_wal_sync_durability() {
        let temp_dir = TempDir::new().unwrap();
        let dir_path = temp_dir.path().to_path_buf();
        let fs = LocalFileSystem;

        {
            let mut wal = Wal::create(&fs, dir_path.clone(), 1).unwrap();
            wal.append_normal(1, Bytes::from("k1"), Bytes::from("v1"))
                .unwrap();
            wal.sync().unwrap();
        }
        let (entries, _) = Wal::read_all_entries(&fs, dir_path, 0).unwrap();
        assert_eq!(entries.len(), 1);
    }

    #[test]
    fn test_wal_concurrent_writes() {
        use std::sync::Arc;
        use std::thread;

        let temp_dir = TempDir::new().unwrap();
        let dir_path = temp_dir.path().to_path_buf();
        let fs = Arc::new(LocalFileSystem);

        let mut handles = vec![];
        let num_threads = 10;
        let entries_per_thread = 100;

        for thread_id in 0..num_threads {
            let fs = Arc::clone(&fs);
            let dir_path = dir_path.clone();
            let handle = thread::spawn(move || {
                let mut wal = Wal::create(&*fs, dir_path.clone(), thread_id as u64).unwrap();
                for i in 0..entries_per_thread {
                    let sequence = (thread_id * entries_per_thread + i) as u64;
                    let key = format!("thread_{}_key_{}", thread_id, i);
                    let value = format!("thread_{}_value_{}", thread_id, i);
                    wal.append_normal(sequence, Bytes::from(key), Bytes::from(value))
                        .unwrap();
                }
                wal.sync().unwrap();
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let (entries, max_seq) = Wal::read_all_entries(&*fs, dir_path, 0).unwrap();
        assert_eq!(entries.len(), num_threads * entries_per_thread);
        assert_eq!(max_seq, (num_threads * entries_per_thread - 1) as u64);

        for i in 1..entries.len() {
            assert!(entries[i].sequence >= entries[i - 1].sequence);
        }
    }

    #[test]
    fn test_wal_high_volume_writes() {
        let temp_dir = TempDir::new().unwrap();
        let dir_path = temp_dir.path().to_path_buf();
        let fs = LocalFileSystem;

        let num_entries: usize = 10000;

        {
            let mut wal = Wal::create(&fs, dir_path.clone(), 1).unwrap();
            for i in 0..num_entries {
                let key = format!("key_{:05}", i);
                let value = format!("value_{:05}", i);
                wal.append_normal(i as u64, Bytes::from(key), Bytes::from(value))
                    .unwrap();
            }
            wal.sync().unwrap();
        }

        let (entries, max_seq) = Wal::read_all_entries(&fs, dir_path, 0).unwrap();
        assert_eq!(entries.len(), num_entries);
        assert_eq!(max_seq, (num_entries - 1) as u64);

        for (i, entry) in entries.iter().enumerate() {
            assert_eq!(entry.sequence, i as u64);
            assert_eq!(entry.key, format!("key_{:05}", i));
            match &entry.value {
                ValueType::Normal(data) => {
                    assert_eq!(data.as_ref(), format!("value_{:05}", i).as_bytes());
                }
                _ => panic!("Expected Normal value"),
            }
        }
    }

    #[test]
    fn test_wal_mixed_operations_sequence() {
        let temp_dir = TempDir::new().unwrap();
        let dir_path = temp_dir.path().to_path_buf();
        let fs = LocalFileSystem;

        {
            let mut wal = Wal::create(&fs, dir_path.clone(), 1).unwrap();
            wal.append_normal(1, Bytes::from("user:1"), Bytes::from("Alice"))
                .unwrap();
            wal.append_normal(2, Bytes::from("user:2"), Bytes::from("Bob"))
                .unwrap();
            wal.append_normal(3, Bytes::from("user:3"), Bytes::from("Charlie"))
                .unwrap();
            wal.append_tombstone(4, Bytes::from("user:2")).unwrap();
            wal.append_normal(5, Bytes::from("user:2"), Bytes::from("BobV2"))
                .unwrap();
            wal.append_expire(
                6,
                Bytes::from("session:abc"),
                Bytes::from("token123"),
                1234567890,
            )
            .unwrap();
            wal.sync().unwrap();
        }

        let (entries, max_seq) = Wal::read_all_entries(&fs, dir_path, 0).unwrap();
        assert_eq!(entries.len(), 6);
        assert_eq!(max_seq, 6);

        assert_eq!(entries[0].sequence, 1);
        assert_eq!(entries[0].key.as_ref(), b"user:1");
        assert_eq!(entries[1].sequence, 2);
        assert_eq!(entries[2].sequence, 3);
        assert_eq!(entries[3].sequence, 4);
        assert!(entries[3].is_tombstone());
        assert_eq!(entries[4].sequence, 5);
        assert_eq!(entries[5].sequence, 6);
        assert!(matches!(entries[5].value, ValueType::Expiring { .. }));
    }

    #[test]
    fn test_wal_file_id_ordering() {
        let temp_dir = TempDir::new().unwrap();
        let dir_path = temp_dir.path().to_path_buf();
        let fs = LocalFileSystem;

        {
            let mut wal3 = Wal::create(&fs, dir_path.clone(), 3).unwrap();
            wal3.append_normal(30, Bytes::from("k3"), Bytes::from("v3"))
                .unwrap();
            wal3.sync().unwrap();
        }

        {
            let mut wal1 = Wal::create(&fs, dir_path.clone(), 1).unwrap();
            wal1.append_normal(10, Bytes::from("k1"), Bytes::from("v1"))
                .unwrap();
            wal1.sync().unwrap();
        }

        {
            let mut wal2 = Wal::create(&fs, dir_path.clone(), 2).unwrap();
            wal2.append_normal(20, Bytes::from("k2"), Bytes::from("v2"))
                .unwrap();
            wal2.sync().unwrap();
        }

        let (entries, _) = Wal::read_all_entries(&fs, dir_path, 0).unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].sequence, 10);
        assert_eq!(entries[1].sequence, 20);
        assert_eq!(entries[2].sequence, 30);
    }

    #[test]
    fn test_wal_max_sequence_tracking() {
        let temp_dir = TempDir::new().unwrap();
        let dir_path = temp_dir.path().to_path_buf();
        let fs = LocalFileSystem;

        {
            let mut wal = Wal::create(&fs, dir_path.clone(), 1).unwrap();
            wal.append_normal(1000, Bytes::from("k1"), Bytes::from("v1"))
                .unwrap();
            wal.append_normal(5000, Bytes::from("k2"), Bytes::from("v2"))
                .unwrap();
            wal.append_normal(2000, Bytes::from("k3"), Bytes::from("v3"))
                .unwrap();
            wal.sync().unwrap();
        }

        let (_, max_seq) = Wal::read_all_entries(&fs, dir_path, 0).unwrap();
        assert_eq!(max_seq, 5000);
    }

    #[test]
    fn test_wal_unicode_keys_and_values() {
        let temp_dir = TempDir::new().unwrap();
        let dir_path = temp_dir.path().to_path_buf();
        let fs = LocalFileSystem;

        let unicode_key = "用户:123";
        let unicode_value = "值:测试数据🎉";

        {
            let mut wal = Wal::create(&fs, dir_path.clone(), 1).unwrap();
            wal.append_normal(1, Bytes::from(unicode_key), Bytes::from(unicode_value))
                .unwrap();
            wal.sync().unwrap();
        }

        let (entries, _) = Wal::read_all_entries(&fs, dir_path, 0).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].key.as_ref(), unicode_key.as_bytes());
        match &entries[0].value {
            ValueType::Normal(data) => {
                assert_eq!(data.as_ref(), unicode_value.as_bytes());
            }
            _ => panic!("Expected Normal value"),
        }
    }

    #[test]
    fn test_wal_repeated_keys() {
        let temp_dir = TempDir::new().unwrap();
        let dir_path = temp_dir.path().to_path_buf();
        let fs = LocalFileSystem;

        {
            let mut wal = Wal::create(&fs, dir_path.clone(), 1).unwrap();
            wal.append_normal(1, Bytes::from("key"), Bytes::from("v1"))
                .unwrap();
            wal.append_normal(2, Bytes::from("key"), Bytes::from("v2"))
                .unwrap();
            wal.append_normal(3, Bytes::from("key"), Bytes::from("v3"))
                .unwrap();
            wal.sync().unwrap();
        }

        let (entries, _) = Wal::read_all_entries(&fs, dir_path, 0).unwrap();
        assert_eq!(entries.len(), 3);
        for entry in &entries {
            assert_eq!(entry.key.as_ref(), b"key");
        }
        assert_eq!(entries[0].sequence, 1);
        assert_eq!(entries[1].sequence, 2);
        assert_eq!(entries[2].sequence, 3);
    }

    #[test]
    fn test_wal_expiring_value_recovery() {
        let temp_dir = TempDir::new().unwrap();
        let dir_path = temp_dir.path().to_path_buf();
        let fs = LocalFileSystem;

        let expire_at = 1234567890u64;

        {
            let mut wal = Wal::create(&fs, dir_path.clone(), 1).unwrap();
            wal.append_expire(1, Bytes::from("key"), Bytes::from("value"), expire_at)
                .unwrap();
            wal.sync().unwrap();
        }

        let (entries, _) = Wal::read_all_entries(&fs, dir_path, 0).unwrap();
        assert_eq!(entries.len(), 1);
        match &entries[0].value {
            ValueType::Expiring {
                data,
                expire_at: exp,
            } => {
                assert_eq!(data.as_ref(), b"value");
                assert_eq!(*exp, expire_at);
            }
            _ => panic!("Expected Expiring value"),
        }
    }

    #[test]
    fn test_wal_performance_benchmark() {
        let temp_dir = TempDir::new().unwrap();
        let dir_path = temp_dir.path().to_path_buf();
        let fs = LocalFileSystem;

        let num_entries: usize = 1000;
        let start = std::time::Instant::now();

        {
            let mut wal = Wal::create(&fs, dir_path.clone(), 1).unwrap();
            for i in 0..num_entries {
                let key = format!("key_{}", i);
                let value = format!("value_{}", i);
                wal.append_normal(i as u64, Bytes::from(key), Bytes::from(value))
                    .unwrap();
            }
            wal.sync().unwrap();
        }

        let write_duration = start.elapsed();

        let read_start = std::time::Instant::now();
        let (entries, _) = Wal::read_all_entries(&fs, dir_path, 0).unwrap();
        let read_duration = read_start.elapsed();

        assert_eq!(entries.len(), num_entries);

        println!(
            "WAL Performance: {} entries - Write: {:?}, Read: {:?}",
            num_entries, write_duration, read_duration
        );

        assert!(write_duration.as_millis() < 10000);
        assert!(read_duration.as_millis() < 5000);
    }

    #[test]
    fn test_wal_zero_sequence() {
        let temp_dir = TempDir::new().unwrap();
        let dir_path = temp_dir.path().to_path_buf();
        let fs = LocalFileSystem;

        {
            let mut wal = Wal::create(&fs, dir_path.clone(), 1).unwrap();
            wal.append_normal(0, Bytes::from("k0"), Bytes::from("v0"))
                .unwrap();
            wal.sync().unwrap();
        }

        let (entries, max_seq) = Wal::read_all_entries(&fs, dir_path, 0).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(max_seq, 0);
        assert_eq!(entries[0].sequence, 0);
    }

    #[test]
    fn test_wal_max_u64_sequence() {
        let temp_dir = TempDir::new().unwrap();
        let dir_path = temp_dir.path().to_path_buf();
        let fs = LocalFileSystem;

        let max_seq = u64::MAX;

        {
            let mut wal = Wal::create(&fs, dir_path.clone(), 1).unwrap();
            wal.append_normal(max_seq, Bytes::from("k"), Bytes::from("v"))
                .unwrap();
            wal.sync().unwrap();
        }

        let (entries, recovered_max) = Wal::read_all_entries(&fs, dir_path, 0).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(recovered_max, max_seq);
        assert_eq!(entries[0].sequence, max_seq);
    }

    #[test]
    fn test_wal_filtering_edge_cases() {
        let temp_dir = TempDir::new().unwrap();
        let dir_path = temp_dir.path().to_path_buf();
        let fs = LocalFileSystem;

        {
            let mut wal = Wal::create(&fs, dir_path.clone(), 1).unwrap();
            wal.append_normal(10, Bytes::from("k1"), Bytes::from("v1"))
                .unwrap();
            wal.append_normal(20, Bytes::from("k2"), Bytes::from("v2"))
                .unwrap();
            wal.sync().unwrap();
        }

        let (entries, _) = Wal::read_all_entries(&fs, dir_path.clone(), 10).unwrap();
        assert_eq!(entries.len(), 2);

        let (entries, _) = Wal::read_all_entries(&fs, dir_path.clone(), 20).unwrap();
        assert_eq!(entries.len(), 1);

        let (entries, _) = Wal::read_all_entries(&fs, dir_path, 21).unwrap();
        assert_eq!(entries.len(), 0);
    }

    #[test]
    fn test_wal_all_value_types_in_sequence() {
        let temp_dir = TempDir::new().unwrap();
        let dir_path = temp_dir.path().to_path_buf();
        let fs = LocalFileSystem;

        {
            let mut wal = Wal::create(&fs, dir_path.clone(), 1).unwrap();
            wal.append_normal(1, Bytes::from("normal"), Bytes::from("data"))
                .unwrap();
            wal.append_tombstone(2, Bytes::from("tombstone")).unwrap();
            wal.append_expire(3, Bytes::from("expiring"), Bytes::from("data"), 9999)
                .unwrap();
            wal.sync().unwrap();
        }

        let (entries, _) = Wal::read_all_entries(&fs, dir_path, 0).unwrap();
        assert_eq!(entries.len(), 3);

        assert!(matches!(entries[0].value, ValueType::Normal(_)));
        assert!(matches!(entries[1].value, ValueType::Tombstone));
        assert!(matches!(entries[2].value, ValueType::Expiring { .. }));
    }
}
