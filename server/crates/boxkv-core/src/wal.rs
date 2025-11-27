mod reader;
mod writer;

use crate::wal::reader::{ReadError, WalIterator};
use crate::wal::writer::{WalWriter, WriteError};

use std::fs;
use std::fs::File;
use std::path::{Path, PathBuf};

use bytes::Bytes;
use thiserror::Error;
use tracing::{debug, info, trace, warn};

use boxkv_common::types::Entry;

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

/// Private trait to add file path context to Results.
/// This implements the "Extension Trait" pattern for cleaner error handling.
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

impl<T> WalContext<T, std::io::Error> for Result<T, std::io::Error> {
    fn with_context(self, path: &Path) -> Result<T, WalError> {
        self.map_err(|e| WalError::Read {
            path: path.to_path_buf(),
            source: ReadError::Io(e),
        })
    }
}

/// WAL Binary Format Specification
///
/// ## Header (21 bytes, fixed):
/// ```text
/// +----------+----------------+--------------+----------------+
/// | CRC (4B) | PayloadLen (8B)| ValueTag(1B) | Seq (8B)       |
/// +----------+----------------+--------------+----------------+
/// ```
///
/// ## Payload (variable length):
/// ```text
/// +-------------+----------+----------------------+
/// | KeyLen (8B) | Key Data | Value Section        |
/// +-------------+----------+----------------------+
/// ```
///
/// ## Value Section (format depends on ValueTag):
///
/// **[ValueTag = 0] Normal:**
/// ```text
/// +------------+
/// | Value Data |
/// +------------+
/// ```
///
/// **[ValueTag = 1] Tombstone:**
/// ```text
/// (empty - no data)
/// ```
///
/// **[ValueTag = 2] Expiring:**
/// ```text
/// +-------------+------------+
/// | ExpireAt(8B)| Value Data |
/// +-------------+------------+
/// ```
///
/// ## CRC Checksum Coverage:
/// The CRC32 checksum covers all fields except itself:
/// - PayloadLen (8 bytes)
/// - ValueTag (1 byte)
/// - Seq (8 bytes)
/// - KeyLen (8 bytes)
/// - Key Data (variable)
/// - Value Section (variable)
const WAL_CRC_SIZE: usize = 4;
const WAL_PAYLOAD_LEN_SIZE: usize = 8;
const WAL_TYPE_SIZE: usize = 1;
const WAL_SEQ_SIZE: usize = 8;
const WAL_HEADER_SIZE: usize = WAL_CRC_SIZE + WAL_PAYLOAD_LEN_SIZE + WAL_TYPE_SIZE + WAL_SEQ_SIZE;

const WAL_KEY_LEN_SIZE: usize = 8;
const WAL_EXPIRE_LEN_SIZE: usize = 8;

/// Manages the Write-Ahead Log (WAL) for data persistence and crash recovery.
///
/// This struct represents the *active* WAL file being written to.
pub struct Wal {
    writer: WalWriter,
    path: PathBuf,
}

impl Wal {
    /// Creates a new active WAL file for writing.
    ///
    /// # Arguments
    /// * `dir` - Directory path where the WAL file will be created
    /// * `file_id` - Unique file identifier (formatted as 9-digit zero-padded filename)
    ///
    /// # File Naming
    /// Files are named as `{:09}.wal`, e.g., `000000001.wal`, `000000042.wal`
    ///
    /// # Errors
    /// Returns `WalError::Write` if file creation fails.
    pub fn create(dir: PathBuf, file_id: u64) -> Result<Self, WalError> {
        let path = dir.join(format!("{:09}.wal", file_id));

        info!(file_id, ?path, "Creating WAL file");

        Ok(Self {
            writer: WalWriter::new(path.clone()).with_context(&path)?,
            path,
        })
    }

    /// Recovers all entries from WAL files in the specified directory.
    ///
    /// This function performs crash recovery by:
    /// 1. Scanning all `.wal` files in the directory
    /// 2. Sorting them by file ID (chronological order)
    /// 3. Reading entries from each file sequentially
    /// 4. Filtering out entries with `seq < min_seq` (already persisted to SSTable)
    /// 5. Sorting all recovered entries by sequence number
    ///
    /// # Arguments
    /// * `dir` - Directory containing WAL files
    /// * `min_seq` - Minimum sequence number to recover (entries below this are skipped)
    ///
    /// # Returns
    /// A tuple containing:
    /// - `Vec<Entry>` - All recovered entries sorted by sequence number
    /// - `u64` - Maximum sequence number found (used to resume sequence allocation)
    ///
    /// # Error Handling
    /// - Truncated WAL files (partial last record) are handled gracefully with a warning
    /// - CRC mismatches result in an error
    /// - I/O errors are propagated
    pub fn read_all_entries(dir: PathBuf, min_seq: u64) -> Result<(Vec<Entry>, u64), WalError> {
        info!(min_seq, ?dir, "Starting WAL recovery");
        let start = std::time::Instant::now();

        let read_dir = fs::read_dir(&dir).with_context(&dir)?;

        let mut wal_files: Vec<(u64, PathBuf)> = Vec::new();

        // 1. Scan directory for WAL files
        for entry in read_dir {
            let entry = entry.with_context(&dir)?;
            let path = entry.path();

            if !path.is_file() || path.extension().and_then(|s| s.to_str()) != Some("wal") {
                continue;
            }

            // Parse file ID from filename (e.g., "000000001.wal" -> 1)
            if let Some(stem) = path.file_stem().and_then(|s| s.to_str())
                && let Ok(id) = stem.parse::<u64>()
            {
                wal_files.push((id, path));
            }
        }

        // 2. Sort files by ID to ensure chronological order
        wal_files.sort_unstable_by_key(|&(id, _)| id);

        debug!(file_count = wal_files.len(), "Scanned WAL files");

        let mut max_seq = u64::MIN;
        let mut all_entrise = Vec::new();

        // 3. Iterate through each file and read records
        for (file_id, path) in &wal_files {
            let file = File::open(path).with_context(path)?;
            let read_it = WalIterator::new(file);

            let mut entry_count = 0;
            for res in read_it {
                match res {
                    Ok(entry) => {
                        if entry.seq() >= min_seq {
                            max_seq = max_seq.max(entry.seq());
                            all_entrise.push(entry);
                            entry_count += 1;
                        }
                    }
                    Err(ReadError::Io(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                        // Warn: WAL file truncated at the end.
                        // This is expected if the system crashed while writing the last record.
                        // We ignore the partial record and stop reading this file.
                        warn!(
                            file_id,
                            ?path,
                            "WAL file truncated, skipping partial record"
                        );
                        break;
                    }
                    Err(e) => {
                        return Err(WalError::Read {
                            path: path.clone(),
                            source: e,
                        });
                    }
                }
            }

            debug!(file_id, entry_count, ?path, "Completed reading WAL file");
        }

        // 4. Final sort by sequence number
        // This handles potential out-of-order writes if multiple threads allocated Seqs
        // but wrote to the WAL in a slightly different physical order.
        all_entrise.sort_by_key(|r| r.seq());

        let elapsed = start.elapsed();
        info!(
            record_count = all_entrise.len(),
            max_seq,
            elapsed_ms = elapsed.as_millis(),
            "WAL recovery completed"
        );

        Ok((all_entrise, max_seq))
    }

    /// Appends a PUT operation to the WAL.
    ///
    /// # Arguments
    /// * `seq` - Sequence number for MVCC
    /// * `key` - Key bytes
    /// * `val` - Value bytes
    pub fn append_normal(&mut self, seq: u64, key: Bytes, val: Bytes) -> Result<(), WalError> {
        trace!(
            seq,
            key_len = key.len(),
            val_len = val.len(),
            "Appending PUT to WAL"
        );

        self.writer
            .append(&Entry::new_normal(seq, key, val))
            .with_context(&self.path)?;

        Ok(())
    }

    /// Appends a DELETE operation (Tombstone) to the WAL.
    ///
    /// # Arguments
    /// * `seq` - Sequence number for MVCC
    /// * `key` - Key to delete
    pub fn append_tombstone(&mut self, seq: u64, key: Bytes) -> Result<(), WalError> {
        trace!(seq, key_len = key.len(), "Appending DELETE to WAL");

        self.writer
            .append(&Entry::new_tombstone(seq, key))
            .with_context(&self.path)?;

        Ok(())
    }

    /// Appends an expiring value entry with TTL to the WAL.
    ///
    /// # Arguments
    /// * `seq` - Sequence number for MVCC
    /// * `key` - Key bytes
    /// * `val` - Value bytes
    /// * `expire_at` - Unix timestamp (seconds) when this entry expires
    pub fn append_expire(
        &mut self,
        seq: u64,
        key: Bytes,
        val: Bytes,
        expire_at: u64,
    ) -> Result<(), WalError> {
        trace!(seq, key_len = key.len(), "Appending EXPIRE to WAL");

        self.writer
            .append(&Entry::new_expiring(seq, key, val, expire_at))
            .with_context(&self.path)?;

        Ok(())
    }

    /// Deletes a WAL file by its ID.
    ///
    /// This is typically called after the corresponding Memtable has been successfully
    /// flushed to an SSTable file. Once persisted to disk via SSTable, the WAL records
    /// are no longer needed for recovery.
    ///
    /// # Arguments
    /// * `dir` - Directory containing the WAL file
    /// * `file_id` - File identifier to delete
    pub fn delete(dir: PathBuf, file_id: u64) -> Result<(), WalError> {
        let path = dir.join(format!("{:09}.wal", file_id));

        info!(file_id, ?path, "Deleting WAL file");

        fs::remove_file(&path).with_context(&path)
    }

    /// Syncs all pending writes to physical disk (fsync).
    ///
    /// This ensures durability by flushing:
    /// 1. BufWriter buffer to OS page cache
    /// 2. OS page cache to physical disk
    ///
    /// Must be called to guarantee crash recovery works correctly.
    pub fn sync(&mut self) -> Result<(), WalError> {
        debug!(?self.path, "Syncing WAL to disk");

        self.writer.sync().with_context(&self.path)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use boxkv_common::types::ValueType;
    use tempfile::TempDir;

    #[test]
    fn test_wal_create_and_file_naming() {
        let temp_dir = TempDir::new().unwrap();
        let dir_path = temp_dir.path().to_path_buf();

        let _wal = Wal::create(dir_path.clone(), 1).unwrap();
        assert!(dir_path.join("000000001.wal").exists());

        let _wal2 = Wal::create(dir_path.clone(), 42).unwrap();
        assert!(dir_path.join("000000042.wal").exists());

        let _wal3 = Wal::create(dir_path.clone(), 123456789).unwrap();
        assert!(dir_path.join("123456789.wal").exists());
    }

    #[test]
    fn test_wal_append_normal_value() {
        let temp_dir = TempDir::new().unwrap();
        let dir_path = temp_dir.path().to_path_buf();

        {
            let mut wal = Wal::create(dir_path.clone(), 1).unwrap();
            wal.append_normal(100, Bytes::from("key1"), Bytes::from("value1"))
                .unwrap();
            wal.append_normal(101, Bytes::from("key2"), Bytes::from("value2"))
                .unwrap();
            wal.sync().unwrap();
        }

        let (entries, max_seq) = Wal::read_all_entries(dir_path, 0).unwrap();
        assert_eq!(max_seq, 101);
        assert_eq!(entries.len(), 2);

        assert_eq!(entries[0].seq(), 100);
        assert_eq!(entries[0].key().as_ref(), b"key1");
        match entries[0].val() {
            ValueType::Normal(data) => assert_eq!(data.as_ref(), b"value1"),
            _ => panic!("Expected Normal value"),
        }

        assert_eq!(entries[1].seq(), 101);
        assert_eq!(entries[1].key().as_ref(), b"key2");
        match entries[1].val() {
            ValueType::Normal(data) => assert_eq!(data.as_ref(), b"value2"),
            _ => panic!("Expected Normal value"),
        }
    }

    #[test]
    fn test_wal_append_tombstone() {
        let temp_dir = TempDir::new().unwrap();
        let dir_path = temp_dir.path().to_path_buf();

        {
            let mut wal = Wal::create(dir_path.clone(), 1).unwrap();
            wal.append_tombstone(200, Bytes::from("deleted_key"))
                .unwrap();
            wal.sync().unwrap();
        }

        let (entries, max_seq) = Wal::read_all_entries(dir_path, 0).unwrap();
        assert_eq!(max_seq, 200);
        assert_eq!(entries.len(), 1);

        assert_eq!(entries[0].seq(), 200);
        assert_eq!(entries[0].key().as_ref(), b"deleted_key");
        assert!(entries[0].is_tombstone());
        assert!(matches!(entries[0].val(), ValueType::Tombstone));
    }

    #[test]
    fn test_wal_append_expiring_value() {
        let temp_dir = TempDir::new().unwrap();
        let dir_path = temp_dir.path().to_path_buf();

        let expire_at = 1234567890u64;

        {
            let mut wal = Wal::create(dir_path.clone(), 1).unwrap();
            wal.append_expire(
                300,
                Bytes::from("expire_key"),
                Bytes::from("expire_value"),
                expire_at,
            )
            .unwrap();
            wal.sync().unwrap();
        }

        let (entries, max_seq) = Wal::read_all_entries(dir_path, 0).unwrap();
        assert_eq!(max_seq, 300);
        assert_eq!(entries.len(), 1);

        assert_eq!(entries[0].seq(), 300);
        assert_eq!(entries[0].key().as_ref(), b"expire_key");
        match entries[0].val() {
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

        {
            let mut wal = Wal::create(dir_path.clone(), 1).unwrap();
            wal.append_normal(1, Bytes::from("k1"), Bytes::from("v1"))
                .unwrap();
            wal.append_tombstone(2, Bytes::from("k2")).unwrap();
            wal.append_expire(3, Bytes::from("k3"), Bytes::from("v3"), 9999)
                .unwrap();
            wal.append_normal(4, Bytes::from("k4"), Bytes::from("v4"))
                .unwrap();
            wal.sync().unwrap();
        }

        let (entries, max_seq) = Wal::read_all_entries(dir_path, 0).unwrap();
        assert_eq!(max_seq, 4);
        assert_eq!(entries.len(), 4);

        assert!(matches!(entries[0].val(), ValueType::Normal(_)));
        assert!(matches!(entries[1].val(), ValueType::Tombstone));
        assert!(matches!(entries[2].val(), ValueType::Expiring { .. }));
        assert!(matches!(entries[3].val(), ValueType::Normal(_)));
    }

    #[test]
    fn test_wal_multiple_files_recovery() {
        let temp_dir = TempDir::new().unwrap();
        let dir_path = temp_dir.path().to_path_buf();

        // Create WAL file 1
        {
            let mut wal1 = Wal::create(dir_path.clone(), 1).unwrap();
            wal1.append_normal(10, Bytes::from("k1"), Bytes::from("v1"))
                .unwrap();
            wal1.append_normal(20, Bytes::from("k2"), Bytes::from("v2"))
                .unwrap();
            wal1.sync().unwrap();
        }

        // Create WAL file 2
        {
            let mut wal2 = Wal::create(dir_path.clone(), 2).unwrap();
            wal2.append_normal(30, Bytes::from("k3"), Bytes::from("v3"))
                .unwrap();
            wal2.append_tombstone(40, Bytes::from("k1")).unwrap();
            wal2.sync().unwrap();
        }

        // Create WAL file 3
        {
            let mut wal3 = Wal::create(dir_path.clone(), 3).unwrap();
            wal3.append_expire(50, Bytes::from("k4"), Bytes::from("v4"), 8888)
                .unwrap();
            wal3.sync().unwrap();
        }

        // Recover all
        let (entries, max_seq) = Wal::read_all_entries(dir_path.clone(), 0).unwrap();
        assert_eq!(max_seq, 50);
        assert_eq!(entries.len(), 5);

        // Verify chronological order
        assert_eq!(entries[0].seq(), 10);
        assert_eq!(entries[1].seq(), 20);
        assert_eq!(entries[2].seq(), 30);
        assert_eq!(entries[3].seq(), 40);
        assert_eq!(entries[4].seq(), 50);
    }

    #[test]
    fn test_wal_min_seq_filtering() {
        let temp_dir = TempDir::new().unwrap();
        let dir_path = temp_dir.path().to_path_buf();

        {
            let mut wal = Wal::create(dir_path.clone(), 1).unwrap();
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

        // Filter seq < 25 (should get seq 30 and 40)
        let (entries, max_seq) = Wal::read_all_entries(dir_path.clone(), 25).unwrap();
        assert_eq!(max_seq, 40);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].seq(), 30);
        assert_eq!(entries[1].seq(), 40);

        // Filter seq < 40 (should get only seq 40)
        let (entries, max_seq) = Wal::read_all_entries(dir_path.clone(), 40).unwrap();
        assert_eq!(max_seq, 40);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].seq(), 40);

        // Filter seq < 100 (should get nothing)
        let (entries, _) = Wal::read_all_entries(dir_path, 100).unwrap();
        assert_eq!(entries.len(), 0);
    }

    #[test]
    fn test_wal_delete_file() {
        let temp_dir = TempDir::new().unwrap();
        let dir_path = temp_dir.path().to_path_buf();

        let _wal = Wal::create(dir_path.clone(), 1).unwrap();
        let file_path = dir_path.join("000000001.wal");
        assert!(file_path.exists());

        Wal::delete(dir_path.clone(), 1).unwrap();
        assert!(!file_path.exists());

        // Deleting non-existent file should return error
        assert!(Wal::delete(dir_path, 999).is_err());
    }

    #[test]
    fn test_wal_empty_recovery() {
        let temp_dir = TempDir::new().unwrap();
        let dir_path = temp_dir.path().to_path_buf();

        // No WAL files exist
        let (entries, max_seq) = Wal::read_all_entries(dir_path, 0).unwrap();
        assert_eq!(entries.len(), 0);
        assert_eq!(max_seq, u64::MIN);
    }

    #[test]
    fn test_wal_large_values() {
        let temp_dir = TempDir::new().unwrap();
        let dir_path = temp_dir.path().to_path_buf();

        let large_key = vec![b'k'; 1024]; // 1KB key
        let large_value = vec![b'v'; 1024 * 1024]; // 1MB value

        {
            let mut wal = Wal::create(dir_path.clone(), 1).unwrap();
            wal.append_normal(
                1,
                Bytes::from(large_key.clone()),
                Bytes::from(large_value.clone()),
            )
            .unwrap();
            wal.sync().unwrap();
        }

        let (entries, _) = Wal::read_all_entries(dir_path, 0).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].key().len(), 1024);
        match entries[0].val() {
            ValueType::Normal(data) => assert_eq!(data.len(), 1024 * 1024),
            _ => panic!("Expected Normal value"),
        }
    }

    #[test]
    fn test_wal_empty_key_and_value() {
        let temp_dir = TempDir::new().unwrap();
        let dir_path = temp_dir.path().to_path_buf();

        {
            let mut wal = Wal::create(dir_path.clone(), 1).unwrap();
            wal.append_normal(1, Bytes::from(""), Bytes::from(""))
                .unwrap();
            wal.append_tombstone(2, Bytes::from("")).unwrap();
            wal.sync().unwrap();
        }

        let (entries, _) = Wal::read_all_entries(dir_path, 0).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].key().len(), 0);
        assert_eq!(entries[1].key().len(), 0);
    }

    #[test]
    fn test_wal_binary_key_and_value() {
        let temp_dir = TempDir::new().unwrap();
        let dir_path = temp_dir.path().to_path_buf();

        // Binary data with all byte values
        let binary_key: Vec<u8> = (0..=255).collect();
        let binary_value: Vec<u8> = (0..=255).rev().collect();

        {
            let mut wal = Wal::create(dir_path.clone(), 1).unwrap();
            wal.append_normal(
                1,
                Bytes::from(binary_key.clone()),
                Bytes::from(binary_value.clone()),
            )
            .unwrap();
            wal.sync().unwrap();
        }

        let (entries, _) = Wal::read_all_entries(dir_path, 0).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].key().as_ref(), binary_key.as_slice());
        match entries[0].val() {
            ValueType::Normal(data) => assert_eq!(data.as_ref(), binary_value.as_slice()),
            _ => panic!("Expected Normal value"),
        }
    }

    #[test]
    fn test_wal_sequence_number_ordering() {
        let temp_dir = TempDir::new().unwrap();
        let dir_path = temp_dir.path().to_path_buf();

        {
            let mut wal = Wal::create(dir_path.clone(), 1).unwrap();
            // Write in non-sequential order
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

        let (entries, max_seq) = Wal::read_all_entries(dir_path, 0).unwrap();
        assert_eq!(max_seq, 200);
        assert_eq!(entries.len(), 4);

        // Should be sorted by sequence number
        assert_eq!(entries[0].seq(), 50);
        assert_eq!(entries[1].seq(), 75);
        assert_eq!(entries[2].seq(), 100);
        assert_eq!(entries[3].seq(), 200);
    }

    #[test]
    fn test_wal_sync_durability() {
        let temp_dir = TempDir::new().unwrap();
        let dir_path = temp_dir.path().to_path_buf();

        {
            let mut wal = Wal::create(dir_path.clone(), 1).unwrap();
            wal.append_normal(1, Bytes::from("k1"), Bytes::from("v1"))
                .unwrap();
            // sync() ensures data is on disk
            wal.sync().unwrap();
        }

        // Drop wal without explicit sync should still work because we called sync()
        let (entries, _) = Wal::read_all_entries(dir_path, 0).unwrap();
        assert_eq!(entries.len(), 1);
    }
}
