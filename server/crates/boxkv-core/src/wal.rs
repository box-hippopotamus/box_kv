mod writer;
mod reader;

use std::fs;
use std::fs::File;
use std::path::{Path, PathBuf};
use bytes::Bytes;
use thiserror::Error;
use crate::wal::reader::{ReadError, WalIterator};
use crate::wal::writer::{WalWriter, WriteError};

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

// Binary Format Specification:
//
// Header (Fixed Size):
// +----------+----------------+-----------+----------------+
// | CRC (4B) | PayloadLen (8B)| Type (1B) | Seq (8B)       |
// +----------+----------------+-----------+----------------+
//
// Payload (Variable Size):
// +-------------+-------------+----------+------------+
// | KeyLen (8B) | ValLen (8B) | Key Data | Value Data |
// +-------------+-------------+----------+------------+

const WAL_CRC_SIZE: usize = 4;
const WAL_PAYLOAD_LEN_SIZE: usize = 8;
const WAL_TYPE_SIZE: usize = 1;
const WAL_SEQ_SIZE: usize = 8;
const WAL_HEADER_SIZE: usize = WAL_CRC_SIZE + WAL_PAYLOAD_LEN_SIZE + WAL_TYPE_SIZE + WAL_SEQ_SIZE;

const WAL_KEY_LEN_SIZE: usize = 8;
const WAL_VAL_LEN_SIZE: usize = 8;

/// Represents a single entry in the Write-Ahead Log.
pub struct LogRecord {
    /// Monotonically increasing sequence number for MVCC and recovery.
    pub seq: u64,
    pub key: Bytes,
    pub value: Bytes,
    pub rec_type: LogRecordType, 
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum LogRecordType {
    Normal = 1,
    Tombstone = 2,
}

impl TryFrom<u8> for LogRecordType {
    type Error = u8; // Returns the invalid value for debugging

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(LogRecordType::Normal),
            2 => Ok(LogRecordType::Tombstone),
            other => Err(other),
        }
    }
}

/// Manages the Write-Ahead Log (WAL) for data persistence and crash recovery.
///
/// This struct represents the *active* WAL file being written to.
pub struct Wal {
    writer: WalWriter,
    path: PathBuf,
}

impl Wal {
    /// Creates a new WAL instance backed by a file with the given `file_id`.
    ///
    /// The file name will be formatted as `{:09}.wal` (e.g., `000000001.wal`).
    pub fn create(dir: PathBuf, file_id: u64) -> Result<Self, WalError> {
        let path = dir.join(format!("{:09}.wal", file_id));

        Ok(Self{
            writer: WalWriter::new(path.clone()).with_context(&path)?,
            path,
        })
    }

    /// Recovers all log records from the specified directory.
    ///
    /// This function scans all `.wal` files, sorts them by ID, and iterates through records.
    /// Records with a sequence number less than `min_seq` are skipped (already persisted).
    ///
    /// Returns:
    /// - A vector of valid `LogRecord`s to be replayed into the Memtable.
    /// - The maximum sequence number found during recovery.
    pub fn read_all_logs(dir: PathBuf, min_seq: u64) -> Result<(Vec<LogRecord>, u64), WalError> {
        let read_dir = fs::read_dir(&dir).with_context(&dir)?;

        let mut wal_files: Vec<(u64, PathBuf)> = Vec::new();
        
        // 1. Scan directory for WAL files
        for entry in read_dir {
            let entry = entry.with_context(&dir)?;
            let path = entry.path();
    
            if !path.is_file() {
                continue;
            }
    
            if path.extension().and_then(|s| s.to_str()) != Some("wal") {
                continue;
            }
    
            // Parse file ID from filename (e.g., "000000001.wal" -> 1)
            if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                if let Ok(id) = stem.parse::<u64>() {
                    wal_files.push((id, path));
                }
            }
        }

        // 2. Sort files by ID to ensure chronological order
        wal_files.sort_unstable_by_key(|&(id, _)| id);

        let mut max_seq = u64::MIN;
        let mut all_records = Vec::new();

        // 3. Iterate through each file and read records
        for (_, path) in &wal_files {
            let file = File::open(&path).with_context(&path)?;
            let read_it = WalIterator::new(file);

            for res in read_it {
                match res {
                    Ok(record) => {
                        if record.seq >= min_seq {
                            max_seq = max_seq.max(record.seq);
                            all_records.push(record);
                        }
                    }
                    Err(ReadError::Io(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                        // Warn: WAL file truncated at the end.
                        // This is expected if the system crashed while writing the last record.
                        // We ignore the partial record and stop reading this file.
                        break; 
                    }
                    Err(e) => return Err(WalError::Read {
                        path: path.clone(),
                        source: e
                    }),
                }
            }
        }

        // 4. Final sort by sequence number
        // This handles potential out-of-order writes if multiple threads allocated Seqs
        // but wrote to the WAL in a slightly different physical order.
        all_records.sort_by_key(|r| r.seq);
        
        Ok((all_records, max_seq))
    }

    /// Appends a Put operation (Key-Value pair) to the WAL.
    pub fn append_put(&mut self, key: &[u8], value: &[u8], seq: u64) -> Result<(), WalError> {
        self.writer.append(&LogRecord {
            seq,
            key: Bytes::from(key.to_vec()),
            value: Bytes::from(value.to_vec()),
            rec_type: LogRecordType::Normal,
        }).with_context(&self.path)?;

        Ok(())
    }

    /// Appends a Delete operation (Tombstone) to the WAL.
    pub fn append_delete(&mut self, key: &[u8], seq: u64) -> Result<(), WalError> {
        self.writer.append(&LogRecord {
            seq,
            key: Bytes::from(key.to_vec()),
            value: Bytes::new(),
            rec_type: LogRecordType::Tombstone,
        }).with_context(&self.path)?;

        Ok(())
    }

    /// Deletes a WAL file by its ID.
    ///
    /// This is typically called after the corresponding Memtable has been successfully flushed to SSTable.
    pub fn delete(dir: PathBuf, file_id: u64) -> Result<(), WalError> {
        let path = dir.join(format!("{:09}.wal", file_id));
        fs::remove_file(&path).with_context(&path)
    }

    /// Syncs all pending writes to the physical disk.
    pub fn sync(&mut self) -> Result<(), WalError> {
        self.writer.sync().with_context(&self.path)?;
        Ok(())
    }
}
