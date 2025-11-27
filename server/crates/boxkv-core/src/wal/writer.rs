use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;

use thiserror::Error;
use tracing::debug;

use super::WAL_KEY_LEN_SIZE;
use boxkv_common::types::{Entry, ValueType};

#[derive(Debug, Error)]
pub enum WriteError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

/// Buffered writer for Write-Ahead Log files.
///
/// Handles serialization of `Entry` records into the WAL binary format.
/// Uses `BufWriter` to batch writes and reduce system call overhead.
pub struct WalWriter {
    writer: BufWriter<File>,
}

impl WalWriter {
    /// Creates a new `WalWriter` for the specified file path.
    ///
    /// The file is created if it doesn't exist, or truncated if it does.
    pub fn new(path: PathBuf) -> Result<Self, WriteError> {
        debug!(?path, "Creating WalWriter");

        let file = File::create(path)?;
        let writer = BufWriter::new(file);

        Ok(Self { writer })
    }

    /// Serializes and appends an `Entry` to the WAL buffer.
    ///
    /// # Format
    /// Writes in the following order:
    /// 1. Header: CRC | PayloadLen | ValueTag | Seq
    /// 2. Payload: KeyLen | Key | Value Section
    ///
    /// The Value Section format depends on the ValueType (see module-level docs).
    ///
    /// # Durability
    /// This writes to the internal buffer only. Call `sync()` to ensure data
    /// reaches physical disk.
    pub fn append(&mut self, entry: &Entry) -> Result<(), WriteError> {
        let val_type = entry.val().type_tag();
        let key_len = entry.key().len() as u64;
        let val_len = entry.val().serialized_len() as u64;
        let seq = entry.seq();

        // Calculate the payload length: Key Length + Value Length + Key Data + Value Data
        let payload_len = WAL_KEY_LEN_SIZE as u64 + key_len + val_len;

        // 1. Calculate CRC Checksum
        // The CRC covers: Payload Length, Type, Sequence Number, Key Length, Value Length, Key, and Value.
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&payload_len.to_be_bytes());
        hasher.update(&[val_type]);
        hasher.update(&seq.to_be_bytes());
        hasher.update(&key_len.to_be_bytes());
        hasher.update(entry.key());

        // value
        match entry.val() {
            ValueType::Normal(data) => {
                hasher.update(data);
            }
            ValueType::Tombstone => {}
            ValueType::Expiring { data, expire_at } => {
                hasher.update(&expire_at.to_be_bytes());
                hasher.update(data);
            }
        }

        let crc = hasher.finalize();

        // 2. Write Header
        // [CRC: 4 bytes]
        self.writer.write_all(&crc.to_be_bytes())?;
        // [Payload Length: 8 bytes]
        self.writer.write_all(&payload_len.to_be_bytes())?;
        // [Type: 1 byte]
        self.writer.write_all(&[val_type])?;
        // [Seq: 8 bytes]
        self.writer.write_all(&seq.to_be_bytes())?;
        // [Key Length: 8 bytes]
        self.writer.write_all(&key_len.to_be_bytes())?;

        self.writer.write_all(entry.key())?;

        match entry.val() {
            ValueType::Normal(data) => {
                self.writer.write_all(data)?;
            }
            ValueType::Tombstone => {}
            ValueType::Expiring { data, expire_at } => {
                self.writer.write_all(&expire_at.to_be_bytes())?;
                self.writer.write_all(data)?;
            }
        }

        Ok(())
    }

    /// Flushes all buffered writes to disk (fsync).
    ///
    /// This ensures crash recovery can see all data written before this call.
    /// Performs:
    /// 1. `flush()` - Flushes BufWriter to OS page cache
    /// 2. `sync_all()` - Fsyncs OS cache to physical disk
    pub fn sync(&mut self) -> Result<(), WriteError> {
        self.writer.flush()?; // Flush BufWriter to OS cache
        self.writer.get_ref().sync_all()?; // Fsync OS cache to physical disk
        Ok(())
    }
}
