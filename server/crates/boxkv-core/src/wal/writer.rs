use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;

use thiserror::Error;

use super::{LogRecord, WAL_KEY_LEN_SIZE, WAL_VAL_LEN_SIZE};

#[derive(Debug, Error)]
pub enum WriteError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

/// A buffered writer for the Write-Ahead Log.
///
/// This struct handles the serialization of `LogRecord`s into the underlying file.
/// It utilizes a `BufWriter` to minimize system calls for better performance.
pub struct WalWriter {
    writer: BufWriter<File>,
}

impl WalWriter {
    /// Creates a new `WalWriter` for the specified path.
    pub fn new(path: PathBuf) -> Result<Self, WriteError> {
        let file = File::create(path)?;
        let writer = BufWriter::new(file);

        Ok(Self {
            writer,
        })
    }

    /// Serializes and appends a `LogRecord` to the WAL buffer.
    ///
    /// Note: This does not guarantee persistence. You must call `sync()` to ensure
    /// data is flushed to the physical disk.
    pub fn append(&mut self, record: &LogRecord) -> Result<(), WriteError> {
        let rec_type = record.rec_type as u8;
        let key_len = record.key.len() as u64;
        let val_len = record.value.len() as u64;
        let seq = record.seq;
        
        // Calculate the payload length: Key Length + Value Length + Key Data + Value Data
        let payload_len = (WAL_KEY_LEN_SIZE + WAL_VAL_LEN_SIZE) as u64 + key_len + val_len;

        // 1. Calculate CRC Checksum
        // The CRC covers: Payload Length, Type, Sequence Number, Key Length, Value Length, Key, and Value.
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&payload_len.to_be_bytes());
        hasher.update(&[rec_type]);
        hasher.update(&seq.to_be_bytes());
        hasher.update(&key_len.to_be_bytes());
        hasher.update(&val_len.to_be_bytes());
        hasher.update(&record.key);
        hasher.update(&record.value);

        let crc = hasher.finalize();

        // 2. Write Header
        // [CRC: 4 bytes]
        self.writer.write_all(&crc.to_be_bytes())?;
        // [Payload Length: 8 bytes]
        self.writer.write_all(&payload_len.to_be_bytes())?;
        // [Type: 1 byte]
        self.writer.write_all(&[rec_type])?;
        // [Seq: 8 bytes]
        self.writer.write_all(&seq.to_be_bytes())?;
        // [Key Length: 8 bytes]
        self.writer.write_all(&key_len.to_be_bytes())?;
        // [Val Length: 8 bytes]
        self.writer.write_all(&val_len.to_be_bytes())?;

        // 3. Write Payload
        self.writer.write_all(&record.key)?;
        self.writer.write_all(&record.value)?;
        Ok(())
    }

    /// Flushes the buffer and fsyncs the file to ensure durability.
    pub fn sync(&mut self) -> Result<(), WriteError> {
        self.writer.flush()?; // Flush BufWriter to OS cache
        self.writer.get_ref().sync_all()?; // Fsync OS cache to physical disk
        Ok(())
    }
}
