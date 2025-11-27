use std::fs::File;
use std::io::{BufReader, Read};
use thiserror::Error;
use tracing::warn;

use super::{
    Bytes, WAL_CRC_SIZE, WAL_EXPIRE_LEN_SIZE, WAL_HEADER_SIZE, WAL_KEY_LEN_SIZE,
    WAL_PAYLOAD_LEN_SIZE, WAL_TYPE_SIZE,
};

use boxkv_common::types::{EXPIRING_VALUE_TYPE, Entry, NORMAL_VALUE_TYPE, TOMBSTONE_VALUE_TYPE};

// Safety limits to prevent OOM attacks from corrupted/malicious WAL files.
// Adjust these values based on your system requirements.
const WAL_MAX_KEY_SIZE: u64 = 1024 * 1024; // 1MB
const WAL_MAX_VAL_SIZE: u64 = 64 * 1024 * 1024; // 64MB

#[derive(Debug, Error)]
pub enum ReadError {
    #[error(transparent)]
    Io(#[from] std::io::Error),

    /// CRC checksum mismatch (data corruption or invalid checksum).
    #[error("CRC checksum mismatch: expected {expected:08x}, got {actual:08x}")]
    CrcMismatch {
        expected: u32, // The expected CRC value stored in the header.
        actual: u32,   // The actual CRC value calculated from the payload.
    },

    /// Encountered an unknown or invalid record type byte.
    #[error("Invalid record type: {0}")]
    InvalidRecordType(u8),

    /// The key or value size exceeds the allowed limit.
    #[error(
        "Payload too large: key_len={key_len}, val_len={val_len} (max_key={max_key}, max_val={max_val})"
    )]
    PayloadTooLarge {
        key_len: u64,
        val_len: u64,
        max_key: u64,
        max_val: u64,
    },
}

/// Iterator over `Entry` records in a WAL file.
///
/// Reads and deserializes entries sequentially from the WAL binary format.
/// Uses `BufReader` for efficient I/O.
pub struct WalIterator {
    reader: BufReader<File>,
}

impl WalIterator {
    /// Creates a new iterator from an open file handle.
    pub fn new(file: File) -> Self {
        Self {
            reader: BufReader::new(file),
        }
    }
}

impl WalIterator {
    /// Reads and deserializes the next entry from the WAL.
    ///
    /// # Returns
    /// - `Ok(None)`: Clean EOF reached (no more records)
    /// - `Ok(Some(Entry))`: Successfully read and validated entry
    /// - `Err(ReadError)`: Corruption, I/O error, or validation failure
    ///
    /// # Error Handling
    /// - Partial reads at EOF are treated as truncation (expected during crash)
    /// - CRC mismatches indicate data corruption
    /// - Oversized keys/values are rejected to prevent OOM attacks
    fn read_next_entry(&mut self) -> Result<Option<Entry>, ReadError> {
        // 1. Read Header
        let mut header_buf = [0u8; WAL_HEADER_SIZE];
        // Attempt to read the fixed-size header.
        // If we read 0 bytes, it's a clean EOF.
        // If we read partial bytes, we try to fill the buffer or error out.
        match self.reader.read(&mut header_buf)? {
            0 => return Ok(None),
            WAL_HEADER_SIZE => (),
            n => self.reader.read_exact(&mut header_buf[n..])?,
        }

        // 2. Parse Header
        let header_crc = u32::from_be_bytes(header_buf[0..WAL_CRC_SIZE].try_into().unwrap());
        let payload_len = u64::from_be_bytes(
            header_buf[WAL_CRC_SIZE..WAL_CRC_SIZE + WAL_PAYLOAD_LEN_SIZE]
                .try_into()
                .unwrap(),
        );
        let val_type_u8 = header_buf[WAL_CRC_SIZE + WAL_PAYLOAD_LEN_SIZE];
        let seq = u64::from_be_bytes(
            header_buf[WAL_CRC_SIZE + WAL_PAYLOAD_LEN_SIZE + WAL_TYPE_SIZE..]
                .try_into()
                .unwrap(),
        );

        // 3. (Key Length & Key Data)
        let mut key_len_buf = [0u8; WAL_KEY_LEN_SIZE];
        self.reader.read_exact(&mut key_len_buf)?;
        let key_len = u64::from_be_bytes(key_len_buf);

        let mut key_buf = vec![0u8; key_len as usize];
        self.reader.read_exact(&mut key_buf)?;

        // Calculate value section length
        // payload_len = KeyLen(8B) + Key + Value Section
        let val_len = payload_len - WAL_KEY_LEN_SIZE as u64 - key_len;

        // Validate Safety Limits
        if key_len > WAL_MAX_KEY_SIZE || val_len > WAL_MAX_VAL_SIZE {
            warn!(
                key_len,
                val_len,
                max_key = WAL_MAX_KEY_SIZE,
                max_val = WAL_MAX_VAL_SIZE,
                "Payload size exceeds safety limits"
            );
            return Err(ReadError::PayloadTooLarge {
                key_len,
                val_len,
                max_key: WAL_MAX_KEY_SIZE,
                max_val: WAL_MAX_VAL_SIZE,
            });
        }

        // 4. Value
        let mut val_buf = vec![0u8; val_len as usize];
        self.reader.read_exact(&mut val_buf)?;

        // 5. Verify CRC
        // Reconstruct the CRC calculation to verify data integrity.
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&payload_len.to_be_bytes());
        hasher.update(&[val_type_u8]);
        hasher.update(&seq.to_be_bytes());
        hasher.update(&key_len.to_be_bytes());
        hasher.update(&key_buf);
        hasher.update(&val_buf);

        let calculate_crc = hasher.finalize();
        if calculate_crc != header_crc {
            warn!(
                expected = header_crc,
                actual = calculate_crc,
                seq,
                "CRC checksum mismatch detected"
            );
            return Err(ReadError::CrcMismatch {
                expected: header_crc,
                actual: calculate_crc,
            });
        }

        let key = Bytes::from(key_buf);
        match val_type_u8 {
            NORMAL_VALUE_TYPE => {
                let data = Bytes::from(val_buf);
                Ok(Some(Entry::new_normal(seq, key, data)))
            }
            TOMBSTONE_VALUE_TYPE => Ok(Some(Entry::new_tombstone(seq, key))),
            EXPIRING_VALUE_TYPE => {
                let expire_at =
                    u64::from_be_bytes(val_buf[..WAL_EXPIRE_LEN_SIZE].try_into().unwrap());
                let data = Bytes::from(val_buf).slice(WAL_EXPIRE_LEN_SIZE..);
                Ok(Some(Entry::new_expiring(seq, key, data, expire_at)))
            }
            _ => Err(ReadError::InvalidRecordType(val_type_u8)),
        }
    }
}

impl Iterator for WalIterator {
    type Item = Result<Entry, ReadError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.read_next_entry() {
            Ok(Some(entry)) => Some(Ok(entry)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}
