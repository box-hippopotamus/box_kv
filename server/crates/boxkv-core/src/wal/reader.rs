use std::fs::File;
use std::io::{BufReader, Read};

use thiserror::Error;

use super::{Bytes, LogRecord, LogRecordType, WAL_CRC_SIZE, WAL_HEADER_SIZE, WAL_KEY_LEN_SIZE, WAL_PAYLOAD_LEN_SIZE, WAL_TYPE_SIZE, WAL_VAL_LEN_SIZE};

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

    /// The declared payload length does not match the sum of its components.
    #[error("length mismatch: payload length ({payload_len}) does not equal field length ({field_len}) + key length ({key_len}) + value length ({val_len})")]
    LengthSumMismatch {
        payload_len: u64,
        field_len: u64,
        key_len: u64,
        val_len: u64,
    },

    /// Encountered an unknown or invalid record type byte.
    #[error("Invalid record type: {0}")]
    InvalidRecordType(u8),

    /// The key or value size exceeds the allowed limit.
    #[error("Payload too large: key_len={key_len}, val_len={val_len} (max_key={max_key}, max_val={max_val})")]
    PayloadTooLarge {
        key_len: u64,
        val_len: u64,
        max_key: u64,
        max_val: u64,
    },
}

/// An iterator over `LogRecord`s in a WAL file.
pub struct WalIterator {
    reader: BufReader<File>,
}

impl WalIterator {
    pub fn new(file: File) -> Self {
        Self {
            reader: BufReader::new(file),
        }
    }
}

impl WalIterator {
    /// Reads the next record from the log.
    ///
    /// Returns:
    /// - `Ok(None)`: Reached legitimate End of File (EOF).
    /// - `Ok(Some(record))`: Successfully read a record.
    /// - `Err(e)`: Encountered a corruption or I/O error.
    fn read_next_record(&mut self) -> Result<Option<LogRecord>, ReadError> {
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
        let payload_len = u64::from_be_bytes(header_buf[WAL_CRC_SIZE..WAL_CRC_SIZE+WAL_PAYLOAD_LEN_SIZE].try_into().unwrap());
        let rec_type_u8 = header_buf[WAL_CRC_SIZE+WAL_PAYLOAD_LEN_SIZE];
        let seq = u64::from_be_bytes(header_buf[WAL_CRC_SIZE+WAL_PAYLOAD_LEN_SIZE+WAL_TYPE_SIZE..].try_into().unwrap());

        let rec_type = LogRecordType::try_from(rec_type_u8).map_err(ReadError::InvalidRecordType)?;

        // 3. Read Payload Metadata (Key Length & Value Length)
        const FIELDS_LEN_TOTAL_SIZE: usize = WAL_KEY_LEN_SIZE + WAL_VAL_LEN_SIZE;
        let mut field_len_buf = [0u8; FIELDS_LEN_TOTAL_SIZE];
        self.reader.read_exact(&mut field_len_buf)?;

        let key_len = u64::from_be_bytes(field_len_buf[0..WAL_KEY_LEN_SIZE].try_into().unwrap());
        let val_len = u64::from_be_bytes(field_len_buf[WAL_KEY_LEN_SIZE..].try_into().unwrap());

        // Validate consistency between Payload Length and its components
        if payload_len != (WAL_KEY_LEN_SIZE + WAL_VAL_LEN_SIZE) as u64 + key_len + val_len {
            return Err(ReadError::LengthSumMismatch {
                payload_len,
                field_len: (WAL_KEY_LEN_SIZE + WAL_VAL_LEN_SIZE) as u64,
                key_len,
                val_len });
        }

        // Validate Safety Limits
        if key_len > WAL_MAX_KEY_SIZE || val_len > WAL_MAX_VAL_SIZE {
            return Err(ReadError::PayloadTooLarge {
                key_len,
                val_len,
                max_key: WAL_MAX_KEY_SIZE,
                max_val: WAL_MAX_VAL_SIZE,
            });
        }

        // 4. Read Variable-Length Payload (Key & Value)
        let mut key_buf = vec![0u8; key_len as usize];
        let mut val_buf = vec![0u8; val_len as usize];

        self.reader.read_exact(&mut key_buf)?;
        self.reader.read_exact(&mut val_buf)?;

        // 5. Verify CRC
        // Reconstruct the CRC calculation to verify data integrity.
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&payload_len.to_be_bytes());
        hasher.update(&[rec_type_u8]);
        hasher.update(&seq.to_be_bytes());
        hasher.update(&key_len.to_be_bytes());
        hasher.update(&val_len.to_be_bytes());
        hasher.update(&key_buf);
        hasher.update(&val_buf);

        let calculate_crc = hasher.finalize();
        if calculate_crc != header_crc {
            return Err(ReadError::CrcMismatch { expected: header_crc, actual: calculate_crc });
        }

        // 6. Return LogRecord
        Ok(Some(LogRecord {
            seq,
            key: Bytes::from(key_buf),
            value: Bytes::from(val_buf),
            rec_type,
        }))
    }
}

impl Iterator for WalIterator {
    type Item = Result<LogRecord, ReadError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.read_next_record() {
            Ok(Some(record)) => Some(Ok(record)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}
