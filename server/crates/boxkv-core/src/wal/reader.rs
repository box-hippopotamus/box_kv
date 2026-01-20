use bytes::Bytes;
use thiserror::Error;
use tracing::warn;

use super::{WAL_CRC_SIZE, WAL_HEADER_SIZE, WAL_KEY_LEN_SIZE, WAL_PAYLOAD_LEN_SIZE, WAL_TYPE_SIZE};

use boxkv_common::codec::DecodeWithContext;
use boxkv_common::config::GlobalConfig;
use boxkv_common::types::{Entry, ValueType};
use boxkv_storage::{ReadableFile, StorageError};

const DEFAULT_WAL_MAX_KEY_SIZE: u64 = 1024 * 1024; // 1MB
const DEFAULT_WAL_MAX_VAL_SIZE: u64 = 64 * 1024 * 1024; // 64MB

#[inline]
fn wal_limits() -> (u64, u64) {
    if let Some(cfg) = GlobalConfig::try_get() {
        (
            cfg.storage.wal_max_key_size_bytes,
            cfg.storage.wal_max_value_size_bytes,
        )
    } else {
        (DEFAULT_WAL_MAX_KEY_SIZE, DEFAULT_WAL_MAX_VAL_SIZE)
    }
}

#[derive(Debug, Error)]
pub enum ReadError {
    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Storage(#[from] StorageError),

    /// CRC 校验失败（数据损坏）
    #[error("CRC checksum mismatch: expected {expected:08x}, got {actual:08x}")]
    CrcMismatch {
        expected: u32, // 头部存储的 CRC 值
        actual: u32,   // 从 payload 计算的 CRC 值
    },

    /// 无效的记录类型
    #[error("Invalid record type: {0}")]
    InvalidRecordType(u8),

    /// key 或 value 大小超过限制
    #[error(
        "Payload too large: key_len={key_len}, val_len={val_len} (max_key={max_key}, max_val={max_val})"
    )]
    PayloadTooLarge {
        key_len: u64,
        val_len: u64,
        max_key: u64,
        max_val: u64,
    },

    #[error("Decode error: {0}")]
    Decode(String),
}

/// WAL 文件迭代器
///
/// 顺序读取并反序列化 WAL 文件中的 Entry 记录
pub struct WalIterator {
    file: Box<dyn ReadableFile>,
    position: u64,
}

impl WalIterator {
    /// 从打开的文件句柄创建迭代器
    pub fn new(file: Box<dyn ReadableFile>) -> Self {
        Self { file, position: 0 }
    }
}

impl WalIterator {
    /// 读取并反序列化下一条 Entry
    ///
    /// # 返回值
    /// - `Ok(None)`: 到达文件末尾
    /// - `Ok(Some(Entry))`: 成功读取并验证
    /// - `Err(ReadError)`: 数据损坏、IO 错误或验证失败
    ///
    /// # 错误处理
    /// - EOF 处的部分读取视为截断（崩溃恢复场景）
    /// - CRC 不匹配表示数据损坏
    /// - 超大 key/value 被拒绝以防止 OOM
    fn read_next_entry(&mut self) -> Result<Option<Entry>, ReadError> {
        // 1. Read Header
        let header_data = match self.file.read_at(self.position, WAL_HEADER_SIZE) {
            Ok(data) if data.len() == WAL_HEADER_SIZE => data,
            Ok(data) if data.is_empty() => {
                return Ok(None); // EOF
            }
            Ok(data) => {
                warn!(
                    position = self.position,
                    expected = WAL_HEADER_SIZE,
                    actual = data.len(),
                    "WAL Reader: Partial header read"
                );
                return Err(ReadError::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    format!(
                        "Partial header read: expected {} bytes, got {}",
                        WAL_HEADER_SIZE,
                        data.len()
                    ),
                )));
            }
            Err(e) => {
                // 检查是否为 UnexpectedEof 错误（正常 EOF）
                match &e {
                    StorageError::Io(io_err)
                        if io_err.kind() == std::io::ErrorKind::UnexpectedEof =>
                    {
                        return Ok(None); // Normal EOF
                    }
                    _ => {}
                }
                warn!(
                    position = self.position,
                    expected = WAL_HEADER_SIZE,
                    error = ?e,
                    "WAL Reader: Failed to read header"
                );
                return Err(ReadError::Storage(e));
            }
        };

        let header_buf = header_data.as_ref();

        // 2. Parse Header
        let header_crc =
            u32::from_be_bytes(header_buf[0..WAL_CRC_SIZE].try_into().map_err(|_| {
                ReadError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Invalid header CRC size",
                ))
            })?);
        let payload_len = u64::from_be_bytes(
            header_buf[WAL_CRC_SIZE..WAL_CRC_SIZE + WAL_PAYLOAD_LEN_SIZE]
                .try_into()
                .map_err(|_| {
                    ReadError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "Invalid payload length size",
                    ))
                })?,
        );
        let value_type_u8 = header_buf[WAL_CRC_SIZE + WAL_PAYLOAD_LEN_SIZE];
        let seq_start = WAL_CRC_SIZE + WAL_PAYLOAD_LEN_SIZE + WAL_TYPE_SIZE;
        let seq_end = seq_start + 8;
        let sequence =
            u64::from_be_bytes(header_buf[seq_start..seq_end].try_into().map_err(|_| {
                ReadError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Invalid sequence size",
                ))
            })?);

        self.position += WAL_HEADER_SIZE as u64;

        // 3. (Key Length & Key Data)
        let key_len_data = match self.file.read_at(self.position, WAL_KEY_LEN_SIZE) {
            Ok(data) => data,
            Err(e) => {
                warn!(
                    position = self.position,
                    expected = WAL_KEY_LEN_SIZE,
                    error = ?e,
                    error_msg = %e,
                    "WAL Reader: Failed to read key length - file may be truncated or not fully written"
                );
                return Err(ReadError::Storage(e));
            }
        };

        if key_len_data.len() != WAL_KEY_LEN_SIZE {
            warn!(
                position = self.position,
                expected = WAL_KEY_LEN_SIZE,
                actual = key_len_data.len(),
                "WAL Reader: Partial key length read"
            );
            return Err(ReadError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!(
                    "Partial key length read: expected {} bytes, got {}",
                    WAL_KEY_LEN_SIZE,
                    key_len_data.len()
                ),
            )));
        }
        let key_len = u64::from_be_bytes(key_len_data.as_ref().try_into().map_err(|_| {
            ReadError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid key length size",
            ))
        })?);
        self.position += WAL_KEY_LEN_SIZE as u64;

        let key_data = match self.file.read_at(self.position, key_len as usize) {
            Ok(data) => data,
            Err(e) => {
                warn!(
                    position = self.position,
                    key_len,
                    error = ?e,
                    "WAL Reader: Failed to read key data"
                );
                return Err(ReadError::Storage(e));
            }
        };

        if key_data.len() != key_len as usize {
            warn!(
                position = self.position,
                expected = key_len,
                actual = key_data.len(),
                "WAL Reader: Partial key data read"
            );
            return Err(ReadError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!(
                    "Partial key data read: expected {} bytes, got {}",
                    key_len,
                    key_data.len()
                ),
            )));
        }
        self.position += key_len;

        // 计算 value 部分长度
        // payload_len = KeyLen(8B) + Key + Value Section
        let value_len = payload_len - WAL_KEY_LEN_SIZE as u64 - key_len;

        // 验证安全限制（来自 GlobalConfig 或默认值）
        let (max_key, max_val) = wal_limits();
        if key_len > max_key || value_len > max_val {
            warn!(
                key_len,
                value_len, max_key, max_val, "Payload size exceeds safety limits"
            );
            return Err(ReadError::PayloadTooLarge {
                key_len,
                val_len: value_len,
                max_key,
                max_val,
            });
        }

        // 4. Value
        let value_data = match self.file.read_at(self.position, value_len as usize) {
            Ok(data) => data,
            Err(e) => {
                warn!(
                    position = self.position,
                    value_len,
                    sequence,
                    error = ?e,
                    "WAL Reader: Failed to read value data"
                );
                return Err(ReadError::Storage(e));
            }
        };

        if value_data.len() != value_len as usize {
            warn!(
                position = self.position,
                expected = value_len,
                actual = value_data.len(),
                "WAL Reader: Partial value data read"
            );
            return Err(ReadError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!(
                    "Partial value data read: expected {} bytes, got {}",
                    value_len,
                    value_data.len()
                ),
            )));
        }
        self.position += value_len;

        // 5. 验证 CRC
        // 重建 CRC 计算以验证数据完整性
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&payload_len.to_be_bytes());
        hasher.update(&[value_type_u8]);
        hasher.update(&sequence.to_be_bytes());
        hasher.update(&key_len.to_be_bytes());
        hasher.update(&key_data);
        hasher.update(&value_data);

        let calculate_crc = hasher.finalize();
        if calculate_crc != header_crc {
            warn!(
                expected = header_crc,
                actual = calculate_crc,
                sequence,
                "CRC checksum mismatch detected"
            );
            return Err(ReadError::CrcMismatch {
                expected: header_crc,
                actual: calculate_crc,
            });
        }

        let key = Bytes::copy_from_slice(&key_data);

        // 通过 ValueType 解码 value body
        let (value, _) = ValueType::decode_with(&value_data, value_type_u8).map_err(|e| {
            warn!(
                sequence,
                value_type = value_type_u8,
                error = ?e,
                "WAL Reader: Failed to decode value"
            );
            ReadError::Decode(format!("{:?}", e))
        })?;

        Ok(Some(Entry {
            key,
            value,
            sequence,
        }))
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
