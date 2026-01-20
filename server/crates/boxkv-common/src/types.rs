use crate::codec::{DecodeWithContext, Encode};
use bytes::{BufMut, Bytes};
use std::cmp::{Ordering, min};
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::mem::size_of;
use thiserror::Error;

/// ValueType 编解码专属错误类型
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ValueTypeError {
    /// 未知的类型标签
    #[error("unknown value type tag: {0}")]
    UnknownTypeTag(u8),

    /// 缓冲区太短，无法解码
    #[error("buffer too short: expected at least {expected} bytes, got {actual}")]
    BufferTooShort { expected: usize, actual: usize },

    /// 数据格式无效
    #[error("invalid data format: {0}")]
    InvalidFormat(String),
}

/// 调试日志中 key 字符串展示的最大长度
#[allow(dead_code)]
const MAX_KEY_DEBUG_LEN: usize = 64;
/// 调试日志中 value 字符串展示的最大长度
#[allow(dead_code)]
const MAX_VALUE_DEBUG_LEN: usize = 64;

/// 在 WAL 与 SSTable 编解码中的类型标签
pub const NORMAL_VALUE_TYPE: u8 = 0;
pub const TOMBSTONE_VALUE_TYPE: u8 = 1;
pub const EXPIRING_VALUE_TYPE: u8 = 2;

/// LSM 条目中值的类型
///
/// - `Normal`：普通键值
/// - `Tombstone`：删除标记（不含数据）
/// - `Expiring`：带过期时间的值（TTL）
///
/// 序列化：三种变体分别使用标签 0/1/2。
#[derive(Clone, PartialEq)]
#[repr(u8)]
pub enum ValueType {
    /// Standard value containing raw bytes.
    Normal(Bytes) = NORMAL_VALUE_TYPE,

    /// Deletion marker. Indicates the key has been deleted but not yet compacted.
    Tombstone = TOMBSTONE_VALUE_TYPE,

    /// Value with TTL. Contains both data and an expiration timestamp (Unix epoch seconds).
    Expiring {
        data: Bytes,
        expire_at: u64, // Unix timestamp in seconds
    } = EXPIRING_VALUE_TYPE,
}

const VALUE_TOMBSTONE_LEN: usize = 0;
const VALUE_EXPIRING_AT_LEN: usize = size_of::<u64>();

impl ValueType {
    /// 返回用于序列化的类型标签
    ///
    /// 用于 WAL/SSTable 在反序列化时识别变体
    pub fn tag(&self) -> u8 {
        match self {
            ValueType::Normal(_) => NORMAL_VALUE_TYPE,
            ValueType::Tombstone => TOMBSTONE_VALUE_TYPE,
            ValueType::Expiring { .. } => EXPIRING_VALUE_TYPE,
        }
    }

    /// 是否为删除标记
    pub fn is_tombstone(&self) -> bool {
        matches!(self, ValueType::Tombstone)
    }

    /// 是否在给定时间戳 `now` 上已过期
    pub fn is_expired(&self, now: u64) -> bool {
        match self {
            &ValueType::Expiring { expire_at, .. } => expire_at <= now,
            _ => false,
        }
    }

    /// 获取过期时间戳（若无则返回 None）
    pub fn expire_at(&self) -> Option<u64> {
        match self {
            &ValueType::Expiring { expire_at, .. } => Some(expire_at),
            _ => None,
        }
    }

    /// 获取数据内容（Tombstone 返回 None）
    pub fn data(&self) -> Option<Bytes> {
        match self {
            ValueType::Normal(data) => Some(data.clone()),
            ValueType::Expiring { data, .. } => Some(data.clone()),
            ValueType::Tombstone => None,
        }
    }
}

impl Encode for ValueType {
    type CodecError = ValueTypeError;

    fn encode_to(&self, buf: &mut impl BufMut) -> Result<(), Self::CodecError> {
        match self {
            ValueType::Normal(data) => {
                buf.put_slice(data);
                Ok(())
            }
            ValueType::Tombstone => Ok(()), // nothing to write
            ValueType::Expiring { data, expire_at } => {
                buf.put_u64(*expire_at);
                buf.put_slice(data);
                Ok(())
            }
        }
    }

    fn encoded_len(&self) -> usize {
        match self {
            ValueType::Normal(bytes) => bytes.len(),
            ValueType::Tombstone => VALUE_TOMBSTONE_LEN,
            ValueType::Expiring { data, .. } => data.len() + VALUE_EXPIRING_AT_LEN,
        }
    }
}

impl DecodeWithContext for ValueType {
    type CodecError = ValueTypeError;
    type Context = u8;

    fn decode_with(buf: &[u8], tag: Self::Context) -> Result<(Self, usize), Self::CodecError> {
        match tag {
            NORMAL_VALUE_TYPE => Ok((ValueType::Normal(Bytes::copy_from_slice(buf)), buf.len())),
            TOMBSTONE_VALUE_TYPE => Ok((ValueType::Tombstone, buf.len())),
            EXPIRING_VALUE_TYPE => {
                // Body layout: [expire_at: u64 (BE)][data]
                if buf.len() < VALUE_EXPIRING_AT_LEN {
                    return Err(ValueTypeError::BufferTooShort {
                        expected: VALUE_EXPIRING_AT_LEN,
                        actual: buf.len(),
                    });
                }

                let mut ts_buf = [0u8; VALUE_EXPIRING_AT_LEN];
                ts_buf.copy_from_slice(&buf[..VALUE_EXPIRING_AT_LEN]);
                let expire_at = u64::from_be_bytes(ts_buf);

                let data = Bytes::copy_from_slice(&buf[VALUE_EXPIRING_AT_LEN..]);

                Ok((ValueType::Expiring { data, expire_at }, buf.len()))
            }
            other => Err(ValueTypeError::UnknownTypeTag(other)),
        }
    }
}

impl Debug for ValueType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Normal(bytes) => {
                let debug_len = min(bytes.len(), MAX_VALUE_DEBUG_LEN);
                write!(
                    f,
                    "Normal(len={}, data={:?})",
                    bytes.len(),
                    &String::from_utf8_lossy(&bytes[..debug_len])
                )
            }
            Self::Tombstone => write!(f, "Tombstone"),
            Self::Expiring { data, expire_at } => {
                let debug_len = min(data.len(), MAX_VALUE_DEBUG_LEN);
                write!(
                    f,
                    "Expiring(expire_at={}, len={}, data={:?})",
                    expire_at,
                    data.len(),
                    &String::from_utf8_lossy(&data[..debug_len])
                )
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct Entry {
    /// 键，使用 Bytes 避免拷贝
    pub key: Bytes,

    /// 值类型（包含实际数据或标记）
    pub value: ValueType,

    /// 全局序列号，保证写入顺序
    pub sequence: u64,
}

impl Entry {
    /// 创建普通数据条目
    pub fn new(key: Bytes, value: ValueType, sequence: u64) -> Self {
        Self {
            key,
            value,
            sequence,
        }
    }

    /// 创建删除标记条目
    pub fn new_tombstone(key: Bytes, sequence: u64) -> Self {
        Self {
            key,
            value: ValueType::Tombstone,
            sequence,
        }
    }

    /// 创建带TTL的条目
    pub fn new_expiring(key: Bytes, value: Bytes, sequence: u64, expire_at: u64) -> Self {
        Self {
            key,
            value: ValueType::Expiring {
                data: value,
                expire_at,
            },
            sequence,
        }
    }

    /// 创建Normal的条目
    pub fn new_normal(key: Bytes, value: Bytes, sequence: u64) -> Self {
        Self {
            key,
            value: ValueType::Normal(value),
            sequence,
        }
    }

    /// 检查条目是否已过期
    pub fn is_expired(&self, now: u64) -> bool {
        self.value.is_expired(now)
    }

    /// 检查是否为删除标记
    pub fn is_tombstone(&self) -> bool {
        self.value.is_tombstone()
    }

    /// 获取实际数据（如果存在）
    pub fn data(&self) -> Option<Bytes> {
        self.value.data()
    }
}

/// Entry 按 key 排序，相同 key 按 sequence 降序
impl Ord for Entry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key
            .cmp(&other.key)
            .then(other.sequence.cmp(&self.sequence))
    }
}

impl PartialOrd for Entry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Entry {
    fn eq(&self, other: &Self) -> bool {
        // 与 Ord 保持一致：同时比较 Key 与 Seq。
        // 说明：在有效的 LSM 系统中，Seq 全局唯一；若 Seq 相等，则 Key 也应相等。
        // 这里仍检查 Key 以严格遵守 PartialOrd/Ord 契约；
        // 使用短路求值避免 Seq 不等时的多余比较。
        self.sequence == other.sequence && self.key == other.key
    }
}

impl Eq for Entry {}
