use bytes::{BufMut, Bytes};

/// Filter Implementation Marker 常量
pub mod marker {
    /// Bloom Filter marker
    pub const BLOOM: u8 = 1;

    /// Ribbon Filter marker
    pub const RIBBON: u8 = 2;
}

/// Filter Metadata
///
/// 格式：5 字节
/// ```text
/// [implementation_marker: 1 byte][metadata_bytes: 4 bytes]
/// ```
///
/// - `implementation_marker`: 标识 filter 算法类型（1 = Bloom, 2 = Ribbon）
/// - `metadata_bytes`: 算法特定的元数据
///   - Bloom Filter: [num_probes: 1 byte][reserved: 3 bytes]
///   - Ribbon Filter: [seed: 1 byte][num_blocks: 3 bytes (24-bit, big-endian)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FilterMetadata {
    /// Implementation marker（标识 filter 实现类型）
    pub implementation_marker: u8,

    /// 算法特定的元数据（4 bytes）
    pub metadata_bytes: [u8; 4],
}

impl FilterMetadata {
    /// Metadata 总大小（5 bytes）
    pub const SIZE: usize = 5;

    /// 创建新的 FilterMetadata
    pub fn new(implementation_marker: u8, metadata_bytes: [u8; 4]) -> Self {
        Self {
            implementation_marker,
            metadata_bytes,
        }
    }

    /// 为 Bloom Filter 创建 FilterMetadata
    pub fn for_bloom(num_probes: u8) -> Self {
        Self {
            implementation_marker: marker::BLOOM,
            metadata_bytes: [num_probes, 0, 0, 0],
        }
    }

    /// 为 Ribbon Filter 创建 FilterMetadata
    pub fn for_ribbon(seed: u8, num_blocks: u32) -> Self {
        // num_blocks 存储在 3 字节中（24 位，大端序）
        let num_blocks_bytes = [
            ((num_blocks >> 16) & 0xFF) as u8, // 最高 8 位
            ((num_blocks >> 8) & 0xFF) as u8,  // 中间 8 位
            (num_blocks & 0xFF) as u8,         // 最低 8 位
        ];

        Self {
            implementation_marker: marker::RIBBON,
            metadata_bytes: [
                seed,
                num_blocks_bytes[0],
                num_blocks_bytes[1],
                num_blocks_bytes[2],
            ],
        }
    }

    /// 获取 Bloom Filter 的 num_probes（k值）
    pub fn bloom_num_probes(&self) -> Option<u8> {
        if self.implementation_marker == marker::BLOOM {
            Some(self.metadata_bytes[0])
        } else {
            None
        }
    }

    /// 获取 Ribbon Filter 的 seed
    pub fn ribbon_seed(&self) -> Option<u8> {
        if self.implementation_marker == marker::RIBBON {
            Some(self.metadata_bytes[0])
        } else {
            None
        }
    }

    /// 获取 Ribbon Filter 的 num_blocks
    pub fn ribbon_num_blocks(&self) -> Option<u32> {
        if self.implementation_marker == marker::RIBBON {
            let num_blocks = ((self.metadata_bytes[1] as u32) << 16)
                | ((self.metadata_bytes[2] as u32) << 8)
                | (self.metadata_bytes[3] as u32);
            Some(num_blocks)
        } else {
            None
        }
    }

    /// 编码到缓冲区
    ///
    /// # 格式
    /// ```text
    /// [implementation_marker: 1 byte][metadata_bytes: 4 bytes]
    /// ```
    pub fn encode_to(&self, buf: &mut impl BufMut) {
        buf.put_u8(self.implementation_marker);
        buf.put_slice(&self.metadata_bytes);
    }

    /// 从字节解码
    pub fn decode_from(data: &[u8]) -> Result<Self, FilterError> {
        if data.len() < Self::SIZE {
            return Err(FilterError::TruncatedData {
                expected: Self::SIZE,
                actual: data.len(),
            });
        }

        let implementation_marker = data[0];
        let mut metadata_bytes = [0u8; 4];
        metadata_bytes.copy_from_slice(&data[1..5]);

        Ok(Self {
            implementation_marker,
            metadata_bytes,
        })
    }
}

/// Filter Entry（用于 PartitionedFilterBlock 构建过程）
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FilterEntry {
    /// 用于构建 Filter Partition Index 的 key（separator key）
    pub separator_key: crate::sstable::data_block::InternalKey,

    /// Filter 数据（该 partition 的 filter bits + metadata）
    pub filter_data: Bytes,
}

impl FilterEntry {
    /// 创建新的 FilterEntry
    pub fn new(separator_key: crate::sstable::data_block::InternalKey, filter_data: Bytes) -> Self {
        Self {
            separator_key,
            filter_data,
        }
    }
}

/// Filter 错误类型
#[derive(Debug, thiserror::Error)]
pub enum FilterError {
    #[error("Filter encode error: {0}")]
    EncodeError(String),

    #[error("Filter decode error: {0}")]
    DecodeError(String),

    #[error("Invalid filter data: {0}")]
    InvalidData(String),

    #[error("Truncated data: expected {expected} bytes, got {actual}")]
    TruncatedData { expected: usize, actual: usize },

    #[error("Unsupported filter type: {0}")]
    Unsupported(String),
}
