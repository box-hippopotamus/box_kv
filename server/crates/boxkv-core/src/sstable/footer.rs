use super::SSTableError;
use super::format::BlockHandle;

use boxkv_common::codec::{Decode, Encode};
use bytes::BufMut;

pub const FOOTER_SIZE: usize = 48;
const MAGIC: u64 = 123;
const MAGIC_SIZE: usize = 8;

/// Footer - SSTable 文件尾部元数据
///
/// 固定 48 字节，存储在文件末尾，包含 MetaIndex 和 Index Block 的位置信息。
///
/// 格式：
/// ```text
/// [meta_index_handle (varint)][index_handle (varint)][padding][magic (8 bytes)]
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Footer {
    /// MetaIndex Block 的位置和大小
    pub metaindex_block_handle: BlockHandle,
    /// Index Block 的位置和大小
    pub index_block_handle: BlockHandle,
    /// Magic number，用于验证文件格式
    pub magic: u64,
}

impl Footer {
    /// 创建新的 Footer
    pub fn new(meta_index: BlockHandle, index: BlockHandle) -> Self {
        Self {
            metaindex_block_handle: meta_index,
            index_block_handle: index,
            magic: MAGIC,
        }
    }

    /// 编码 Footer
    ///
    /// # 格式
    /// ```text
    /// +----------------------+-------------------+----------+------------------+
    /// | meta_index_handle    | index_handle      | padding  | magic (8 bytes)  |
    /// | (varint, variable)   | (varint, variable)| (zeros)  | (big-endian u64)|
    /// +----------------------+-------------------+----------+------------------+
    /// ```
    pub fn encode(&self, buf: &mut impl BufMut) -> Result<(), SSTableError> {
        self.metaindex_block_handle.encode_to(buf)?;
        self.index_block_handle.encode_to(buf)?;

        let written =
            self.metaindex_block_handle.encoded_len() + self.index_block_handle.encoded_len();
        let padding_len = FOOTER_SIZE - written - MAGIC_SIZE;

        for _ in 0..padding_len {
            buf.put_u8(0);
        }

        buf.put_u64(MAGIC);
        Ok(())
    }

    /// 解码 Footer
    pub fn decode(data: &[u8]) -> Result<Self, SSTableError> {
        if data.len() != FOOTER_SIZE {
            return Err(SSTableError::Corrupted(format!(
                "Footer size mismatch: expected {}, got {}",
                FOOTER_SIZE,
                data.len()
            )));
        }
        let (metaindex_block_handle, metaindex_read) = BlockHandle::decode_from(data)?;
        let (index_block_handle, _index_read) = BlockHandle::decode_from(&data[metaindex_read..])?;

        let magic_bytes: [u8; MAGIC_SIZE] =
            data[FOOTER_SIZE - MAGIC_SIZE..].try_into().map_err(|_| {
                SSTableError::Corrupted(format!(
                    "Invalid footer magic size: expected {}, got {}",
                    MAGIC_SIZE,
                    data[FOOTER_SIZE - MAGIC_SIZE..].len()
                ))
            })?;
        let magic = u64::from_be_bytes(magic_bytes);

        Ok(Self {
            metaindex_block_handle,
            index_block_handle,
            magic,
        })
    }

    /// 验证 magic number
    pub fn validate_magic(&self) -> bool {
        self.magic == MAGIC
    }
}
