use std::result::Result;

use bytes::BufMut;

use crate::sstable::SSTableError;
use crate::sstable::compression::CompressionType;
use boxkv_common::codec::{Decode, Encode};
use boxkv_common::varint;

/// BlockHandle - Block 在文件中的位置和大小
///
/// offset 和 size 均使用 varint 编码
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockHandle {
    pub offset: u64,
    pub size: u64,
}

impl BlockHandle {
    pub fn new(offset: u64, size: u64) -> Self {
        Self { offset, size }
    }
}

impl Encode for BlockHandle {
    type CodecError = SSTableError;

    fn encode_to(&self, buf: &mut impl BufMut) -> Result<(), Self::CodecError> {
        varint::encode(self.offset, buf);
        varint::encode(self.size, buf);
        Ok(())
    }

    fn encoded_len(&self) -> usize {
        varint::encoded_len(self.offset) + varint::encoded_len(self.size)
    }
}

impl Decode for BlockHandle {
    type CodecError = SSTableError;

    fn decode_from(buf: &[u8]) -> Result<(Self, usize), Self::CodecError> {
        let (offset, offset_read) = varint::decode(buf)?;
        let (size, size_read) = varint::decode(&buf[offset_read..])?;
        Ok((Self { offset, size }, offset_read + size_read))
    }
}

/// Block 尾部信息（附加在每个压缩 Block 后面）
///
/// 格式：
/// ```text
/// [compressed_data][compression_type: 1 byte][crc32: 4 bytes]
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockTrailer {
    /// 压缩类型
    pub compression_type: CompressionType,
    /// CRC32 校验码（校验 compressed_data + compression_type）
    pub crc32: u32,
}

impl BlockTrailer {
    /// BlockTrailer 的固定大小：1 字节压缩类型 + 4 字节 CRC32
    pub const SIZE: usize = 5;

    /// 创建新的 BlockTrailer
    pub fn new(compression_type: CompressionType, crc32: u32) -> Self {
        Self {
            compression_type,
            crc32,
        }
    }
}

impl Encode for BlockTrailer {
    type CodecError = SSTableError;

    fn encode_to(&self, buf: &mut impl BufMut) -> Result<(), Self::CodecError> {
        buf.put_u8(self.compression_type.to_u8());
        buf.put_u32_le(self.crc32);
        Ok(())
    }

    fn encoded_len(&self) -> usize {
        Self::SIZE
    }
}

impl Decode for BlockTrailer {
    type CodecError = SSTableError;

    fn decode_from(buf: &[u8]) -> Result<(Self, usize), Self::CodecError> {
        if buf.len() < Self::SIZE {
            return Err(SSTableError::Corrupted(format!(
                "BlockTrailer too short: expected {} bytes, got {}",
                Self::SIZE,
                buf.len()
            )));
        }

        let compression_type = CompressionType::from_u8(buf[0])?;
        let crc32 = u32::from_le_bytes([buf[1], buf[2], buf[3], buf[4]]);

        Ok((
            Self {
                compression_type,
                crc32,
            },
            Self::SIZE,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    // BlockHandle 测试

    #[test]
    fn test_block_handle_encode_decode() {
        let handle = BlockHandle::new(1024, 4096);
        let mut buf = BytesMut::new();

        handle.encode_to(&mut buf).unwrap();
        let (decoded, consumed) = BlockHandle::decode_from(&buf).unwrap();

        assert_eq!(decoded.offset, 1024);
        assert_eq!(decoded.size, 4096);
        assert_eq!(consumed, buf.len());
    }

    #[test]
    fn test_block_handle_encoded_len() {
        let handle = BlockHandle::new(100, 200);
        let mut buf = BytesMut::new();

        handle.encode_to(&mut buf).unwrap();

        assert_eq!(handle.encoded_len(), buf.len());
    }

    // BlockTrailer 测试

    #[test]
    fn test_block_trailer_new() {
        let trailer = BlockTrailer::new(CompressionType::Snappy, 0x12345678);

        assert_eq!(trailer.compression_type, CompressionType::Snappy);
        assert_eq!(trailer.crc32, 0x12345678);
    }

    #[test]
    fn test_block_trailer_size() {
        assert_eq!(BlockTrailer::SIZE, 5);
    }

    #[test]
    fn test_block_trailer_encode_decode() {
        let trailer = BlockTrailer::new(CompressionType::Snappy, 0xDEADBEEF);
        let mut buf = BytesMut::new();

        trailer.encode_to(&mut buf).unwrap();

        assert_eq!(buf.len(), BlockTrailer::SIZE);

        let (decoded, consumed) = BlockTrailer::decode_from(&buf).unwrap();
        assert_eq!(decoded.compression_type, CompressionType::Snappy);
        assert_eq!(decoded.crc32, 0xDEADBEEF);
        assert_eq!(consumed, BlockTrailer::SIZE);
    }

    #[test]
    fn test_block_trailer_all_compression_types() {
        let types = [
            CompressionType::None,
            CompressionType::Snappy,
            CompressionType::Lz4,
            CompressionType::Zstd,
        ];

        for ct in types {
            let trailer = BlockTrailer::new(ct, 0x11223344);
            let mut buf = BytesMut::new();

            trailer.encode_to(&mut buf).unwrap();
            let (decoded, _) = BlockTrailer::decode_from(&buf).unwrap();

            assert_eq!(decoded.compression_type, ct);
            assert_eq!(decoded.crc32, 0x11223344);
        }
    }

    #[test]
    fn test_block_trailer_decode_too_short() {
        let data = vec![0x01, 0x02, 0x03];
        let result = BlockTrailer::decode_from(&data);

        assert!(result.is_err());
        assert!(matches!(result, Err(SSTableError::Corrupted(_))));
    }

    #[test]
    fn test_block_trailer_decode_invalid_compression_type() {
        let data = vec![0xFF, 0x00, 0x00, 0x00, 0x00];
        let result = BlockTrailer::decode_from(&data);

        assert!(result.is_err());
    }

    #[test]
    fn test_block_trailer_zero_crc() {
        let trailer = BlockTrailer::new(CompressionType::None, 0);
        let mut buf = BytesMut::new();

        trailer.encode_to(&mut buf).unwrap();
        let (decoded, _) = BlockTrailer::decode_from(&buf).unwrap();

        assert_eq!(decoded.crc32, 0);
    }

    #[test]
    fn test_block_trailer_max_crc() {
        let trailer = BlockTrailer::new(CompressionType::None, u32::MAX);
        let mut buf = BytesMut::new();

        trailer.encode_to(&mut buf).unwrap();
        let (decoded, _) = BlockTrailer::decode_from(&buf).unwrap();

        assert_eq!(decoded.crc32, u32::MAX);
    }
}
