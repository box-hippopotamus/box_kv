use bytes::{BufMut, Bytes};
use thiserror::Error;

/// 压缩类型
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CompressionType {
    None = 0,
    Snappy = 1,
    Lz4 = 2,
    Zstd = 3,
}

impl CompressionType {
    /// 从 u8 转换为 CompressionType
    pub fn from_u8(value: u8) -> Result<Self, CompressionError> {
        match value {
            0 => Ok(Self::None),
            1 => Ok(Self::Snappy),
            2 => Ok(Self::Lz4),
            3 => Ok(Self::Zstd),
            _ => Err(CompressionError::UnknownType(value)),
        }
    }

    /// 转换为 u8
    pub fn to_u8(self) -> u8 {
        self as u8
    }
}

/// 压缩相关错误
#[derive(Debug, Error)]
pub enum CompressionError {
    #[error("Unknown compression type: {0}")]
    UnknownType(u8),

    #[error("Compression failed: {0}")]
    CompressionFailed(String),

    #[error("Decompression failed: {0}")]
    DecompressionFailed(String),

    #[error("Invalid compressed data")]
    InvalidData,
}

/// 压缩数据
pub fn compress(
    data: Bytes,
    compression_type: CompressionType,
    buf: &mut impl BufMut,
) -> Result<(), CompressionError> {
    match compression_type {
        CompressionType::None => compress_none(data, buf),
        CompressionType::Snappy => compress_snappy(data, buf),
        CompressionType::Lz4 => compress_lz4(data, buf),
        CompressionType::Zstd => compress_zstd(data, buf),
    }
}

/// 解压缩数据
pub fn decompress(
    data: &[u8],
    compression_type: CompressionType,
    buf: &mut impl BufMut,
) -> Result<(), CompressionError> {
    match compression_type {
        CompressionType::None => decompress_none(data, buf),
        CompressionType::Snappy => decompress_snappy(data, buf),
        CompressionType::Lz4 => decompress_lz4(data, buf),
        CompressionType::Zstd => decompress_zstd(data, buf),
    }
}

// ==================== None 压缩（无压缩）====================

fn compress_none(data: Bytes, buf: &mut impl BufMut) -> Result<(), CompressionError> {
    buf.put_slice(&data);
    Ok(())
}

fn decompress_none(data: &[u8], buf: &mut impl BufMut) -> Result<(), CompressionError> {
    buf.put_slice(data);
    Ok(())
}

// ==================== Snappy 压缩 ====================

#[cfg(feature = "snappy")]
fn compress_snappy(data: Bytes, buf: &mut impl BufMut) -> Result<(), CompressionError> {
    use snap::raw::Encoder;

    let mut encoder = Encoder::new();
    let compressed = encoder
        .compress_vec(&data)
        .map_err(|e| CompressionError::CompressionFailed(e.to_string()))?;

    buf.put_slice(&compressed);
    Ok(())
}

#[cfg(feature = "snappy")]
fn decompress_snappy(data: &[u8], buf: &mut impl BufMut) -> Result<(), CompressionError> {
    use snap::raw::Decoder;

    let mut decoder = Decoder::new();
    let decompressed = decoder
        .decompress_vec(data)
        .map_err(|e| CompressionError::DecompressionFailed(e.to_string()))?;

    buf.put_slice(&decompressed);
    Ok(())
}

#[cfg(not(feature = "snappy"))]
fn compress_snappy(_data: Bytes, _buf: &mut impl BufMut) -> Result<(), CompressionError> {
    Err(CompressionError::CompressionFailed(
        "Snappy support not enabled".to_string(),
    ))
}

#[cfg(not(feature = "snappy"))]
fn decompress_snappy(_data: &[u8], _buf: &mut impl BufMut) -> Result<(), CompressionError> {
    Err(CompressionError::DecompressionFailed(
        "Snappy support not enabled".to_string(),
    ))
}

// ==================== LZ4 压缩 ====================

#[cfg(feature = "lz4")]
fn compress_lz4(data: Bytes, buf: &mut impl BufMut) -> Result<(), CompressionError> {
    use lz4::block::compress;

    let compressed = compress(&data, None, false)
        .map_err(|e| CompressionError::CompressionFailed(e.to_string()))?;

    buf.put_slice(&compressed);
    Ok(())
}

#[cfg(feature = "lz4")]
fn decompress_lz4(data: &[u8], buf: &mut impl BufMut) -> Result<(), CompressionError> {
    use lz4::block::decompress;

    let max_size = data.len() * 10;
    let decompressed = decompress(data, Some(max_size as i32))
        .map_err(|e| CompressionError::DecompressionFailed(e.to_string()))?;

    buf.put_slice(&decompressed);
    Ok(())
}

#[cfg(not(feature = "lz4"))]
fn compress_lz4(_data: Bytes, _buf: &mut impl BufMut) -> Result<(), CompressionError> {
    Err(CompressionError::CompressionFailed(
        "LZ4 support not enabled".to_string(),
    ))
}

#[cfg(not(feature = "lz4"))]
fn decompress_lz4(_data: &[u8], _buf: &mut impl BufMut) -> Result<(), CompressionError> {
    Err(CompressionError::DecompressionFailed(
        "LZ4 support not enabled".to_string(),
    ))
}

// ==================== Zstd 压缩 ====================

#[cfg(feature = "zstd")]
fn compress_zstd(data: Bytes, buf: &mut impl BufMut) -> Result<(), CompressionError> {
    use std::io::Write;

    let mut encoder = zstd::Encoder::new(Vec::new(), 3)
        .map_err(|e| CompressionError::CompressionFailed(e.to_string()))?;

    encoder
        .write_all(&data)
        .map_err(|e| CompressionError::CompressionFailed(e.to_string()))?;

    let compressed = encoder
        .finish()
        .map_err(|e| CompressionError::CompressionFailed(e.to_string()))?;

    buf.put_slice(&compressed);
    Ok(())
}

#[cfg(feature = "zstd")]
fn decompress_zstd(data: &[u8], buf: &mut impl BufMut) -> Result<(), CompressionError> {
    let decompressed =
        zstd::decode_all(data).map_err(|e| CompressionError::DecompressionFailed(e.to_string()))?;

    buf.put_slice(&decompressed);
    Ok(())
}

#[cfg(not(feature = "zstd"))]
fn compress_zstd(_data: Bytes, _buf: &mut impl BufMut) -> Result<(), CompressionError> {
    Err(CompressionError::CompressionFailed(
        "Zstd support not enabled".to_string(),
    ))
}

#[cfg(not(feature = "zstd"))]
fn decompress_zstd(_data: &[u8], _buf: &mut impl BufMut) -> Result<(), CompressionError> {
    Err(CompressionError::DecompressionFailed(
        "Zstd support not enabled".to_string(),
    ))
}

// ==================== CRC32 校验 ====================

/// 计算 CRC32C 校验码
pub fn compute_crc32c(data: &[u8]) -> u32 {
    crc32c::crc32c(data)
}

/// 验证 CRC32C 校验码
pub fn verify_crc32c(data: &[u8], expected: u32) -> bool {
    compute_crc32c(data) == expected
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    fn test_data() -> Bytes {
        Bytes::from("Hello, World! This is a test data for compression.")
    }

    // ==================== CompressionType 测试 ====================

    #[test]
    fn test_compression_type_from_u8() {
        assert_eq!(CompressionType::from_u8(0).unwrap(), CompressionType::None);
        assert_eq!(
            CompressionType::from_u8(1).unwrap(),
            CompressionType::Snappy
        );
        assert_eq!(CompressionType::from_u8(2).unwrap(), CompressionType::Lz4);
        assert_eq!(CompressionType::from_u8(3).unwrap(), CompressionType::Zstd);
        assert!(CompressionType::from_u8(99).is_err());
    }

    #[test]
    fn test_compression_type_to_u8() {
        assert_eq!(CompressionType::None.to_u8(), 0);
        assert_eq!(CompressionType::Snappy.to_u8(), 1);
        assert_eq!(CompressionType::Lz4.to_u8(), 2);
        assert_eq!(CompressionType::Zstd.to_u8(), 3);
    }

    // ==================== None 压缩测试 ====================

    #[test]
    fn test_compress_none() {
        let data = test_data();
        let mut buf = BytesMut::new();

        compress(data.clone(), CompressionType::None, &mut buf).unwrap();

        assert_eq!(buf.len(), data.len());
        assert_eq!(&buf[..], &data[..]);
    }

    #[test]
    fn test_decompress_none() {
        let data = test_data();
        let mut buf = BytesMut::new();

        decompress(&data, CompressionType::None, &mut buf).unwrap();

        assert_eq!(buf.len(), data.len());
        assert_eq!(&buf[..], &data[..]);
    }

    #[test]
    fn test_roundtrip_none() {
        let original = test_data();

        // 压缩
        let mut compressed = BytesMut::new();
        compress(original.clone(), CompressionType::None, &mut compressed).unwrap();

        // 解压
        let mut decompressed = BytesMut::new();
        decompress(&compressed, CompressionType::None, &mut decompressed).unwrap();

        assert_eq!(&decompressed[..], &original[..]);
    }

    // ==================== Snappy 压缩测试 ====================

    #[cfg(feature = "snappy")]
    #[test]
    fn test_compress_snappy() {
        let data = test_data();
        let mut buf = BytesMut::new();

        compress(data.clone(), CompressionType::Snappy, &mut buf).unwrap();

        assert!(buf.len() > 0);
    }

    #[cfg(feature = "snappy")]
    #[test]
    fn test_roundtrip_snappy() {
        let original = test_data();

        // 压缩
        let mut compressed = BytesMut::new();
        compress(original.clone(), CompressionType::Snappy, &mut compressed).unwrap();

        // 解压
        let mut decompressed = BytesMut::new();
        decompress(&compressed, CompressionType::Snappy, &mut decompressed).unwrap();

        assert_eq!(&decompressed[..], &original[..]);
    }

    #[cfg(feature = "snappy")]
    #[test]
    fn test_snappy_compression_ratio() {
        // 创建一个更大的、有重复模式的数据
        let data = Bytes::from(vec![b'a'; 1000]);
        let mut compressed = BytesMut::new();

        compress(data.clone(), CompressionType::Snappy, &mut compressed).unwrap();

        // Snappy 应该能压缩重复数据
        assert!(compressed.len() < data.len());
    }

    // ==================== LZ4 压缩测试 ====================

    #[cfg(feature = "lz4")]
    #[test]
    fn test_roundtrip_lz4() {
        let original = test_data();

        // 压缩
        let mut compressed = BytesMut::new();
        compress(original.clone(), CompressionType::Lz4, &mut compressed).unwrap();

        // 解压
        let mut decompressed = BytesMut::new();
        decompress(&compressed, CompressionType::Lz4, &mut decompressed).unwrap();

        assert_eq!(&decompressed[..], &original[..]);
    }

    // ==================== Zstd 压缩测试 ====================

    #[cfg(feature = "zstd")]
    #[test]
    fn test_roundtrip_zstd() {
        let original = test_data();

        // 压缩
        let mut compressed = BytesMut::new();
        compress(original.clone(), CompressionType::Zstd, &mut compressed).unwrap();

        // 解压
        let mut decompressed = BytesMut::new();
        decompress(&compressed, CompressionType::Zstd, &mut decompressed).unwrap();

        assert_eq!(&decompressed[..], &original[..]);
    }

    // ==================== CRC32 测试 ====================

    #[test]
    fn test_compute_crc32c() {
        let data = b"test data";
        let crc = compute_crc32c(data);
        assert_ne!(crc, 0);
    }

    #[test]
    fn test_verify_crc32c() {
        let data = b"test data";
        let crc = compute_crc32c(data);

        assert!(verify_crc32c(data, crc));
        assert!(!verify_crc32c(data, crc + 1));
    }

    #[test]
    fn test_crc32c_consistency() {
        let data = b"consistent data";
        let crc1 = compute_crc32c(data);
        let crc2 = compute_crc32c(data);

        assert_eq!(crc1, crc2);
    }

    #[test]
    fn test_crc32c_different_data() {
        let data1 = b"data1";
        let data2 = b"data2";

        let crc1 = compute_crc32c(data1);
        let crc2 = compute_crc32c(data2);

        assert_ne!(crc1, crc2);
    }

    // ==================== 边界条件测试 ====================

    #[test]
    fn test_empty_data_none() {
        let data = Bytes::new();
        let mut buf = BytesMut::new();

        compress(data.clone(), CompressionType::None, &mut buf).unwrap();
        assert_eq!(buf.len(), 0);

        let mut decompressed = BytesMut::new();
        decompress(&buf, CompressionType::None, &mut decompressed).unwrap();
        assert_eq!(decompressed.len(), 0);
    }

    #[test]
    fn test_large_data() {
        let data = Bytes::from(vec![0u8; 1024 * 1024]); // 1MB
        let mut buf = BytesMut::new();

        compress(data.clone(), CompressionType::None, &mut buf).unwrap();
        assert_eq!(buf.len(), data.len());
    }
}
