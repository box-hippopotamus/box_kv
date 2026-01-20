use crate::sstable::block::types::BlockCodec;
use crate::sstable::format::BlockHandle;
use crate::sstable::meta_index::types::{MetaIndexKey, MetaIndexValue};
use boxkv_common::codec::{Decode, Encode};
use bytes::{BufMut, Bytes};
use thiserror::Error;

/// MetaIndex Block 编解码器
///
/// Key 格式: [name_bytes]
/// Value 格式: [offset: varint][size: varint]
#[derive(Clone, Debug)]
pub struct MetaIndexCodec;

impl MetaIndexCodec {
    /// 创建新的编解码器
    pub fn new() -> Self {
        Self
    }
}

#[derive(Debug, Error)]
pub enum MetaIndexError {
    #[error("BlockHandle decode error: {0}")]
    BlockHandleError(String),

    #[error("Truncated data: expected {expected} bytes, got {actual}")]
    TruncatedData { expected: usize, actual: usize },

    #[error("Invalid shared prefix: shared_len={shared_len}, prev_key_len={prev_key_len}")]
    InvalidSharedPrefix {
        shared_len: usize,
        prev_key_len: usize,
    },
}

impl BlockCodec for MetaIndexCodec {
    type Key = MetaIndexKey;
    type Value = MetaIndexValue;
    type Error = MetaIndexError;

    fn encode_key(
        &self,
        key: &Self::Key,
        buf: &mut impl BufMut,
        shared_prefix_len: usize,
    ) -> Result<(), Self::Error> {
        // Key 格式：[name_bytes]
        let name_bytes = &key.name;

        if shared_prefix_len >= name_bytes.len() {
            // 完全共享，不写入任何内容
            return Ok(());
        }

        // 写入未共享部分
        buf.put_slice(&name_bytes[shared_prefix_len..]);
        Ok(())
    }

    fn decode_key(&self, data: &[u8]) -> Result<(Self::Key, usize), Self::Error> {
        // Key 格式：[name_bytes]
        let name_bytes = Bytes::copy_from_slice(data);
        Ok((MetaIndexKey::from_bytes(name_bytes), data.len()))
    }

    fn decode_key_with_prefix(
        &self,
        prev_key: &Self::Key,
        unshared_data: &[u8],
        shared_len: usize,
    ) -> Result<Self::Key, Self::Error> {
        let prev_name_bytes = &prev_key.name;

        if shared_len > prev_name_bytes.len() {
            return Err(MetaIndexError::InvalidSharedPrefix {
                shared_len,
                prev_key_len: prev_name_bytes.len(),
            });
        }

        // 重建完整的 name bytes
        let mut name_bytes = Vec::with_capacity(shared_len + unshared_data.len());
        name_bytes.extend_from_slice(&prev_name_bytes[..shared_len]);
        name_bytes.extend_from_slice(unshared_data);

        Ok(MetaIndexKey::from_bytes(Bytes::from(name_bytes)))
    }

    fn encode_value(&self, value: &Self::Value, buf: &mut impl BufMut) -> Result<(), Self::Error> {
        // Value 格式：BlockHandle 的 varint 编码
        value
            .encode_to(buf)
            .map_err(|e| MetaIndexError::BlockHandleError(e.to_string()))?;
        Ok(())
    }

    fn decode_value(
        &self,
        data: &[u8],
        value_len: usize,
    ) -> Result<(Self::Value, usize), Self::Error> {
        // Value 格式：[offset: varint][size: varint]
        if data.len() < value_len {
            return Err(MetaIndexError::TruncatedData {
                expected: value_len,
                actual: data.len(),
            });
        }

        let (handle, consumed) = BlockHandle::decode_from(&data[..value_len])
            .map_err(|e| MetaIndexError::BlockHandleError(e.to_string()))?;

        Ok((handle, consumed))
    }

    fn encoded_key_len(&self, key: &Self::Key) -> usize {
        key.encoded_len()
    }

    fn encoded_value_len(&self, value: &Self::Value) -> usize {
        value.encoded_len()
    }

    fn shared_prefix_len(&self, a: &Self::Key, b: &Self::Key) -> usize {
        // 比较 name bytes 的共享前缀长度
        let a_name = &a.name;
        let b_name = &b.name;
        let max_len = a_name.len().min(b_name.len());

        for i in 0..max_len {
            if a_name[i] != b_name[i] {
                return i;
            }
        }

        max_len
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn test_encode_decode_key() {
        let codec = MetaIndexCodec::new();
        let key = MetaIndexKey::new("fullfilter.bloom".to_string());

        // 编码
        let mut buf = BytesMut::new();
        codec.encode_key(&key, &mut buf, 0).unwrap();
        assert_eq!(buf.as_ref(), b"fullfilter.bloom");

        // 解码
        let (decoded_key, consumed) = codec.decode_key(&buf).unwrap();
        assert_eq!(decoded_key.name(), "fullfilter.bloom");
        assert_eq!(consumed, buf.len());
    }

    #[test]
    fn test_encode_decode_key_with_prefix() {
        let codec = MetaIndexCodec::new();
        let prev_key = MetaIndexKey::new("fullfilter.bloom".to_string());
        let key = MetaIndexKey::new("fullfilter.ribbon".to_string());

        // 编码（共享前缀 "fullfilter."）
        let shared_len = "fullfilter.".len();
        let mut buf = BytesMut::new();
        codec.encode_key(&key, &mut buf, shared_len).unwrap();
        assert_eq!(buf.as_ref(), b"ribbon");

        // 解码（使用前缀）
        let decoded_key = codec
            .decode_key_with_prefix(&prev_key, &buf, shared_len)
            .unwrap();
        assert_eq!(decoded_key.name(), "fullfilter.ribbon");
    }

    #[test]
    fn test_encode_decode_value() {
        let codec = MetaIndexCodec::new();
        let handle = BlockHandle::new(1024, 512);

        // 编码
        let mut buf = BytesMut::new();
        codec.encode_value(&handle, &mut buf).unwrap();

        // 解码
        let (decoded_handle, consumed) = codec.decode_value(&buf, buf.len()).unwrap();
        assert_eq!(decoded_handle, handle);
        assert_eq!(consumed, buf.len());
    }

    #[test]
    fn test_encoded_key_len() {
        let codec = MetaIndexCodec::new();
        let key = MetaIndexKey::new("test.meta".to_string());
        assert_eq!(codec.encoded_key_len(&key), 9);
    }

    #[test]
    fn test_encoded_value_len() {
        let codec = MetaIndexCodec::new();
        let handle = BlockHandle::new(1024, 512);
        let encoded_len = codec.encoded_value_len(&handle);

        // 验证编码长度
        let mut buf = BytesMut::new();
        codec.encode_value(&handle, &mut buf).unwrap();
        assert_eq!(encoded_len, buf.len());
    }

    #[test]
    fn test_decode_key_with_invalid_prefix() {
        let codec = MetaIndexCodec::new();
        let prev_key = MetaIndexKey::new("short".to_string());

        let result = codec.decode_key_with_prefix(&prev_key, b"extra", 10);
        assert!(result.is_err());
        match result {
            Err(MetaIndexError::InvalidSharedPrefix {
                shared_len,
                prev_key_len,
            }) => {
                assert_eq!(shared_len, 10);
                assert_eq!(prev_key_len, 5);
            }
            _ => panic!("Expected InvalidSharedPrefix error"),
        }
    }

    #[test]
    fn test_decode_value_truncated() {
        let codec = MetaIndexCodec::new();
        let truncated_data = vec![0u8; 2];

        let result = codec.decode_value(&truncated_data, 10);
        assert!(result.is_err());
        match result {
            Err(MetaIndexError::TruncatedData { expected, actual }) => {
                assert_eq!(expected, 10);
                assert_eq!(actual, 2);
            }
            _ => panic!("Expected TruncatedData error"),
        }
    }
}
