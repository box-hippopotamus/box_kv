use crate::sstable::block::types::BlockCodec;
use crate::sstable::format::BlockHandle;
use crate::sstable::index_block::types::{IndexKey, IndexValue};
use boxkv_common::codec::{Decode, Encode};
use bytes::{BufMut, Bytes};
use thiserror::Error;

/// Index Block 编解码器
///
/// Key 格式: [user_key]
/// Value 格式: [offset: varint][size: varint]
#[derive(Clone, Debug)]
pub struct IndexBlockCodec;

impl IndexBlockCodec {
    /// 创建新的编解码器
    pub fn new() -> Self {
        Self
    }
}

impl BlockCodec for IndexBlockCodec {
    type Key = IndexKey;
    type Value = IndexValue;
    type Error = IndexBlockError;

    fn encode_key(
        &self,
        key: &Self::Key,
        buf: &mut impl BufMut,
        shared_prefix_len: usize,
    ) -> Result<(), Self::Error> {
        // Key 格式：[user_key]
        let user_key = &key.user_key;

        if shared_prefix_len >= user_key.len() {
            // 完全共享，不写入任何内容
            return Ok(());
        }

        // 写入未共享部分
        buf.put_slice(&user_key[shared_prefix_len..]);
        Ok(())
    }

    fn decode_key(&self, data: &[u8]) -> Result<(Self::Key, usize), Self::Error> {
        // Key 格式：[user_key]
        let user_key = Bytes::copy_from_slice(data);
        Ok((IndexKey::new(user_key), data.len()))
    }

    fn decode_key_with_prefix(
        &self,
        prev_key: &Self::Key,
        unshared_data: &[u8],
        shared_len: usize,
    ) -> Result<Self::Key, Self::Error> {
        let prev_user_key = &prev_key.user_key;

        if shared_len > prev_user_key.len() {
            return Err(IndexBlockError::InvalidSharedPrefix {
                shared_len,
                prev_key_len: prev_user_key.len(),
            });
        }

        // 重建完整的 user_key
        let mut user_key = Vec::with_capacity(shared_len + unshared_data.len());
        user_key.extend_from_slice(&prev_user_key[..shared_len]);
        user_key.extend_from_slice(unshared_data);

        Ok(IndexKey::new(Bytes::from(user_key)))
    }

    fn encode_value(&self, value: &Self::Value, buf: &mut impl BufMut) -> Result<(), Self::Error> {
        // Value 格式：BlockHandle 的 varint 编码
        value
            .encode_to(buf)
            .map_err(|e| IndexBlockError::BlockHandleError(e.to_string()))?;
        Ok(())
    }

    fn decode_value(
        &self,
        data: &[u8],
        value_len: usize,
    ) -> Result<(Self::Value, usize), Self::Error> {
        // Value 格式：[offset: varint][size: varint]
        if data.len() < value_len {
            return Err(IndexBlockError::TruncatedData {
                expected: value_len,
                actual: data.len(),
            });
        }

        let (handle, consumed) = BlockHandle::decode_from(&data[..value_len])
            .map_err(|e| IndexBlockError::BlockHandleError(e.to_string()))?;

        Ok((handle, consumed))
    }

    fn encoded_key_len(&self, key: &Self::Key) -> usize {
        key.encoded_len()
    }

    fn encoded_value_len(&self, value: &Self::Value) -> usize {
        value.encoded_len()
    }

    fn shared_prefix_len(&self, a: &Self::Key, b: &Self::Key) -> usize {
        // 比较 user_key 的字节前缀
        let a_key = &a.user_key;
        let b_key = &b.user_key;
        let max_len = a_key.len().min(b_key.len());

        for i in 0..max_len {
            if a_key[i] != b_key[i] {
                return i;
            }
        }

        max_len
    }
}

/// Index Block 错误
#[derive(Debug, Error)]
pub enum IndexBlockError {
    /// 数据截断
    #[error("Truncated data: expected {expected} bytes, got {actual}")]
    TruncatedData { expected: usize, actual: usize },

    /// 无效的共享前缀
    #[error("Invalid shared prefix: {shared_len} > {prev_key_len}")]
    InvalidSharedPrefix {
        shared_len: usize,
        prev_key_len: usize,
    },

    /// BlockHandle 解码错误
    #[error("BlockHandle decode error: {0}")]
    BlockHandleError(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    fn test_key(user_key: &str) -> IndexKey {
        IndexKey::new(Bytes::copy_from_slice(user_key.as_bytes()))
    }

    fn test_value(offset: u64, size: u64) -> IndexValue {
        BlockHandle::new(offset, size)
    }

    // ==================== 编解码器基础功能测试 ====================

    #[test]
    fn test_codec_new() {
        let codec = IndexBlockCodec::new();

        let key = test_key("test");
        assert_eq!(codec.encoded_key_len(&key), 4);
    }

    #[test]
    fn test_encode_decode_key_simple() {
        let codec = IndexBlockCodec::new();
        let key = test_key("hello");

        let mut buf = BytesMut::new();
        codec.encode_key(&key, &mut buf, 0).unwrap();

        let (decoded, consumed) = codec.decode_key(&buf).unwrap();
        assert_eq!(decoded, key);
        assert_eq!(consumed, buf.len());
    }

    #[test]
    fn test_encode_decode_value_simple() {
        let codec = IndexBlockCodec::new();
        let value = test_value(1024, 4096);

        let mut buf = BytesMut::new();
        codec.encode_value(&value, &mut buf).unwrap();

        let value_len = buf.len();
        let (decoded, consumed) = codec.decode_value(&buf, value_len).unwrap();
        assert_eq!(decoded, value);
        assert_eq!(consumed, value_len);
    }

    // ==================== 前缀压缩测试 ====================

    #[test]
    fn test_shared_prefix_len_no_shared() {
        let codec = IndexBlockCodec::new();
        let key1 = test_key("aaa");
        let key2 = test_key("bbb");

        assert_eq!(codec.shared_prefix_len(&key1, &key2), 0);
    }

    #[test]
    fn test_shared_prefix_len_partial() {
        let codec = IndexBlockCodec::new();
        let key1 = test_key("prefix_aaa");
        let key2 = test_key("prefix_bbb");

        // 共享 "prefix_"
        assert_eq!(codec.shared_prefix_len(&key1, &key2), 7);
    }

    #[test]
    fn test_shared_prefix_len_identical() {
        let codec = IndexBlockCodec::new();
        let key1 = test_key("identical");
        let key2 = test_key("identical");

        assert_eq!(codec.shared_prefix_len(&key1, &key2), 9);
    }

    // ==================== 前缀压缩编码测试 ====================

    #[test]
    fn test_encode_key_with_full_prefix_shared() {
        let codec = IndexBlockCodec::new();
        let key = test_key("key");

        let mut buf = BytesMut::new();
        let shared_len = key.encoded_len();
        codec.encode_key(&key, &mut buf, shared_len).unwrap();

        assert_eq!(buf.len(), 0);
    }

    #[test]
    fn test_encode_key_with_partial_shared() {
        let codec = IndexBlockCodec::new();
        let key = test_key("abcde");

        let mut buf = BytesMut::new();
        let shared_len = 3; // 共享 "abc"
        codec.encode_key(&key, &mut buf, shared_len).unwrap();

        assert_eq!(buf.len(), 2);
        assert_eq!(&buf[..], b"de");
    }

    // ==================== 前缀压缩解码测试 ====================

    #[test]
    fn test_decode_key_with_prefix() {
        let codec = IndexBlockCodec::new();
        let prev_key = test_key("prefix_old");

        let shared_len = 7; // "prefix_"
        let unshared_data = b"new";

        let decoded = codec
            .decode_key_with_prefix(&prev_key, unshared_data, shared_len)
            .unwrap();

        assert_eq!(decoded.user_key(), &Bytes::from("prefix_new"));
    }

    #[test]
    fn test_decode_key_with_prefix_no_shared() {
        let codec = IndexBlockCodec::new();
        let prev_key = test_key("old");

        let shared_len = 0;
        let unshared_data = b"new";

        let decoded = codec
            .decode_key_with_prefix(&prev_key, unshared_data, shared_len)
            .unwrap();

        assert_eq!(decoded.user_key(), &Bytes::from("new"));
    }

    #[test]
    fn test_decode_key_with_prefix_full_shared() {
        let codec = IndexBlockCodec::new();
        let prev_key = test_key("same");

        let shared_len = 4;
        let unshared_data = b"";

        let decoded = codec
            .decode_key_with_prefix(&prev_key, unshared_data, shared_len)
            .unwrap();

        assert_eq!(decoded.user_key(), &Bytes::from("same"));
    }

    // ==================== 错误情况测试 ====================

    #[test]
    fn test_decode_value_truncated() {
        let codec = IndexBlockCodec::new();
        let data = vec![0x01]; // 太短

        let result = codec.decode_value(&data, 10);
        assert!(matches!(result, Err(IndexBlockError::TruncatedData { .. })));
    }

    #[test]
    fn test_decode_key_with_prefix_invalid_shared_len() {
        let codec = IndexBlockCodec::new();
        let prev_key = test_key("abc");
        let shared_len = 999; // 超过 prev_key 长度
        let unshared_data = b"x";

        let result = codec.decode_key_with_prefix(&prev_key, unshared_data, shared_len);
        assert!(matches!(
            result,
            Err(IndexBlockError::InvalidSharedPrefix { .. })
        ));
    }

    // ==================== 编解码一致性测试（往返测试）====================

    #[test]
    fn test_encode_decode_roundtrip_key() {
        let codec = IndexBlockCodec::new();
        let original_key = test_key("roundtrip_key");

        let mut buf = BytesMut::new();
        codec.encode_key(&original_key, &mut buf, 0).unwrap();

        let (decoded_key, _) = codec.decode_key(&buf).unwrap();
        assert_eq!(decoded_key, original_key);
    }

    #[test]
    fn test_encode_decode_roundtrip_with_compression() {
        let codec = IndexBlockCodec::new();
        let prev_key = test_key("common_prefix_aaa");
        let curr_key = test_key("common_prefix_bbb");

        let shared_len = codec.shared_prefix_len(&prev_key, &curr_key);

        let mut buf = BytesMut::new();
        codec.encode_key(&curr_key, &mut buf, shared_len).unwrap();

        let decoded_key = codec
            .decode_key_with_prefix(&prev_key, &buf, shared_len)
            .unwrap();
        assert_eq!(decoded_key, curr_key);
    }

    #[test]
    fn test_value_roundtrip() {
        let codec = IndexBlockCodec::new();
        let original_value = test_value(123456, 789012);

        let mut buf = BytesMut::new();
        codec.encode_value(&original_value, &mut buf).unwrap();

        let (decoded_value, _) = codec.decode_value(&buf, buf.len()).unwrap();
        assert_eq!(decoded_value, original_value);
    }

    // ==================== 边界条件测试 ====================

    #[test]
    fn test_empty_user_key() {
        let codec = IndexBlockCodec::new();
        let key = test_key("");

        let mut buf = BytesMut::new();
        codec.encode_key(&key, &mut buf, 0).unwrap();

        let (decoded, _) = codec.decode_key(&buf).unwrap();
        assert_eq!(decoded.user_key().len(), 0);
    }

    #[test]
    fn test_large_user_key() {
        let codec = IndexBlockCodec::new();
        let large_data = vec![b'x'; 1024];
        let key = test_key(std::str::from_utf8(&large_data).unwrap());

        let mut buf = BytesMut::new();
        codec.encode_key(&key, &mut buf, 0).unwrap();

        let (decoded, _) = codec.decode_key(&buf).unwrap();
        assert_eq!(decoded.user_key().len(), 1024);
    }

    #[test]
    fn test_block_handle_zero_values() {
        let codec = IndexBlockCodec::new();
        let value = test_value(0, 0);

        let mut buf = BytesMut::new();
        codec.encode_value(&value, &mut buf).unwrap();

        let (decoded, _) = codec.decode_value(&buf, buf.len()).unwrap();
        assert_eq!(decoded.offset, 0);
        assert_eq!(decoded.size, 0);
    }

    #[test]
    fn test_block_handle_large_values() {
        let codec = IndexBlockCodec::new();
        let value = test_value(u64::MAX, u64::MAX);

        let mut buf = BytesMut::new();
        codec.encode_value(&value, &mut buf).unwrap();

        let (decoded, _) = codec.decode_value(&buf, buf.len()).unwrap();
        assert_eq!(decoded.offset, u64::MAX);
        assert_eq!(decoded.size, u64::MAX);
    }

    // ==================== 性能相关测试 ====================

    #[test]
    fn test_encoded_len_accuracy() {
        let codec = IndexBlockCodec::new();
        let key = test_key("length_test");
        let value = test_value(12345, 67890);

        let mut key_buf = BytesMut::new();
        codec.encode_key(&key, &mut key_buf, 0).unwrap();
        assert_eq!(codec.encoded_key_len(&key), key_buf.len());

        let mut value_buf = BytesMut::new();
        codec.encode_value(&value, &mut value_buf).unwrap();
        assert_eq!(codec.encoded_value_len(&value), value_buf.len());
    }
}
