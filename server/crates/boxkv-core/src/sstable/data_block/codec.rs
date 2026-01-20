use crate::sstable::block::types::BlockCodec;
use crate::sstable::data_block::types::{InternalKey, InternalValue};
use boxkv_common::codec::{DecodeWithContext, Encode};
use boxkv_common::types::ValueTypeError;
use bytes::{BufMut, Bytes, BytesMut};
use thiserror::Error;

/// Sequence 长度（8 字节）
const SEQUENCE_LEN: usize = 8;

/// Type tag 长度（1 字节）
const TYPE_TAG_LEN: usize = 1;

/// Data Block 编解码器
///
/// Key 格式: [user_key][sequence: 8 bytes BE]
/// Value 格式: [type_tag: 1 byte][value_data]
#[derive(Clone, Debug)]
pub struct DataBlockCodec;

impl DataBlockCodec {
    /// 创建新的编解码器
    pub fn new() -> Self {
        Self
    }
}

impl Default for DataBlockCodec {
    fn default() -> Self {
        Self
    }
}

impl BlockCodec for DataBlockCodec {
    type Key = InternalKey;
    type Value = InternalValue;
    type Error = DataBlockError;

    fn encode_key(
        &self,
        key: &Self::Key,
        buf: &mut impl BufMut,
        shared_prefix_len: usize,
    ) -> Result<(), Self::Error> {
        // Key 格式：[user_key][sequence: SEQUENCE_LEN bytes]
        let user_key = &key.user_key;
        let total_len = user_key.len() + SEQUENCE_LEN;

        if shared_prefix_len >= total_len {
            // 完全共享，不写入任何内容
            return Ok(());
        }

        if shared_prefix_len < user_key.len() {
            // 部分或全部 user_key 需要写入
            buf.put_slice(&user_key[shared_prefix_len..]);
            // 写入完整的 sequence
            buf.put_u64(key.sequence);
        } else {
            // user_key 完全共享，只写入部分 sequence
            let sequence_skip = shared_prefix_len - user_key.len();
            let sequence_bytes = key.sequence.to_be_bytes();
            buf.put_slice(&sequence_bytes[sequence_skip..]);
        }

        Ok(())
    }

    fn decode_key(&self, data: &[u8]) -> Result<(Self::Key, usize), Self::Error> {
        // Key 格式：[user_key][sequence: SEQUENCE_LEN bytes]
        if data.len() < SEQUENCE_LEN {
            return Err(DataBlockError::TruncatedData {
                expected: SEQUENCE_LEN,
                actual: data.len(),
            });
        }

        let user_key_len = data.len() - SEQUENCE_LEN;
        let user_key = Bytes::copy_from_slice(&data[..user_key_len]);

        let sequence_bytes: [u8; SEQUENCE_LEN] = data[user_key_len..user_key_len + SEQUENCE_LEN]
            .try_into()
            .map_err(|_| DataBlockError::InvalidSequence)?;
        let sequence = u64::from_be_bytes(sequence_bytes);

        Ok((InternalKey::new(user_key, sequence), data.len()))
    }

    fn decode_key_with_prefix(
        &self,
        prev_key: &Self::Key,
        unshared_data: &[u8],
        shared_len: usize,
    ) -> Result<Self::Key, Self::Error> {
        // 重建完整的 key
        let prev_user_key = &prev_key.user_key;
        let prev_total_len = prev_user_key.len() + SEQUENCE_LEN;

        if shared_len > prev_total_len {
            return Err(DataBlockError::InvalidSharedPrefix {
                shared_len,
                prev_key_len: prev_total_len,
            });
        }

        // 计算当前 key 的总长度
        let current_total_len = shared_len + unshared_data.len();
        if current_total_len < SEQUENCE_LEN {
            return Err(DataBlockError::TruncatedData {
                expected: SEQUENCE_LEN,
                actual: current_total_len,
            });
        }

        let current_user_key_len = current_total_len - SEQUENCE_LEN;

        // 根据 unshared_len 判断是否共享了 sequence
        if unshared_data.len() >= SEQUENCE_LEN {
            // 情况1：unshared_len >= 8，说明只共享了 key，没有共享 sequence
            // unshared_data = [剩余 user_key][完整 sequence]

            let unshared_user_key_len = current_user_key_len - shared_len;
            let mut user_key_buf = BytesMut::with_capacity(current_user_key_len);

            // 调用字节填充，填充 (..shared_len) 范围（从 prev_key）
            prev_key.copy_range_to(0..shared_len, &mut user_key_buf)?;

            // 从 unshared_data 填充剩余 key（不包含 sequence）
            user_key_buf.put_slice(&unshared_data[..unshared_user_key_len]);

            let user_key = user_key_buf.freeze();

            // 单独从 unshared_data 的最后 8 字节提取 sequence
            let sequence_bytes: [u8; SEQUENCE_LEN] = unshared_data
                [unshared_user_key_len..unshared_user_key_len + SEQUENCE_LEN]
                .try_into()
                .map_err(|_| DataBlockError::InvalidSequence)?;
            let sequence = u64::from_be_bytes(sequence_bytes);

            Ok(InternalKey::new(user_key, sequence))
        } else {
            // 情况2：unshared_len < 8，说明 sequence 也被共享了
            let mut user_key_buf = BytesMut::with_capacity(current_user_key_len);

            if current_user_key_len > prev_key.encoded_len() {
                return Err(DataBlockError::InvalidSharedPrefix {
                    shared_len,
                    prev_key_len: prev_total_len,
                });
            }
            prev_key.copy_range_to(0..current_user_key_len, &mut user_key_buf)?;

            let user_key = user_key_buf.freeze();

            let _sequence_shared_len = shared_len - current_user_key_len;
            let _sequence_unshared_len = unshared_data.len();

            // 重建当前 key 的 sequence
            let mut sequence_buf = BytesMut::with_capacity(SEQUENCE_LEN);

            // 从 prev_key 提取被共享的 sequence 部分
            prev_key.copy_range_to(current_user_key_len..shared_len, &mut sequence_buf)?;

            // 从 unshared_data 提取未共享的 sequence 部分
            sequence_buf.put_slice(unshared_data);

            let sequence_bytes: [u8; SEQUENCE_LEN] = sequence_buf
                .freeze()
                .as_ref()
                .try_into()
                .map_err(|_| DataBlockError::InvalidSequence)?;
            let sequence = u64::from_be_bytes(sequence_bytes);

            Ok(InternalKey::new(user_key, sequence))
        }
    }

    fn encode_value(&self, value: &Self::Value, buf: &mut impl BufMut) -> Result<(), Self::Error> {
        // Value 格式：[type_tag: 1 byte][value_data]
        let type_tag = value.tag();
        buf.put_u8(type_tag);

        value
            .encode_to(buf)
            .map_err(DataBlockError::ValueTypeError)?;

        Ok(())
    }

    fn decode_value(
        &self,
        data: &[u8],
        value_len: usize,
    ) -> Result<(Self::Value, usize), Self::Error> {
        // Value 格式：[type_tag: TYPE_TAG_LEN byte][value_data]
        if value_len < TYPE_TAG_LEN {
            return Err(DataBlockError::TruncatedData {
                expected: TYPE_TAG_LEN,
                actual: value_len,
            });
        }

        if data.len() < value_len {
            return Err(DataBlockError::TruncatedData {
                expected: value_len,
                actual: data.len(),
            });
        }

        let type_tag = data[0];
        let value_data = &data[TYPE_TAG_LEN..value_len];

        let (value, consumed) = InternalValue::decode_with(value_data, type_tag)
            .map_err(DataBlockError::ValueTypeError)?;

        Ok((value, consumed + TYPE_TAG_LEN))
    }

    fn encoded_key_len(&self, key: &Self::Key) -> usize {
        key.encoded_len()
    }

    fn encoded_value_len(&self, value: &Self::Value) -> usize {
        TYPE_TAG_LEN + value.encoded_len()
    }

    fn shared_prefix_len(&self, a: &Self::Key, b: &Self::Key) -> usize {
        let max_len = a.encoded_len().min(b.encoded_len());
        for i in 0..max_len {
            if a.get(i) != b.get(i) {
                return i;
            }
        }
        max_len
    }
}

/// Data Block 错误
#[derive(Debug, Error)]
pub enum DataBlockError {
    /// Varint 解码失败
    #[error("Invalid varint")]
    InvalidVarint,

    /// 数据截断
    #[error("Truncated data: expected {expected} bytes, got {actual}")]
    TruncatedData { expected: usize, actual: usize },

    /// 无效的 sequence
    #[error("Invalid sequence")]
    InvalidSequence,

    /// 无效的共享前缀
    #[error("Invalid shared prefix: {shared_len} > {prev_key_len}")]
    InvalidSharedPrefix {
        shared_len: usize,
        prev_key_len: usize,
    },

    /// ValueType 编解码错误
    #[error("ValueType error: {0}")]
    ValueTypeError(#[from] ValueTypeError),

    /// 缓冲区太小
    #[error("Buffer too small: required {required}, available {available}")]
    BufferTooSmall { required: usize, available: usize },

    /// 范围越界
    #[error("Range out of bounds: {range:?}, total length: {total_len}")]
    RangeOutOfBounds {
        range: std::ops::Range<usize>,
        total_len: usize,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use boxkv_common::types::ValueType;
    use bytes::BytesMut;

    fn test_key(user_key: &str, seq: u64) -> InternalKey {
        InternalKey::new(Bytes::copy_from_slice(user_key.as_bytes()), seq)
    }

    fn test_value(data: &[u8]) -> InternalValue {
        ValueType::Normal(Bytes::copy_from_slice(data))
    }

    // ==================== 编解码器基础功能测试 ====================

    #[test]
    fn test_codec_new_and_default() {
        let codec1 = DataBlockCodec::new();
        let codec2 = DataBlockCodec::default();

        // 两者应该行为一致
        let key = test_key("test", 100);
        assert_eq!(codec1.encoded_key_len(&key), codec2.encoded_key_len(&key));
    }

    #[test]
    fn test_encode_decode_key_simple() {
        let codec = DataBlockCodec::new();
        let key = test_key("hello", 12345);

        let mut buf = BytesMut::new();
        codec.encode_key(&key, &mut buf, 0).unwrap();

        let (decoded, consumed) = codec.decode_key(&buf).unwrap();
        assert_eq!(decoded, key);
        assert_eq!(consumed, buf.len());
    }

    #[test]
    fn test_encode_decode_value_simple() {
        let codec = DataBlockCodec::new();
        let value = test_value(b"world");

        let mut buf = BytesMut::new();
        codec.encode_value(&value, &mut buf).unwrap();

        let value_len = buf.len();
        let (decoded, consumed) = codec.decode_value(&buf, value_len).unwrap();
        assert_eq!(decoded, value);
        assert_eq!(consumed, value_len);
    }

    #[test]
    fn test_encode_decode_tombstone() {
        let codec = DataBlockCodec::new();
        let value = ValueType::Tombstone;

        let mut buf = BytesMut::new();
        codec.encode_value(&value, &mut buf).unwrap();

        let (decoded, _) = codec.decode_value(&buf, buf.len()).unwrap();
        assert_eq!(decoded, ValueType::Tombstone);
    }

    // ==================== 前缀压缩测试 ====================

    #[test]
    fn test_shared_prefix_len_no_shared() {
        let codec = DataBlockCodec::new();
        let key1 = test_key("aaa", 100);
        let key2 = test_key("bbb", 100);

        assert_eq!(codec.shared_prefix_len(&key1, &key2), 0);
    }

    #[test]
    fn test_shared_prefix_len_partial_user_key() {
        let codec = DataBlockCodec::new();
        let key1 = test_key("prefix_aaa", 100);
        let key2 = test_key("prefix_bbb", 200);

        // 共享 "prefix_"
        assert_eq!(codec.shared_prefix_len(&key1, &key2), 7);
    }

    #[test]
    fn test_shared_prefix_len_same_user_key_diff_sequence() {
        let codec = DataBlockCodec::new();
        let key1 = test_key("same", 0x0102030405060708);
        let key2 = test_key("same", 0x0102030405060799);

        // user_key 完全相同(4字节) + sequence 前 7 字节相同
        assert_eq!(codec.shared_prefix_len(&key1, &key2), 4 + 7);
    }

    #[test]
    fn test_shared_prefix_len_identical_keys() {
        let codec = DataBlockCodec::new();
        let key1 = test_key("identical", 12345);
        let key2 = test_key("identical", 12345);

        // 完全相同
        assert_eq!(codec.shared_prefix_len(&key1, &key2), key1.encoded_len());
    }

    // ==================== 前缀压缩编码测试 ====================

    #[test]
    fn test_encode_key_with_full_prefix_shared() {
        let codec = DataBlockCodec::new();
        let key = test_key("key", 100);

        let mut buf = BytesMut::new();
        let shared_len = key.encoded_len(); // 完全共享
        codec.encode_key(&key, &mut buf, shared_len).unwrap();

        // 完全共享，不应写入任何数据
        assert_eq!(buf.len(), 0);
    }

    #[test]
    fn test_encode_key_with_partial_user_key_shared() {
        let codec = DataBlockCodec::new();
        let key = test_key("abcde", 12345);

        let mut buf = BytesMut::new();
        let shared_len = 3; // 共享 "abc"
        codec.encode_key(&key, &mut buf, shared_len).unwrap();

        // 应该写入: "de" + 完整 sequence (8 bytes)
        assert_eq!(buf.len(), 2 + 8);
        assert_eq!(&buf[0..2], b"de");
    }

    #[test]
    fn test_encode_key_with_user_key_fully_shared() {
        let codec = DataBlockCodec::new();
        let key = test_key("abc", 0x0102030405060708);

        let mut buf = BytesMut::new();
        let shared_len = 5; // user_key 完全共享(3) + sequence 前 2 字节
        codec.encode_key(&key, &mut buf, shared_len).unwrap();

        // 应该写入: sequence 的后 6 字节
        assert_eq!(buf.len(), 6);
        assert_eq!(&buf[..], &[0x03, 0x04, 0x05, 0x06, 0x07, 0x08]);
    }

    // ==================== 前缀压缩解码测试 ====================

    #[test]
    fn test_decode_key_with_prefix_case1_only_user_key_shared() {
        let codec = DataBlockCodec::new();
        let prev_key = test_key("prefix_old", 999);

        // 当前 key: "prefix_new" + sequence 888
        let shared_len = 7; // "prefix_"
        let mut unshared_data = BytesMut::new();
        unshared_data.put_slice(b"new");
        unshared_data.put_u64(888);

        let decoded = codec
            .decode_key_with_prefix(&prev_key, &unshared_data, shared_len)
            .unwrap();

        assert_eq!(decoded.user_key(), &Bytes::from("prefix_new"));
        assert_eq!(decoded.sequence(), 888);
    }

    #[test]
    fn test_decode_key_with_prefix_case2_sequence_also_shared() {
        let codec = DataBlockCodec::new();
        let prev_key = test_key("abc", 0x0102030405060708);

        // 当前 key: "abc" + 0x01020304050607FF
        // 共享: user_key(3) + sequence 前 7 字节 = 10
        let shared_len = 10;
        let unshared_data = vec![0xFF]; // sequence 最后 1 字节

        let decoded = codec
            .decode_key_with_prefix(&prev_key, &unshared_data, shared_len)
            .unwrap();

        assert_eq!(decoded.user_key(), &Bytes::from("abc"));
        assert_eq!(decoded.sequence(), 0x01020304050607FF);
    }

    #[test]
    fn test_decode_key_with_prefix_empty_user_key() {
        let codec = DataBlockCodec::new();
        let prev_key = test_key("", 0x0102030405060708);

        // 共享前 4 字节的 sequence
        let shared_len = 4;
        let unshared_data = vec![0x05, 0x06, 0x07, 0xFF]; // sequence 后 4 字节

        let decoded = codec
            .decode_key_with_prefix(&prev_key, &unshared_data, shared_len)
            .unwrap();

        assert_eq!(decoded.user_key().len(), 0);
        assert_eq!(decoded.sequence(), 0x01020304050607FF);
    }

    // ==================== 错误情况测试 ====================

    #[test]
    fn test_decode_key_truncated() {
        let codec = DataBlockCodec::new();
        let data = vec![0x01, 0x02]; // 少于 8 字节

        let result = codec.decode_key(&data);
        assert!(matches!(result, Err(DataBlockError::TruncatedData { .. })));
    }

    #[test]
    fn test_decode_value_truncated() {
        let codec = DataBlockCodec::new();
        let data = vec![]; // 空数据

        let result = codec.decode_value(&data, 1);
        assert!(matches!(result, Err(DataBlockError::TruncatedData { .. })));
    }

    #[test]
    fn test_decode_key_with_prefix_invalid_shared_len() {
        let codec = DataBlockCodec::new();
        let prev_key = test_key("abc", 100);
        let shared_len = 999; // 超过 prev_key 长度
        let unshared_data = vec![0x01];

        let result = codec.decode_key_with_prefix(&prev_key, &unshared_data, shared_len);
        assert!(matches!(
            result,
            Err(DataBlockError::InvalidSharedPrefix { .. })
        ));
    }

    // ==================== 编解码一致性测试（往返测试）====================

    #[test]
    fn test_encode_decode_roundtrip_no_compression() {
        let codec = DataBlockCodec::new();
        let original_key = test_key("roundtrip_key", 777);

        let mut buf = BytesMut::new();
        codec.encode_key(&original_key, &mut buf, 0).unwrap();

        let (decoded_key, _) = codec.decode_key(&buf).unwrap();
        assert_eq!(decoded_key, original_key);
    }

    #[test]
    fn test_encode_decode_roundtrip_with_compression() {
        let codec = DataBlockCodec::new();
        let prev_key = test_key("common_prefix_aaa", 100);
        let curr_key = test_key("common_prefix_bbb", 200);

        let shared_len = codec.shared_prefix_len(&prev_key, &curr_key);

        let mut buf = BytesMut::new();
        codec.encode_key(&curr_key, &mut buf, shared_len).unwrap();

        let decoded_key = codec
            .decode_key_with_prefix(&prev_key, &buf, shared_len)
            .unwrap();
        assert_eq!(decoded_key, curr_key);
    }

    #[test]
    fn test_value_roundtrip_with_data() {
        let codec = DataBlockCodec::new();
        let original_value = test_value(b"test_data_12345");

        let mut buf = BytesMut::new();
        codec.encode_value(&original_value, &mut buf).unwrap();

        let (decoded_value, _) = codec.decode_value(&buf, buf.len()).unwrap();
        assert_eq!(decoded_value, original_value);
    }

    // ==================== 性能相关测试 ====================

    #[test]
    fn test_encoded_len_accuracy() {
        let codec = DataBlockCodec::new();
        let key = test_key("length_test", 99999);
        let value = test_value(b"value_data");

        // 测试 encoded_key_len
        let mut key_buf = BytesMut::new();
        codec.encode_key(&key, &mut key_buf, 0).unwrap();
        assert_eq!(codec.encoded_key_len(&key), key_buf.len());

        // 测试 encoded_value_len
        let mut value_buf = BytesMut::new();
        codec.encode_value(&value, &mut value_buf).unwrap();
        assert_eq!(codec.encoded_value_len(&value), value_buf.len());
    }

    // ==================== 边界条件和特殊情况 ====================

    #[test]
    fn test_empty_user_key_encode_decode() {
        let codec = DataBlockCodec::new();
        let key = test_key("", 12345);

        let mut buf = BytesMut::new();
        codec.encode_key(&key, &mut buf, 0).unwrap();

        let (decoded, _) = codec.decode_key(&buf).unwrap();
        assert_eq!(decoded.user_key().len(), 0);
        assert_eq!(decoded.sequence(), 12345);
    }

    #[test]
    fn test_large_user_key() {
        let codec = DataBlockCodec::new();
        let large_data = vec![b'x'; 1024];
        let key = test_key(std::str::from_utf8(&large_data).unwrap(), 99999);

        let mut buf = BytesMut::new();
        codec.encode_key(&key, &mut buf, 0).unwrap();

        let (decoded, _) = codec.decode_key(&buf).unwrap();
        assert_eq!(decoded.user_key().len(), 1024);
        assert_eq!(decoded.sequence(), 99999);
    }

    #[test]
    fn test_max_sequence_value() {
        let codec = DataBlockCodec::new();
        let key = test_key("max_seq", u64::MAX);

        let mut buf = BytesMut::new();
        codec.encode_key(&key, &mut buf, 0).unwrap();

        let (decoded, _) = codec.decode_key(&buf).unwrap();
        assert_eq!(decoded.sequence(), u64::MAX);
    }

    #[test]
    fn test_zero_sequence_value() {
        let codec = DataBlockCodec::new();
        let key = test_key("zero_seq", 0);

        let mut buf = BytesMut::new();
        codec.encode_key(&key, &mut buf, 0).unwrap();

        let (decoded, _) = codec.decode_key(&buf).unwrap();
        assert_eq!(decoded.sequence(), 0);
    }
}
