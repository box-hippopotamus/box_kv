use boxkv_common::types::ValueType;
use bytes::{BufMut, Bytes};
use std::cmp::Ordering;
use std::ops::Range;

use super::codec::DataBlockError;

const SEQUENCE_LEN: usize = 8;

/// 内部 Key（InternalKey）
///
/// 格式：user_key + sequence (8 bytes BE)
/// 排序：user_key 升序，sequence 降序
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct InternalKey {
    /// 用户 key
    pub user_key: Bytes,

    /// 序列号（全局递增）
    pub sequence: u64,
}

impl InternalKey {
    /// 创建新的 InternalKey
    pub fn new(user_key: Bytes, sequence: u64) -> Self {
        Self { user_key, sequence }
    }

    /// 获取 user_key
    pub fn user_key(&self) -> &Bytes {
        &self.user_key
    }

    /// 获取序列号
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    /// 计算编码后的大小
    pub fn encoded_len(&self) -> usize {
        self.user_key.len() + SEQUENCE_LEN
    }

    /// 根据下标获取字节
    pub fn get(&self, index: usize) -> u8 {
        if index < self.user_key.len() {
            self.user_key[index]
        } else {
            let seq_index = index - self.user_key.len();
            self.sequence.to_be_bytes()[seq_index]
        }
    }

    /// 填充指定范围的字节到缓冲区
    pub fn copy_range_to(
        &self,
        range: Range<usize>,
        buf: &mut impl BufMut,
    ) -> Result<(), DataBlockError> {
        let total_len = self.user_key.len() + SEQUENCE_LEN;
        if range.start >= total_len || range.end > total_len {
            return Err(DataBlockError::RangeOutOfBounds { range, total_len });
        }

        let user_key_len = self.user_key.len();

        if range.end <= user_key_len {
            // 范围完全在 user_key 内
            buf.put_slice(&self.user_key[range]);
        } else if range.start >= user_key_len {
            // 范围完全在 sequence 内
            let seq_start = range.start - user_key_len;
            let seq_end = range.end - user_key_len;
            buf.put_slice(&self.sequence.to_be_bytes()[seq_start..seq_end]);
        } else {
            // 范围跨越 user_key 和 sequence
            // 先填充 user_key 部分
            buf.put_slice(&self.user_key[range.start..]);
            // 再填充 sequence 部分
            let seq_end = range.end - user_key_len;
            buf.put_slice(&self.sequence.to_be_bytes()[..seq_end]);
        }
        Ok(())
    }
}

impl Ord for InternalKey {
    fn cmp(&self, other: &Self) -> Ordering {
        // 1. user_key 升序
        // 2. sequence 降序（新的优先）
        self.user_key
            .cmp(&other.user_key)
            .then_with(|| other.sequence.cmp(&self.sequence))
    }
}

impl PartialOrd for InternalKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// 内部 Value
///
/// 格式：`[type_tag: 1 byte][value_data]`
///
/// 就是 ValueType，但编码时需要在前面加 type_tag
pub type InternalValue = ValueType;

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    // ==================== InternalKey 基础功能测试 ====================

    #[test]
    fn test_internal_key_new() {
        let key = InternalKey::new(Bytes::from("test_key"), 100);
        assert_eq!(key.user_key(), &Bytes::from("test_key"));
        assert_eq!(key.sequence(), 100);
    }

    #[test]
    fn test_internal_key_encoded_len() {
        let key = InternalKey::new(Bytes::from("hello"), 999);
        assert_eq!(key.encoded_len(), 5 + 8); // "hello" + 8 bytes sequence
    }

    #[test]
    fn test_internal_key_get_user_key_part() {
        let key = InternalKey::new(Bytes::from("abc"), 0x0102030405060708);
        // 测试 user_key 部分
        assert_eq!(key.get(0), b'a');
        assert_eq!(key.get(1), b'b');
        assert_eq!(key.get(2), b'c');
    }

    #[test]
    fn test_internal_key_get_sequence_part() {
        let key = InternalKey::new(Bytes::from("k"), 0x0102030405060708);
        // sequence 在大端序中的字节
        assert_eq!(key.get(1), 0x01); // sequence[0]
        assert_eq!(key.get(2), 0x02); // sequence[1]
        assert_eq!(key.get(8), 0x08); // sequence[7]
    }

    #[test]
    #[should_panic(expected = "index out of bounds")]
    fn test_internal_key_get_out_of_bounds() {
        let key = InternalKey::new(Bytes::from("k"), 100);
        let _ = key.get(100); // 超出范围
    }

    // ==================== InternalKey 排序测试 ====================

    #[test]
    fn test_internal_key_ordering_by_user_key() {
        let key1 = InternalKey::new(Bytes::from("aaa"), 100);
        let key2 = InternalKey::new(Bytes::from("bbb"), 50);

        // user_key 优先：aaa < bbb
        assert!(key1 < key2);
        assert_eq!(key1.cmp(&key2), Ordering::Less);
    }

    #[test]
    fn test_internal_key_ordering_by_sequence() {
        let key1 = InternalKey::new(Bytes::from("key"), 200);
        let key2 = InternalKey::new(Bytes::from("key"), 100);

        // user_key 相同，sequence 降序：200 > 100，所以 key1 < key2
        assert!(key1 < key2);
        assert_eq!(key1.cmp(&key2), Ordering::Less);
    }

    #[test]
    fn test_internal_key_ordering_equal() {
        let key1 = InternalKey::new(Bytes::from("key"), 100);
        let key2 = InternalKey::new(Bytes::from("key"), 100);

        assert_eq!(key1, key2);
        assert_eq!(key1.cmp(&key2), Ordering::Equal);
    }

    #[test]
    fn test_internal_key_ordering_complex() {
        let mut keys = vec![
            InternalKey::new(Bytes::from("key2"), 100),
            InternalKey::new(Bytes::from("key1"), 50),
            InternalKey::new(Bytes::from("key1"), 100),
            InternalKey::new(Bytes::from("key2"), 200),
        ];

        keys.sort();

        // 排序后: key1/100, key1/50, key2/200, key2/100
        assert_eq!(keys[0], InternalKey::new(Bytes::from("key1"), 100));
        assert_eq!(keys[1], InternalKey::new(Bytes::from("key1"), 50));
        assert_eq!(keys[2], InternalKey::new(Bytes::from("key2"), 200));
        assert_eq!(keys[3], InternalKey::new(Bytes::from("key2"), 100));
    }

    // ==================== copy_range_to 测试 ====================

    #[test]
    fn test_copy_range_to_user_key_only() {
        let key = InternalKey::new(Bytes::from("hello"), 12345);
        let mut buf = BytesMut::new();

        key.copy_range_to(0..5, &mut buf).unwrap();

        assert_eq!(&buf[..], b"hello");
    }

    #[test]
    fn test_copy_range_to_sequence_only() {
        let key = InternalKey::new(Bytes::from("k"), 0x0102030405060708u64);
        let mut buf = BytesMut::new();

        // 复制 sequence 的前 4 字节
        key.copy_range_to(1..5, &mut buf).unwrap();

        assert_eq!(&buf[..], &[0x01, 0x02, 0x03, 0x04]);
    }

    #[test]
    fn test_copy_range_to_across_boundary() {
        let key = InternalKey::new(Bytes::from("abc"), 0x0102030405060708u64);
        let mut buf = BytesMut::new();

        // 复制从 user_key 最后 1 字节 到 sequence 前 3 字节
        key.copy_range_to(2..6, &mut buf).unwrap();

        // 应该是: 'c', 0x01, 0x02, 0x03
        assert_eq!(&buf[..], &[b'c', 0x01, 0x02, 0x03]);
    }

    #[test]
    fn test_copy_range_to_full() {
        let key = InternalKey::new(Bytes::from("ab"), 0x0001020304050607u64);
        let mut buf = BytesMut::new();

        // 复制完整 key
        key.copy_range_to(0..10, &mut buf).unwrap();

        assert_eq!(buf.len(), 10);
        assert_eq!(&buf[0..2], b"ab");
        assert_eq!(&buf[2..], &[0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07]);
    }

    #[test]
    fn test_copy_range_to_out_of_bounds() {
        let key = InternalKey::new(Bytes::from("k"), 100);
        let mut buf = BytesMut::new();

        let result = key.copy_range_to(0..100, &mut buf);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            DataBlockError::RangeOutOfBounds { .. }
        ));
    }

    #[test]
    fn test_copy_range_to_start_out_of_bounds() {
        let key = InternalKey::new(Bytes::from("k"), 100);
        let mut buf = BytesMut::new();

        let result = key.copy_range_to(100..101, &mut buf);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            DataBlockError::RangeOutOfBounds { .. }
        ));
    }

    // ==================== 边界条件测试 ====================

    #[test]
    fn test_empty_user_key() {
        let key = InternalKey::new(Bytes::new(), 12345);
        assert_eq!(key.encoded_len(), 8); // 只有 sequence
        assert_eq!(key.user_key.len(), 0);
    }

    #[test]
    fn test_large_user_key() {
        let large_key = vec![b'x'; 1024];
        let key = InternalKey::new(Bytes::from(large_key), 99999);
        assert_eq!(key.encoded_len(), 1024 + 8);
    }

    #[test]
    fn test_max_sequence() {
        let key = InternalKey::new(Bytes::from("key"), u64::MAX);
        assert_eq!(key.sequence(), u64::MAX);
    }

    #[test]
    fn test_zero_sequence() {
        let key = InternalKey::new(Bytes::from("key"), 0);
        assert_eq!(key.sequence(), 0);
    }

    // ==================== 克隆和相等性测试 ====================

    #[test]
    fn test_internal_key_clone() {
        let key1 = InternalKey::new(Bytes::from("test"), 100);
        let key2 = key1.clone();

        assert_eq!(key1, key2);
        assert_eq!(key1.user_key(), key2.user_key());
        assert_eq!(key1.sequence(), key2.sequence());
    }

    #[test]
    fn test_internal_key_debug() {
        let key = InternalKey::new(Bytes::from("debug"), 42);
        let debug_str = format!("{:?}", key);
        assert!(debug_str.contains("debug"));
        assert!(debug_str.contains("42"));
    }
}
