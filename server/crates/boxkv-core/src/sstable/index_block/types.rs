use crate::sstable::format::BlockHandle;
use bytes::Bytes;
use std::cmp::Ordering;

/// Index Key - 普通的 user key
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct IndexKey {
    /// 用户 key
    pub user_key: Bytes,
}

impl IndexKey {
    /// 创建新的 IndexKey
    pub fn new(user_key: Bytes) -> Self {
        Self { user_key }
    }

    /// 获取 user_key
    pub fn user_key(&self) -> &Bytes {
        &self.user_key
    }

    /// 计算编码后的大小
    pub fn encoded_len(&self) -> usize {
        self.user_key.len()
    }
}

impl Ord for IndexKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.user_key.cmp(&other.user_key)
    }
}

impl PartialOrd for IndexKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Index Value - BlockHandle
pub type IndexValue = BlockHandle;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_key_new() {
        let key = IndexKey::new(Bytes::from("test_key"));
        assert_eq!(key.user_key(), &Bytes::from("test_key"));
    }

    #[test]
    fn test_index_key_encoded_len() {
        let key = IndexKey::new(Bytes::from("hello"));
        assert_eq!(key.encoded_len(), 5);
    }

    #[test]
    fn test_index_key_ordering() {
        let key1 = IndexKey::new(Bytes::from("aaa"));
        let key2 = IndexKey::new(Bytes::from("bbb"));
        let key3 = IndexKey::new(Bytes::from("aaa"));

        assert!(key1 < key2);
        assert_eq!(key1, key3);
        assert_eq!(key1.cmp(&key2), Ordering::Less);
        assert_eq!(key1.cmp(&key3), Ordering::Equal);
    }

    #[test]
    fn test_index_key_clone() {
        let key1 = IndexKey::new(Bytes::from("test"));
        let key2 = key1.clone();
        assert_eq!(key1, key2);
    }

    #[test]
    fn test_empty_user_key() {
        let key = IndexKey::new(Bytes::new());
        assert_eq!(key.encoded_len(), 0);
    }

    #[test]
    fn test_large_user_key() {
        let large_key = vec![b'x'; 1024];
        let key = IndexKey::new(Bytes::from(large_key));
        assert_eq!(key.encoded_len(), 1024);
    }
}
