use crate::sstable::format::BlockHandle;
use bytes::Bytes;
use std::cmp::Ordering;

/// MetaIndex Key - Meta Block 的名称
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct MetaIndexKey {
    pub name: Bytes,
}

impl MetaIndexKey {
    /// 创建新的 MetaIndexKey
    pub fn new(name: String) -> Self {
        Self {
            name: Bytes::from(name),
        }
    }

    /// 从 Bytes 创建 MetaIndexKey
    pub fn from_bytes(name: Bytes) -> Self {
        Self { name }
    }

    /// 获取 meta block 名称
    pub fn name(&self) -> String {
        String::from_utf8_lossy(&self.name).to_string()
    }

    /// 获取原始 bytes
    pub fn as_bytes(&self) -> &Bytes {
        &self.name
    }

    /// 计算编码后的大小
    pub fn encoded_len(&self) -> usize {
        self.name.len()
    }
}

impl Ord for MetaIndexKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.name.cmp(&other.name)
    }
}

impl PartialOrd for MetaIndexKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// MetaIndex Value - BlockHandle
pub type MetaIndexValue = BlockHandle;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_meta_index_key_new() {
        let key = MetaIndexKey::new("fullfilter.bloom".to_string());
        assert_eq!(key.name(), "fullfilter.bloom");
    }

    #[test]
    fn test_meta_index_key_from_bytes() {
        let bytes = Bytes::from("partitionedfilter.ribbon");
        let key = MetaIndexKey::from_bytes(bytes.clone());
        assert_eq!(key.as_bytes(), &bytes);
    }

    #[test]
    fn test_meta_index_key_encoded_len() {
        let key = MetaIndexKey::new("boxkv.properties".to_string());
        assert_eq!(key.encoded_len(), "boxkv.properties".len());
    }

    #[test]
    fn test_meta_index_key_ordering() {
        let key1 = MetaIndexKey::new("fullfilter.bloom".to_string());
        let key2 = MetaIndexKey::new("partitionedfilter.ribbon".to_string());
        let key3 = MetaIndexKey::new("fullfilter.bloom".to_string());

        assert!(key1 < key2);
        assert_eq!(key1, key3);
        assert_eq!(key1.cmp(&key2), Ordering::Less);
        assert_eq!(key1.cmp(&key3), Ordering::Equal);
    }

    #[test]
    fn test_meta_index_key_clone() {
        let key1 = MetaIndexKey::new("test.meta".to_string());
        let key2 = key1.clone();
        assert_eq!(key1, key2);
    }

    #[test]
    fn test_empty_name() {
        let key = MetaIndexKey::new("".to_string());
        assert_eq!(key.encoded_len(), 0);
    }

    #[test]
    fn test_large_name() {
        let large_name = "x".repeat(1024);
        let key = MetaIndexKey::new(large_name.clone());
        assert_eq!(key.encoded_len(), 1024);
        assert_eq!(key.name(), large_name);
    }
}
