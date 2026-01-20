use crate::sstable::filter::FilterError;
use crate::sstable::filter::bits::FilterBitsReader;
use crate::sstable::filter::block::FilterBlockReader;
use crate::sstable::filter::policy::FilterPolicy;
use crate::sstable::format::BlockHandle;

/// FullFilterBlockReader - 读取整个 SSTable 的单个 filter
pub struct FullFilterBlockReader {
    /// FilterBitsReader（算法层）
    bits_reader: Box<dyn FilterBitsReader>,
}

impl FullFilterBlockReader {
    /// 从 FilterBlock 数据创建 FullFilterBlockReader
    pub fn new(policy: &dyn FilterPolicy, data: &[u8]) -> Result<Self, FilterError> {
        let bits_reader = policy.get_bits_reader(data)?;

        Ok(Self { bits_reader })
    }
}

impl FilterBlockReader for FullFilterBlockReader {
    fn key_may_match(
        &self,
        key: crate::sstable::data_block::InternalKey,
        _block_handle: &BlockHandle,
    ) -> bool {
        self.bits_reader.may_match(key.user_key.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sstable::data_block::InternalKey;
    use crate::sstable::filter::block::{
        FilterBlockBuilder, FinishResult, full::FullFilterBlockBuilder,
    };
    use crate::sstable::filter::policy::{FilterBuildingContext, FixedBloomFilterPolicy};
    use crate::sstable::format::BlockHandle;
    use bytes::Bytes;

    #[test]
    fn test_full_filter_block_reader_roundtrip() {
        // 构建 filter
        let policy = FixedBloomFilterPolicy::new(10);
        let context = FilterBuildingContext::new(0);
        let bits_builder = policy.get_bits_builder(&context);

        let mut builder = FullFilterBlockBuilder::new(bits_builder);
        builder.add(InternalKey::new(Bytes::from("key1"), 0));
        builder.add(InternalKey::new(Bytes::from("key2"), 0));
        builder.add(InternalKey::new(Bytes::from("key3"), 0));

        let FinishResult::Complete(data) = builder.finish(BlockHandle::new(0, 0)).unwrap() else {
            panic!("Expected Complete");
        };

        // 读取 filter
        let reader = FullFilterBlockReader::new(&policy, &data).unwrap();

        // 验证添加的 keys 都能找到
        let handle = BlockHandle { offset: 0, size: 0 };
        assert!(reader.key_may_match(InternalKey::new(Bytes::from("key1"), 0), &handle));
        assert!(reader.key_may_match(InternalKey::new(Bytes::from("key2"), 0), &handle));
        assert!(reader.key_may_match(InternalKey::new(Bytes::from("key3"), 0), &handle));
    }

    #[test]
    fn test_full_filter_block_reader_empty_filter() {
        // 构建空 filter
        let policy = FixedBloomFilterPolicy::new(10);
        let context = FilterBuildingContext::new(0);
        let bits_builder = policy.get_bits_builder(&context);

        let mut builder = FullFilterBlockBuilder::new(bits_builder);
        let FinishResult::Complete(data) = builder.finish(BlockHandle::new(0, 0)).unwrap() else {
            panic!("Expected Complete");
        };

        // 读取 filter
        let reader = FullFilterBlockReader::new(&policy, &data).unwrap();

        // 空过滤器应该返回 true（保守策略）
        let handle = BlockHandle { offset: 0, size: 0 };
        assert!(reader.key_may_match(InternalKey::new(Bytes::from("any_key"), 0), &handle));
    }

    #[test]
    fn test_full_filter_block_reader_invalid_data() {
        // 数据太短
        let policy = FixedBloomFilterPolicy::new(10);
        let data = vec![1, 2, 3];

        let result = FullFilterBlockReader::new(&policy, &data);
        assert!(result.is_err());
    }
}
