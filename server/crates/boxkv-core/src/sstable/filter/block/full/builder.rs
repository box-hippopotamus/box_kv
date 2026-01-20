use crate::sstable::data_block::InternalKey;
use crate::sstable::filter::bits::FilterBitsBuilder;
use crate::sstable::filter::bits::builder::FilterMetadataParams;
use crate::sstable::filter::block::{FilterBlockBuilder, FinishResult};
use crate::sstable::filter::{FilterError, FilterMetadata};
use crate::sstable::format::BlockHandle;
use bytes::{BufMut, BytesMut};

/// FullFilterBlockBuilder - 构建整个 SSTable 的单个 filter
pub struct FullFilterBlockBuilder {
    /// FilterBitsBuilder（算法层）
    bits_builder: Box<dyn FilterBitsBuilder>,
}

impl FullFilterBlockBuilder {
    /// 创建新的 FullFilterBlockBuilder
    pub fn new(bits_builder: Box<dyn FilterBitsBuilder>) -> Self {
        Self { bits_builder }
    }
}

impl FilterBlockBuilder for FullFilterBlockBuilder {
    fn add(&mut self, key: InternalKey) {
        self.bits_builder.add_key(key.user_key.clone());
    }

    fn finish(&mut self, _last_partition_handle: BlockHandle) -> Result<FinishResult, FilterError> {
        let result = self.bits_builder.finish()?;

        let metadata = match result.metadata_params {
            FilterMetadataParams::Bloom { num_probes } => FilterMetadata::for_bloom(num_probes),
            FilterMetadataParams::Ribbon { seed, num_blocks } => {
                FilterMetadata::for_ribbon(seed, num_blocks)
            }
        };

        let mut buf = BytesMut::new();
        buf.put_slice(&result.filter_bits);
        metadata.encode_to(&mut buf);

        Ok(FinishResult::Complete(buf.freeze()))
    }

    fn is_empty(&self) -> bool {
        // 检查 FilterBitsBuilder 是否为空
        self.bits_builder.estimate_entries_added() == 0
    }

    fn estimate_entries_added(&self) -> usize {
        // 返回 FilterBitsBuilder 估算的 entries 数量
        self.bits_builder.estimate_entries_added()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sstable::data_block::InternalKey;
    use crate::sstable::filter::bits::bloom::BloomFilterBitsBuilder;
    use bytes::Bytes;

    #[test]
    fn test_full_filter_block_builder_new() {
        let bits_builder = Box::new(BloomFilterBitsBuilder::new(10));
        let builder = FullFilterBlockBuilder::new(bits_builder);
        assert_eq!(builder.estimate_entries_added(), 0);
        assert!(builder.is_empty());
    }

    #[test]
    fn test_full_filter_block_builder_add() {
        let bits_builder = Box::new(BloomFilterBitsBuilder::new(10));
        let mut builder = FullFilterBlockBuilder::new(bits_builder);
        builder.add(InternalKey::new(Bytes::from("key1"), 0));
        builder.add(InternalKey::new(Bytes::from("key2"), 0));

        assert_eq!(builder.estimate_entries_added(), 2);
        assert!(!builder.is_empty());
    }

    #[test]
    fn test_full_filter_block_builder_finish() {
        let bits_builder = Box::new(BloomFilterBitsBuilder::new(10));
        let mut builder = FullFilterBlockBuilder::new(bits_builder);
        builder.add(InternalKey::new(Bytes::from("hello"), 0));
        builder.add(InternalKey::new(Bytes::from("world"), 0));

        let result = builder.finish(BlockHandle::new(0, 0));
        assert!(result.is_ok());

        let FinishResult::Complete(data) = result.unwrap() else {
            panic!("Expected Complete");
        };
        // 应该包含 filter_bits + metadata (5 bytes)
        assert!(data.len() >= FilterMetadata::SIZE);
    }

    #[test]
    fn test_full_filter_block_builder_finish_empty() {
        let bits_builder = Box::new(BloomFilterBitsBuilder::new(10));
        let mut builder = FullFilterBlockBuilder::new(bits_builder);

        let result = builder.finish(BlockHandle::new(0, 0));
        assert!(result.is_ok());

        let FinishResult::Complete(data) = result.unwrap() else {
            panic!("Expected Complete");
        };
        // 空 filter 应该只有 metadata (5 bytes)
        assert_eq!(data.len(), FilterMetadata::SIZE);
    }
}
