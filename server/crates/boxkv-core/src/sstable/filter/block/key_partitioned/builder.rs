use crate::sstable::data_block::InternalKey;
use crate::sstable::filter::bits::FilterBitsBuilder;
use crate::sstable::filter::bits::builder::FilterMetadataParams;
use crate::sstable::filter::block::key_partitioned::index::FilterPartitionIndexBuilder;
use crate::sstable::filter::block::key_partitioned::index::FilterPartitionIndexCodec;
use crate::sstable::filter::block::{FilterBlockBuilder, FinishResult};
use crate::sstable::filter::{FilterEntry, FilterError, FilterMetadata};
use crate::sstable::format::BlockHandle;
use bytes::{BufMut, BytesMut};
use tracing;

/// PartitionedFilterBlockBuilder - 按 key 数量切分的 filter
pub struct PartitionedFilterBlockBuilder {
    /// FilterBitsBuilder（算法层，当前正在构建的 filter）
    bits_builder: Box<dyn FilterBitsBuilder>,

    /// 已完成的 Filter Partitions
    filters: Vec<FilterEntry>,

    /// 上一个添加的 InternalKey
    prev_key: Option<InternalKey>,

    /// 每个 partition 的目标 key 数量
    keys_per_partition: u32,

    /// Filter Partition Index Builder
    index_builder: FilterPartitionIndexBuilder,

    /// 是否正在 finish 过程中
    finishing: bool,

    /// 当前正在 finish 的 filter 索引
    finishing_index: usize,
}

impl PartitionedFilterBlockBuilder {
    /// 创建新的 PartitionedFilterBlockBuilder
    pub fn new(
        bits_builder: Box<dyn FilterBitsBuilder>,
        partition_size: u32,
        options: &boxkv_common::config::SSTableConfig,
    ) -> Self {
        let keys_per_partition = bits_builder.approximate_num_entries(partition_size as usize);

        let keys_per_partition = if keys_per_partition == 0 {
            (partition_size / 2).max(1) as usize
        } else {
            keys_per_partition
        };

        let keys_per_partition = keys_per_partition.min(u32::MAX as usize) as u32;

        let codec = FilterPartitionIndexCodec::new();
        let index_builder =
            FilterPartitionIndexBuilder::new(codec, options.filter_index_restart_interval);

        Self {
            bits_builder,
            filters: Vec::new(),
            prev_key: None,
            keys_per_partition,
            index_builder,
            finishing: false,
            finishing_index: 0,
        }
    }

    /// 决定是否切分 Filter Block
    ///
    /// # 返回
    /// - `true`: 需要切分
    /// - `false`: 不需要切分
    ///
    /// # 说明
    /// 在解耦模式下，当当前 filter 的 keys 数量 >= keys_per_partition 时切分
    pub fn decide_cut_filter_block(&self) -> bool {
        // 解耦模式：检查当前 filter 的 keys 数量
        let added = self.bits_builder.estimate_entries_added();
        added >= self.keys_per_partition as usize
    }

    fn cut_filter_block(&mut self, prev_key: InternalKey) -> Result<(), FilterError> {
        // 完成当前 filter
        let result = self.bits_builder.finish()?;

        // 创建 FilterMetadata
        let metadata = match result.metadata_params {
            FilterMetadataParams::Bloom { num_probes } => FilterMetadata::for_bloom(num_probes),
            FilterMetadataParams::Ribbon { seed, num_blocks } => {
                FilterMetadata::for_ribbon(seed, num_blocks)
            }
        };

        // 组合 filter_bits + metadata
        let mut filter_buf = BytesMut::new();
        filter_buf.put_slice(&result.filter_bits);
        metadata.encode_to(&mut filter_buf);
        let filter_data = filter_buf.freeze();

        let separator_key = prev_key;
        let filter_entry = FilterEntry::new(separator_key, filter_data.clone());

        // 保存 filter entry
        self.filters.push(filter_entry);

        Ok(())
    }
}

impl FilterBlockBuilder for PartitionedFilterBlockBuilder {
    fn add(&mut self, key: InternalKey) {
        // 检查是否需要切分 filter block
        let should_cut = {
            let added = self.bits_builder.estimate_entries_added();
            added >= self.keys_per_partition as usize
        };

        if should_cut && let Some(prev_key) = self.prev_key.clone() {
            // 切分 filter block
            if let Err(e) = self.cut_filter_block(prev_key) {
                // 如果切分失败，记录错误但继续添加 key
                tracing::warn!(
                    "Failed to cut filter block: {:?}, continuing to add keys",
                    e
                );
            }
        }

        self.bits_builder.add_key(key.user_key.clone());

        self.prev_key = Some(key);
    }

    fn finish(&mut self, last_partition_handle: BlockHandle) -> Result<FinishResult, FilterError> {
        if !self.finishing {
            // 第一次调用 finish()：完成最后一个 filter
            let prev_key = self.prev_key.clone();
            if let Some(ref prev_key) = prev_key
                && self.bits_builder.estimate_entries_added() > 0
            {
                self.cut_filter_block(prev_key.clone())?;
            }

            // 开始 finish 过程
            self.finishing = true;
            self.finishing_index = 0;
        }

        // 如果还有未返回的 filter partition，返回下一个
        if self.finishing_index < self.filters.len() {
            // 如果 finishing_index > 0，说明这是后续调用，需要将上一个 partition 的 handle 添加到 index
            if self.finishing_index > 0 {
                // 将上一个 partition 的 handle 添加到 index
                let prev_entry = &self.filters[self.finishing_index - 1];
                self.index_builder
                    .add(&prev_entry.separator_key, &last_partition_handle)
                    .map_err(|e| {
                        FilterError::EncodeError(format!("Failed to add to index: {:?}", e))
                    })?;
            }

            // 获取当前 partition 的数据
            let entry = &self.filters[self.finishing_index];

            // 移动到下一个 partition
            self.finishing_index += 1;

            // 返回 Filter Partition 数据
            return Ok(FinishResult::Incomplete(entry.filter_data.clone()));
        }

        // 所有 filter partitions 都已返回，但还需要将最后一个 partition 的 handle 添加到 index
        if !self.filters.is_empty() {
            let last_entry = &self.filters[self.filters.len() - 1];
            self.index_builder
                .add(&last_entry.separator_key, &last_partition_handle)
                .map_err(|e| {
                    FilterError::EncodeError(format!("Failed to add to index: {:?}", e))
                })?;
        }

        // 所有 filter partitions 都已返回，现在返回 Filter Partition Index
        // 完成 index 构建
        let index_data = self
            .index_builder
            .finish()
            .map_err(|e| FilterError::EncodeError(format!("Failed to finish index: {:?}", e)))?;

        // 返回 Filter Partition Index 数据（完成）
        Ok(FinishResult::Complete(index_data))
    }

    fn is_empty(&self) -> bool {
        // 检查是否为空：没有已完成的 filter 且当前 filter 也为空
        self.filters.is_empty() && self.bits_builder.estimate_entries_added() == 0
    }

    fn estimate_entries_added(&self) -> usize {
        // 返回所有已完成的 filter 的 entries 数量 + 当前 filter 的 entries 数量
        let completed = self.filters.len() * self.keys_per_partition as usize;
        let current = self.bits_builder.estimate_entries_added();
        completed + current
    }
}
