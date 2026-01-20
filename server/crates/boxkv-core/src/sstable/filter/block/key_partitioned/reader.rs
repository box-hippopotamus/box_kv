use crate::sstable::data_block::InternalKey;
use crate::sstable::filter::FilterError;
use crate::sstable::filter::FilterMetadata;
use crate::sstable::filter::bits::FilterBitsReader;
use crate::sstable::filter::bits::always_true::AlwaysTrueFilter;
use crate::sstable::filter::bits::bloom::BloomFilterBitsReader;
use crate::sstable::filter::bits::ribbon::RibbonFilterBitsReader;
use crate::sstable::filter::block::FilterBlockReader;
use crate::sstable::filter::block::key_partitioned::index::{
    FilterPartitionIndexCodec, FilterPartitionIndexReader,
};
use crate::sstable::filter::common::marker;
use crate::sstable::format::BlockHandle;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;

/// PartitionedFilterBlockReader - 读取按 key 数量切分的 filter
pub struct PartitionedFilterBlockReader {
    /// Filter Partition Index Reader
    partition_index: FilterPartitionIndexReader,

    /// 缓存的已加载 Filter Partitions
    cached_partitions: SkipMap<u64, Box<dyn FilterBitsReader>>,

    /// Filter Partition 数据加载器
    partition_loader: Box<dyn Fn(&BlockHandle) -> Result<Bytes, FilterError> + Send + Sync>,
}

impl PartitionedFilterBlockReader {
    /// 从 Filter Partition Index 数据创建 PartitionedFilterBlockReader
    pub fn new<F>(index_data: Bytes, partition_loader: F) -> Result<Self, FilterError>
    where
        F: Fn(&BlockHandle) -> Result<Bytes, FilterError> + Send + Sync + 'static,
    {
        // 解析 Filter Partition Index
        let codec = FilterPartitionIndexCodec::new();
        let partition_index = FilterPartitionIndexReader::new(codec, index_data)
            .map_err(|e| FilterError::DecodeError(format!("Failed to decode index: {:?}", e)))?;

        Ok(Self {
            partition_index,
            cached_partitions: SkipMap::new(),
            partition_loader: Box::new(partition_loader),
        })
    }

    /// 获取 Filter Partition Handle
    fn get_filter_partition_handle(
        &self,
        search_key: &InternalKey,
    ) -> Result<Option<BlockHandle>, FilterError> {
        // 在 Filter Partition Index 中查找
        // 使用 Seek 找到 >= search_key 的第一个条目
        let mut iter = self.partition_index.iter();

        // Seek 到 >= search_key 的位置
        iter.seek(search_key)
            .map_err(|e| FilterError::DecodeError(format!("Failed to seek in index: {:?}", e)))?;

        if !iter.valid() {
            // 如果找不到，返回最后一个 partition
            // 先回到第一个，然后遍历到最后一个
            iter.seek_to_first().map_err(|e| {
                FilterError::DecodeError(format!("Failed to seek to first: {:?}", e))
            })?;
            let mut last_handle = None;
            while iter.valid() {
                if let Ok(Some(handle)) = iter.value() {
                    last_handle = Some(handle);
                }
                if iter.next().is_err() {
                    break;
                }
            }
            if let Some(handle) = last_handle {
                return Ok(Some(handle));
            }
            return Ok(None);
        }

        // 返回找到的 filter_handle
        let value = iter
            .value()
            .map_err(|e| FilterError::DecodeError(format!("Failed to get value: {:?}", e)))?;
        Ok(value)
    }

    /// 加载并缓存 Filter Partition
    fn load_filter_partition(&self, handle: &BlockHandle) -> Result<bool, FilterError> {
        // 检查是否已缓存
        if self.cached_partitions.get(&handle.offset).is_some() {
            return Ok(true);
        }

        // 加载 Filter Partition 数据
        let partition_data = (self.partition_loader)(handle)?;

        // 直接解析 filter bits + metadata
        if partition_data.len() < FilterMetadata::SIZE {
            return Err(FilterError::TruncatedData {
                expected: FilterMetadata::SIZE,
                actual: partition_data.len(),
            });
        }

        // 最后 5 字节是 metadata
        let metadata_start = partition_data.len() - FilterMetadata::SIZE;
        let metadata = FilterMetadata::decode_from(&partition_data[metadata_start..])?;

        // 前面的数据是 filter bits
        let filter_bits = &partition_data[..metadata_start];

        // 从 FilterMetadata 获取 filter 类型和参数
        let marker = metadata.implementation_marker;

        // 创建 FilterBitsReader
        let reader: Box<dyn FilterBitsReader> = match marker {
            marker::BLOOM => {
                let num_probes = metadata.bloom_num_probes().unwrap_or(6); // 默认值
                match BloomFilterBitsReader::new(filter_bits, num_probes) {
                    Ok(r) => Box::new(r),
                    Err(_) => {
                        // 如果解析失败，使用 AlwaysTrueFilter
                        Box::new(AlwaysTrueFilter::new())
                    }
                }
            }
            marker::RIBBON => {
                let seed = metadata.ribbon_seed().unwrap_or(0);
                let num_blocks = metadata.ribbon_num_blocks().unwrap_or(0);
                match RibbonFilterBitsReader::new(filter_bits, num_blocks, seed) {
                    Ok(r) => Box::new(r),
                    Err(_) => {
                        // 如果解析失败，使用 AlwaysTrueFilter
                        Box::new(AlwaysTrueFilter::new())
                    }
                }
            }
            _ => {
                // 未知类型：使用 AlwaysTrueFilter
                Box::new(AlwaysTrueFilter::new())
            }
        };

        // 缓存 reader
        self.cached_partitions.insert(handle.offset, reader);

        Ok(true)
    }

    /// 查询 key 是否可能在 Filter Partition 中
    fn query_partition(&self, key: InternalKey, handle: &BlockHandle) -> Result<bool, FilterError> {
        // 加载 Filter Partition
        self.load_filter_partition(handle)?;

        // 从缓存中获取 reader
        if let Some(entry) = self.cached_partitions.get(&handle.offset) {
            return Ok(entry.value().may_match(key.user_key.clone()));
        }

        // 如果无法加载，返回 true
        Ok(true)
    }
}

impl FilterBlockReader for PartitionedFilterBlockReader {
    fn key_may_match(&self, key: InternalKey, _block_handle: &BlockHandle) -> bool {
        let filter_handle = match self.get_filter_partition_handle(&key) {
            Ok(Some(handle)) => handle,
            Ok(None) => {
                // 如果找不到 Filter Partition，返回 true
                return true;
            }
            Err(_) => {
                // 如果查找失败，返回 true
                return true;
            }
        };

        // 查询 Filter Partition
        self.query_partition(key, &filter_handle).unwrap_or(true)
    }
}
