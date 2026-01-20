pub mod codec;
pub mod types;

pub use codec::FilterPartitionIndexCodec;

// 使用通用 Block
use crate::sstable::block;

/// Filter Partition Index Builder
pub type FilterPartitionIndexBuilder = block::BlockBuilder<FilterPartitionIndexCodec>;

/// Filter Partition Index Reader
pub type FilterPartitionIndexReader = block::BlockReader<FilterPartitionIndexCodec>;

/// Filter Partition Index Iterator
pub type FilterPartitionIndexIterator<'a> = block::BlockIterator<'a, FilterPartitionIndexCodec>;
