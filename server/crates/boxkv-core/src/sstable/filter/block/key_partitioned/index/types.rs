use crate::sstable::data_block::InternalKey;
use crate::sstable::format::BlockHandle;

pub type FilterPartitionIndexKey = InternalKey;

pub type FilterPartitionIndexValue = BlockHandle;
