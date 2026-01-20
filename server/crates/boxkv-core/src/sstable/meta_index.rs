//! MetaIndex Block - 索引所有 Meta Blocks 的位置

pub mod codec;
pub mod types;

pub use codec::MetaIndexCodec;
pub use types::MetaIndexKey;

use crate::sstable::block;

/// MetaIndex Block 构建器
pub type MetaIndexBuilder = block::BlockBuilder<MetaIndexCodec>;

/// MetaIndex Block 读取器
pub type MetaIndexReader = block::BlockReader<MetaIndexCodec>;

/// MetaIndex Block 迭代器
pub type MetaIndexIterator<'a> = block::BlockIterator<'a, MetaIndexCodec>;
