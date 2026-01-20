//! Index Block - 索引 Data Block 的位置
//!
//! 格式：
//! - Key: user_key（不包含 sequence）
//! - Value: BlockHandle（Data Block 的位置和大小）

pub mod codec;
pub mod types;

pub use codec::IndexBlockCodec;
pub use types::IndexKey;

use crate::sstable::block;

/// Index Block 构建器
pub type IndexBlockBuilder = block::BlockBuilder<IndexBlockCodec>;

/// Index Block 读取器
pub type IndexBlockReader = block::BlockReader<IndexBlockCodec>;

/// Index Block 迭代器
pub type IndexBlockIterator<'a> = block::BlockIterator<'a, IndexBlockCodec>;
