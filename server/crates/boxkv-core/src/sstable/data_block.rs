//! Data Block - 存储实际的 KV 条目
//!
//! 格式：
//! - Key: user_key + sequence (8 bytes BE)
//! - Value: [type_tag: 1 byte][value_data]

pub mod codec;
pub mod types;

pub use codec::DataBlockCodec;
pub use types::{InternalKey, InternalValue};

use crate::sstable::block;

/// Data Block 构建器
pub type DataBlockBuilder = block::BlockBuilder<DataBlockCodec>;

/// Data Block 读取器
pub type DataBlockReader = block::BlockReader<DataBlockCodec>;

/// Data Block 迭代器
pub type DataBlockIterator<'a> = block::BlockIterator<'a, DataBlockCodec>;
