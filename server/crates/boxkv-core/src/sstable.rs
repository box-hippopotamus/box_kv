mod block;
mod builder;
mod compression;
mod context;
mod data_block;
mod filter;
mod footer;
mod format;
mod index_block;
mod iterator;
mod meta_index;
mod reader;

pub use builder::SSTableBuilder;
pub use compression::CompressionType;
pub use context::SSTableContext;
pub use data_block::types::InternalKey;
pub use filter::policy::{
    FilterPolicy, FixedBloomFilterPolicy, FixedRibbonFilterPolicy, LevelBasedFilterPolicy,
};
pub use format::*;
pub use iterator::SSTableIterator;
pub use reader::SSTableReader;

use thiserror::Error;

use crate::sstable::compression::CompressionError;
use boxkv_common::varint::VarintError;
use boxkv_storage::StorageError;

/// SSTable 顶层错误类型
#[derive(Debug, Error)]
pub enum SSTableError {
    /// 标准 I/O 错误
    #[error("I/O error while accessing SSTable: {0}")]
    Io(#[from] std::io::Error),

    /// 抽象存储层错误
    #[error("underlying storage error: {0}")]
    Storage(#[from] StorageError),

    /// 变长整数编码/解码失败
    #[error("varint encode/decode error: {0}")]
    Varint(#[from] VarintError),

    /// 文件内容结构不符合期望
    #[error("invalid SSTable format: {0}")]
    InvalidFormat(String),

    /// 明确的数据损坏场景
    #[error("corrupted SSTable data: {0}")]
    Corrupted(String),

    /// 压缩/解压异常
    #[error("compression error: {0}")]
    Compression(#[from] CompressionError),

    /// 业务层解码失败
    #[error("decode error: {0}")]
    Decode(String),

    /// 预留的其它错误类型
    #[error("SSTable internal error: {0}")]
    Internal(String),
}

/// SSTable 统一 Result 类型别名
pub type Result<T> = std::result::Result<T, SSTableError>;
