//! Filter 模块
//!
//! 提供三种 Filter Block 模式：
//! - FullFilterBlock: 整个 SSTable 一个 filter
//! - PartitionedFilterBlock: 按 key 数量或 Index Partition 切分

// 算法层
pub mod bits;

// 策略层
pub mod policy;

// 存储层
pub mod block;
pub use block::FilterBlockBuilder;

// 工厂（负责从 MetaIndex 查找和构造 FilterBlockReader）
pub mod factory;

// 哈希函数
pub mod hash;

// 共享类型
mod common;
pub use common::*;
