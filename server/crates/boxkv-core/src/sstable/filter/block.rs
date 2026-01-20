pub mod full;
pub mod key_partitioned;

pub use full::FullFilterBlockBuilder;
pub use key_partitioned::PartitionedFilterBlockBuilder;

use crate::sstable::filter::FilterError;
use crate::sstable::format::BlockHandle;
use bytes::Bytes;

/// Finish 结果枚举
#[derive(Debug)]
pub enum FinishResult {
    /// 还有更多 partitions，返回当前 partition 数据
    Incomplete(Bytes),

    /// 完成，返回最终数据
    Complete(Bytes),
}

/// 统一的 FilterBlockBuilder trait
pub trait FilterBlockBuilder: Send + Sync {
    /// 添加一个 key
    fn add(&mut self, key: crate::sstable::data_block::InternalKey);

    /// 完成构建，生成 FilterBlock
    fn finish(&mut self, last_partition_handle: BlockHandle) -> Result<FinishResult, FilterError>;

    /// 检查是否为空
    fn is_empty(&self) -> bool;

    /// 估算已添加的 entries 数量
    fn estimate_entries_added(&self) -> usize;
}

/// 统一的 FilterBlockReader trait
pub trait FilterBlockReader: Send + Sync {
    /// 检查 key 是否可能在指定 BlockHandle 的 Data Block 中
    fn key_may_match(
        &self,
        key: crate::sstable::data_block::InternalKey,
        block_handle: &BlockHandle,
    ) -> bool;
}
