use crate::sstable::filter::FilterError;
use bytes::Bytes;

/// Filter Metadata 参数
#[derive(Debug, Clone)]
pub enum FilterMetadataParams {
    /// Bloom Filter 参数
    Bloom {
        /// 哈希函数数量（k值，1-30）
        num_probes: u8,
    },
    /// Ribbon Filter 参数
    Ribbon {
        /// 哈希种子
        seed: u8,
        /// Block 数量（24 位）
        num_blocks: u32,
    },
}

/// FilterBitsBuilder::finish() 的返回结果
#[derive(Debug)]
pub struct FilterBitsResult {
    /// Filter bits 数据
    pub filter_bits: Bytes,

    /// Metadata 参数
    pub metadata_params: FilterMetadataParams,
}

/// FilterBitsBuilder trait
pub trait FilterBitsBuilder: Send + Sync {
    /// 添加一个 key
    fn add_key(&mut self, key: Bytes);

    /// 完成构建
    fn finish(&mut self) -> Result<FilterBitsResult, FilterError>;

    /// 估算已添加的 entries 数量
    fn estimate_entries_added(&self) -> usize;

    /// 计算所需空间
    fn calculate_space(&self, num_entries: usize) -> usize;

    /// 估算误判率
    fn estimated_fp_rate(&self, num_entries: usize, bytes: usize) -> f64;

    /// 估算在给定字节数（包含 metadata）下可以存储多少个 entries
    fn approximate_num_entries(&self, bytes: usize) -> usize;
}
