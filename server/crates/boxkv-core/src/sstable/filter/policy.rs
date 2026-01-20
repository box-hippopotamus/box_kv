use crate::sstable::filter::FilterError;
use crate::sstable::filter::FilterMetadata;
use crate::sstable::filter::bits::always_true::AlwaysTrueFilter;
use crate::sstable::filter::bits::bloom::{BloomFilterBitsBuilder, BloomFilterBitsReader};
use crate::sstable::filter::bits::ribbon::{RibbonFilterBitsBuilder, RibbonFilterBitsReader};
use crate::sstable::filter::bits::{FilterBitsBuilder, FilterBitsReader};
use crate::sstable::filter::common::marker;

/// Builtin FilterPolicy 的兼容性名称
pub const BUILTIN_FILTER_COMPATIBILITY_NAME: &str = "boxkv.BuiltinFilter";

/// 构建 filter 时的上下文信息
#[derive(Debug, Clone)]
pub struct FilterBuildingContext {
    /// SSTable 创建时的 LSM level（-1 表示未知）
    pub level_at_creation: i32,
}

impl FilterBuildingContext {
    /// 创建新的 FilterBuildingContext
    pub fn new(level_at_creation: i32) -> Self {
        Self { level_at_creation }
    }
}

/// FilterPolicy trait
pub trait FilterPolicy: Send + Sync {
    /// 获取 policy 的名称（用于配置系统）
    fn name(&self) -> &str;

    /// 创建 FilterBitsBuilder（算法层）
    fn get_bits_builder(&self, _context: &FilterBuildingContext) -> Box<dyn FilterBitsBuilder> {
        // 默认实现：调用不需要 context 的版本
        self.get_bits_builder_without_context()
    }

    /// 创建 FilterBitsBuilder
    fn get_bits_builder_without_context(&self) -> Box<dyn FilterBitsBuilder>;

    /// 创建 FilterBitsReader
    fn get_bits_reader(&self, contents: &[u8]) -> Result<Box<dyn FilterBitsReader>, FilterError> {
        // 分析 metadata 选择对应的 Reader
        if contents.len() < FilterMetadata::SIZE {
            return Err(FilterError::InvalidData(
                "Filter data too short".to_string(),
            ));
        }

        // 读取 FilterMetadata（最后 5 字节）
        let len_with_meta = contents.len();
        let metadata =
            FilterMetadata::decode_from(&contents[len_with_meta - FilterMetadata::SIZE..])?;

        // 分离出 filter_bits
        let filter_bits = &contents[..len_with_meta - FilterMetadata::SIZE];

        match metadata.implementation_marker {
            marker::BLOOM => {
                // Bloom Filter
                // filter_bits 格式：[bits_data]
                let num_probes = metadata.bloom_num_probes().ok_or_else(|| {
                    FilterError::InvalidData("Bloom filter metadata missing num_probes".to_string())
                })?;
                let reader = BloomFilterBitsReader::new(filter_bits, num_probes)?;
                Ok(Box::new(reader))
            }
            marker::RIBBON => {
                // Ribbon Filter
                // filter_bits 格式：[slots: n bytes]
                let seed = metadata.ribbon_seed().ok_or_else(|| {
                    FilterError::InvalidData("Ribbon filter metadata missing seed".to_string())
                })?;
                let num_blocks = metadata.ribbon_num_blocks().ok_or_else(|| {
                    FilterError::InvalidData(
                        "Ribbon filter metadata missing num_blocks".to_string(),
                    )
                })?;
                let reader = RibbonFilterBitsReader::new(filter_bits, num_blocks, seed)?;
                Ok(Box::new(reader))
            }
            _ => Ok(Box::new(AlwaysTrueFilter::new())),
        }
    }

    /// 获取兼容性名称（用于 filter 格式识别）
    fn compatibility_name(&self) -> &str;

    /// 获取每个 key 使用的比特数
    fn bits_per_key(&self) -> usize;

    /// 获取 implementation marker
    fn implementation_marker(&self, _context: &FilterBuildingContext) -> u8 {
        // 默认实现：调用不需要 context 的版本
        self.get_implementation_marker_without_context()
    }

    /// 获取 implementation marker（不需要 context）
    /// - marker 值：1 = Bloom, 2 = Ribbon
    fn get_implementation_marker_without_context(&self) -> u8;
}

/// 固定使用 Bloom Filter 的策略
#[derive(Debug, Clone)]
pub struct FixedBloomFilterPolicy {
    /// 每个 key 使用的比特数
    bits_per_key: usize,
}

impl FixedBloomFilterPolicy {
    /// 创建新的 FixedBloomFilterPolicy
    pub fn new(bits_per_key: usize) -> Self {
        Self { bits_per_key }
    }
}

impl FilterPolicy for FixedBloomFilterPolicy {
    fn name(&self) -> &str {
        "fixed_bloom"
    }

    fn compatibility_name(&self) -> &str {
        BUILTIN_FILTER_COMPATIBILITY_NAME
    }

    fn get_bits_builder_without_context(&self) -> Box<dyn FilterBitsBuilder> {
        Box::new(BloomFilterBitsBuilder::new(self.bits_per_key))
    }

    fn bits_per_key(&self) -> usize {
        self.bits_per_key
    }

    fn get_implementation_marker_without_context(&self) -> u8 {
        marker::BLOOM
    }
}

/// 固定使用 Ribbon Filter 的策略
#[derive(Debug, Clone)]
pub struct FixedRibbonFilterPolicy {
    /// 每个 key 使用的比特数（Bloom 等价）
    bits_per_key: usize,
}

impl FixedRibbonFilterPolicy {
    /// 创建新的 FixedRibbonFilterPolicy
    pub fn new(bits_per_key: usize) -> Self {
        Self { bits_per_key }
    }
}

impl FilterPolicy for FixedRibbonFilterPolicy {
    fn name(&self) -> &str {
        "fixed_ribbon"
    }

    fn compatibility_name(&self) -> &str {
        BUILTIN_FILTER_COMPATIBILITY_NAME
    }

    fn get_bits_builder_without_context(&self) -> Box<dyn FilterBitsBuilder> {
        Box::new(RibbonFilterBitsBuilder::new(self.bits_per_key))
    }

    fn bits_per_key(&self) -> usize {
        self.bits_per_key
    }

    fn get_implementation_marker_without_context(&self) -> u8 {
        marker::RIBBON
    }
}

/// 分层存储策略
///
/// 根据 LSM level 选择算法：
/// - 低 level（如 L0-L2）：使用 FastLocalBloom（写入频繁，需要快速构建）
/// - 高 level（如 L3+）：使用 Standard128Ribbon（查询频繁，需要更好的空间效率）
#[derive(Debug, Clone)]
pub struct LevelBasedFilterPolicy {
    /// 每个 key 使用的比特数
    bits_per_key: usize,
    /// 在此 level 之前使用 Bloom，之后使用 Ribbon
    bloom_before_level: usize,
}

impl LevelBasedFilterPolicy {
    /// 创建新的 LevelBasedFilterPolicy
    pub fn new(bits_per_key: usize, bloom_before_level: usize) -> Self {
        Self {
            bits_per_key,
            bloom_before_level,
        }
    }
}

impl FilterPolicy for LevelBasedFilterPolicy {
    fn name(&self) -> &str {
        "level_based"
    }

    fn compatibility_name(&self) -> &str {
        BUILTIN_FILTER_COMPATIBILITY_NAME
    }

    // 重写 get_bits_builder：根据 context 选择算法
    fn get_bits_builder(&self, context: &FilterBuildingContext) -> Box<dyn FilterBitsBuilder> {
        // 根据 context.level_at_creation 选择算法
        let level = context.level_at_creation;

        if level >= 0 && (level as usize) < self.bloom_before_level {
            // 低 level：使用 Bloom（写入频繁，需要快速构建）
            Box::new(BloomFilterBitsBuilder::new(self.bits_per_key))
        } else {
            // 高 level：使用 Ribbon（查询频繁，需要更好的空间效率）
            Box::new(RibbonFilterBitsBuilder::new(self.bits_per_key))
        }
    }

    fn get_bits_builder_without_context(&self) -> Box<dyn FilterBitsBuilder> {
        Box::new(BloomFilterBitsBuilder::new(self.bits_per_key))
    }

    fn bits_per_key(&self) -> usize {
        self.bits_per_key
    }

    // 根据 context 选择 marker
    fn implementation_marker(&self, context: &FilterBuildingContext) -> u8 {
        // 根据 context.level_at_creation 动态选择 marker
        let level = context.level_at_creation;

        if level >= 0 && (level as usize) < self.bloom_before_level {
            // 低 level：使用 Bloom
            marker::BLOOM
        } else {
            // 高 level：使用 Ribbon
            marker::RIBBON
        }
    }

    // 提供默认实现（使用 Bloom）
    fn get_implementation_marker_without_context(&self) -> u8 {
        marker::BLOOM
    }
}
