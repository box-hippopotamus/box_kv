//! Filter 工厂
//!
//! 负责 FilterBlock 在 MetaIndex 中的 key 约定，以及 “查找 → 读取 → 构造 reader” 的集中入口。
//! SSTableReader 仅依赖该入口，不直接感知具体 FilterBlock 形式与解析细节。

use crate::sstable::{
    FilterPolicy, Result, SSTableError,
    filter::{
        FilterError,
        block::{
            FilterBlockReader, full::FullFilterBlockReader,
            key_partitioned::PartitionedFilterBlockReader,
        },
    },
    format::BlockHandle,
    meta_index::{MetaIndexKey, MetaIndexReader},
};
use boxkv_common::config::{FilterBlockType, SSTableConfig};
use bytes::Bytes;

/// FilterBlock 的 MetaIndex key 生成器。
///
/// key 约定集中在此处，避免 builder/reader 两侧分别拼接导致不一致：
/// - Full: `fullfilter.{policy_name}`
/// - Partitioned: `partitionedfilter.{policy_name}`
pub fn meta_key_for(policy_name: &str, filter_type: FilterBlockType) -> Result<String> {
    match filter_type {
        FilterBlockType::Full => Ok(format!("fullfilter.{}", policy_name)),
        FilterBlockType::Partitioned => Ok(format!("partitionedfilter.{}", policy_name)),
    }
}

/// 在 MetaIndex 中查找 FilterBlock 的 handle。
///
/// 查找顺序体现偏好：Full 优先，其次 Partitioned；命中即返回首个匹配项。
pub fn find_filter_handle(
    meta: &MetaIndexReader,
    policy_name: &str,
) -> Result<Option<(FilterBlockType, BlockHandle)>> {
    let candidates = [FilterBlockType::Full, FilterBlockType::Partitioned];

    for filter_type in candidates.iter() {
        let key = meta_key_for(policy_name, *filter_type)?;
        let meta_key = MetaIndexKey::new(key);

        match meta.get(&meta_key) {
            Ok(Some(handle)) => return Ok(Some((*filter_type, handle))),
            Ok(None) => continue,
            Err(e) => {
                return Err(SSTableError::Corrupted(format!(
                    "Failed to search meta index for {:?} filter: {:?}",
                    filter_type, e
                )));
            }
        }
    }

    Ok(None)
}

/// 基于 MetaIndex 打开 FilterBlockReader。
///
/// 仅在配置开启且提供 policy 的前提下尝试打开；未命中则返回 None。
/// Partitioned filter 通过 partition_loader 延迟加载分区数据。
pub fn open_from_metaindex<F, L>(
    meta: &MetaIndexReader,
    config: &SSTableConfig,
    policy: Option<&dyn FilterPolicy>,
    block_loader: L,
    partition_loader: F,
) -> Result<Option<Box<dyn FilterBlockReader>>>
where
    F: Fn(&BlockHandle) -> std::result::Result<Bytes, FilterError> + Send + Sync + 'static,
    L: Fn(&BlockHandle) -> Result<Bytes>,
{
    if !config.filter_enabled {
        return Ok(None);
    }

    let policy = match policy {
        Some(p) => p,
        None => return Ok(None),
    };

    // 将配置枚举映射到兼容性 policy name（与写入侧保持一致）
    let policy_name = match config.filter_policy {
        boxkv_common::config::FilterPolicyType::FixedBloom => "boxkv.BuiltinBloomFilter",
        boxkv_common::config::FilterPolicyType::FixedRibbon => "boxkv.RibbonFilter",
        boxkv_common::config::FilterPolicyType::LevelBased => "boxkv.BuiltinBloomFilter",
    };

    let (filter_type, filter_handle) = match find_filter_handle(meta, policy_name)? {
        Some(result) => result,
        None => return Ok(None),
    };

    let filter_data = block_loader(&filter_handle)?;

    let reader: Box<dyn FilterBlockReader> = match filter_type {
        FilterBlockType::Full => Box::new(
            FullFilterBlockReader::new(policy, &filter_data).map_err(|e| {
                SSTableError::Corrupted(format!("Failed to create full filter reader: {:?}", e))
            })?,
        ),
        FilterBlockType::Partitioned => Box::new(
            PartitionedFilterBlockReader::new(filter_data, partition_loader).map_err(|e| {
                SSTableError::Corrupted(format!(
                    "Failed to create partitioned filter reader: {:?}",
                    e
                ))
            })?,
        ),
    };

    Ok(Some(reader))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sstable::meta_index::{MetaIndexBuilder, MetaIndexCodec};

    #[test]
    fn test_meta_key_for() {
        let policy_name = "bloom_10";

        assert_eq!(
            meta_key_for(policy_name, FilterBlockType::Full).unwrap(),
            "fullfilter.bloom_10"
        );
        assert_eq!(
            meta_key_for(policy_name, FilterBlockType::Partitioned).unwrap(),
            "partitionedfilter.bloom_10"
        );
    }

    #[test]
    fn test_find_filter_handle() {
        let mut builder = MetaIndexBuilder::new(MetaIndexCodec, 1);
        let policy_name = "bloom_10";
        let handle = BlockHandle::new(100, 200);

        let key = meta_key_for(policy_name, FilterBlockType::Full).unwrap();
        builder.add(&MetaIndexKey::new(key), &handle).unwrap();

        let data = builder.finish().unwrap();
        let reader = MetaIndexReader::new(MetaIndexCodec, data).unwrap();

        let result = find_filter_handle(&reader, policy_name).unwrap();
        assert!(result.is_some());
        let (filter_type, found_handle) = result.unwrap();
        assert_eq!(filter_type, FilterBlockType::Full);
        assert_eq!(found_handle.offset, 100);
        assert_eq!(found_handle.size, 200);
    }

    #[test]
    fn test_find_filter_handle_not_found() {
        let mut builder = MetaIndexBuilder::new(MetaIndexCodec, 1);
        let dummy_handle = BlockHandle::new(999, 100);
        builder
            .add(
                &MetaIndexKey::new("other_filter.other_policy".to_string()),
                &dummy_handle,
            )
            .unwrap();

        let data = builder.finish().unwrap();
        let reader = MetaIndexReader::new(MetaIndexCodec, data).unwrap();

        let result = find_filter_handle(&reader, "bloom_10").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_find_filter_handle_priority() {
        let mut builder = MetaIndexBuilder::new(MetaIndexCodec, 1);
        let policy_name = "bloom_10";

        let full_handle = BlockHandle::new(100, 200);
        let part_handle = BlockHandle::new(300, 400);

        builder
            .add(
                &MetaIndexKey::new(meta_key_for(policy_name, FilterBlockType::Full).unwrap()),
                &full_handle,
            )
            .unwrap();
        builder
            .add(
                &MetaIndexKey::new(
                    meta_key_for(policy_name, FilterBlockType::Partitioned).unwrap(),
                ),
                &part_handle,
            )
            .unwrap();

        let data = builder.finish().unwrap();
        let reader = MetaIndexReader::new(MetaIndexCodec, data).unwrap();

        let result = find_filter_handle(&reader, policy_name).unwrap();
        assert!(result.is_some());
        let (filter_type, found_handle) = result.unwrap();
        assert_eq!(filter_type, FilterBlockType::Full);
        assert_eq!(found_handle.offset, 100);
    }
}
