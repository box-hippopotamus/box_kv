use crate::sstable::filter::bits::FilterBitsReader;
use bytes::Bytes;

/// AlwaysTrueFilter - 总是返回 true 的 Filter
///
/// 用于 filter 数据损坏时的安全回退
pub struct AlwaysTrueFilter;

impl AlwaysTrueFilter {
    /// 创建新的 AlwaysTrueFilter
    pub fn new() -> Self {
        Self
    }
}

impl FilterBitsReader for AlwaysTrueFilter {
    fn may_match(&self, _entry: Bytes) -> bool {
        true
    }
}

impl Default for AlwaysTrueFilter {
    fn default() -> Self {
        Self::new()
    }
}
