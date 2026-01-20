// BoxKV Iterator Framework

mod db_iter;
mod level_iter;
mod memtable_iter;
mod merging_iter;
mod sstable_iter;

pub use db_iter::{DBIterator, OwnedDBIterator, ScanStats};
pub use level_iter::LevelIterator;
pub use memtable_iter::MemtableIterator;
pub use merging_iter::MergingIterator;
pub use sstable_iter::SSTableIterator;

pub use crate::sstable::InternalKey;

use crate::error::BoxKVResult;
use bytes::Bytes;
use std::cmp::Ordering;

/// InternalKey 比较器
pub struct InternalKeyComparator;

impl InternalKeyComparator {
    pub fn compare(a: &InternalKey, b: &InternalKey) -> Ordering {
        a.cmp(b)
    }
}

/// 核心迭代器 Trait
pub trait KVIterator: Send + Sync {
    /// 定位到第一个 >= target 的 key
    fn seek(&mut self, target: &InternalKey) -> BoxKVResult<()>;

    /// 定位到第一个 key
    fn seek_to_first(&mut self) -> BoxKVResult<()>;

    /// 定位到最后一个 key
    fn seek_to_last(&mut self) -> BoxKVResult<()>;

    /// 移动到下一个 key
    fn next(&mut self) -> BoxKVResult<()>;

    /// 移动到上一个 key
    fn prev(&mut self) -> BoxKVResult<()>;

    /// 当前位置是否有效
    fn valid(&self) -> bool;

    /// 获取当前 key
    fn key(&self) -> Option<InternalKey>;

    /// 获取当前 value
    fn value(&self) -> Option<Bytes>;

    /// 获取错误状态
    fn status(&self) -> BoxKVResult<()>;
}

/// 用户级迭代器项
#[derive(Debug, Clone)]
pub struct IteratorItem {
    pub key: Bytes,
    pub value: Bytes,
}
