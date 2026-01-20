// Merging Iterator

use super::{InternalKey, IteratorItem, KVIterator};
use crate::error::BoxKVResult;
use bytes::Bytes;
use std::cmp::Ordering;
use std::collections::BinaryHeap;

/// 迭代器包装器（用于堆排序）
struct IteratorWrapper {
    /// 迭代器索引（用于区分不同的迭代器）
    index: usize,

    /// 当前 key（缓存）
    current_key: Option<InternalKey>,

    /// 迭代器
    iter: Box<dyn KVIterator>,
}

impl IteratorWrapper {
    fn new(index: usize, iter: Box<dyn KVIterator>) -> Self {
        let current_key = iter.key();
        Self {
            index,
            current_key,
            iter,
        }
    }

    /// 推进迭代器并更新当前 key
    fn advance(&mut self) -> BoxKVResult<()> {
        self.iter.next()?;
        self.current_key = self.iter.key();
        Ok(())
    }

    fn valid(&self) -> bool {
        self.iter.valid()
    }

    fn key(&self) -> Option<&InternalKey> {
        self.current_key.as_ref()
    }

    fn value(&self) -> Option<Bytes> {
        self.iter.value()
    }
}

/// 实现堆排序（最小堆）
impl PartialEq for IteratorWrapper {
    fn eq(&self, other: &Self) -> bool {
        self.current_key == other.current_key
    }
}

impl Eq for IteratorWrapper {}

impl PartialOrd for IteratorWrapper {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for IteratorWrapper {
    fn cmp(&self, other: &Self) -> Ordering {
        // 反转比较顺序以实现最小堆
        match (&other.current_key, &self.current_key) {
            (Some(a), Some(b)) => a.cmp(b),
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (None, None) => Ordering::Equal,
        }
    }
}

/// 合并迭代器
pub struct MergingIterator {
    /// 最小堆（按 InternalKey 排序）
    heap: BinaryHeap<IteratorWrapper>,

    /// 读取序列号（MVCC 隔离）
    read_sequence: u64,

    /// 上次返回的 user_key（用于去重）
    last_user_key: Option<Bytes>,

    /// 当前有效的 key-value（缓存）
    current: Option<IteratorItem>,

    /// 是否已初始化
    initialized: bool,
}

impl MergingIterator {
    /// 创建新的合并迭代器
    pub fn new(iters: Vec<Box<dyn KVIterator>>, read_sequence: u64) -> Self {
        let mut heap = BinaryHeap::new();

        // 立即初始化堆
        for (idx, iter) in iters.into_iter().enumerate() {
            let wrapper = IteratorWrapper::new(idx, iter);
            if wrapper.valid() {
                heap.push(wrapper);
            }
        }

        Self {
            heap,
            read_sequence,
            last_user_key: None,
            current: None,
            initialized: true,
        }
    }

    /// 初始化堆（在第一次 seek 时调用）
    fn init_heap(&mut self, mut iters: Vec<Box<dyn KVIterator>>) -> BoxKVResult<()> {
        for (idx, iter) in iters.drain(..).enumerate() {
            let wrapper = IteratorWrapper::new(idx, iter);
            if wrapper.valid() {
                self.heap.push(wrapper);
            }
        }
        self.initialized = true;
        Ok(())
    }

    /// 查找下一个有效的用户级条目
    fn find_next_valid(&mut self) -> BoxKVResult<bool> {
        loop {
            // 取出堆顶（最小的 InternalKey）
            let mut top = match self.heap.pop() {
                Some(t) => t,
                None => {
                    // 堆空，迭代结束
                    self.current = None;
                    return Ok(false);
                }
            };

            let key = match top.key() {
                Some(k) => k.clone(),
                None => {
                    // 当前迭代器无效，继续下一个
                    continue;
                }
            };

            // 1. 版本过滤：跳过不可见的版本
            if key.sequence > self.read_sequence {
                // 推进该迭代器
                top.advance()?;
                if top.valid() {
                    self.heap.push(top);
                }
                continue;
            }

            // 2. 去重：跳过同一个 user_key 的旧版本
            if let Some(ref last_key) = self.last_user_key {
                if &key.user_key == last_key {
                    // 这是同一个 key 的旧版本，跳过
                    top.advance()?;
                    if top.valid() {
                        self.heap.push(top);
                    }
                    continue;
                }
            }

            // 3. 处理 Tombstone
            let value = top.value().unwrap_or_else(|| Bytes::new());
            if value.is_empty() {
                // 记录这个 key 已被删除
                self.last_user_key = Some(key.user_key.clone());

                top.advance()?;
                if top.valid() {
                    self.heap.push(top);
                }

                continue;
            }

            // 找到了有效的版本
            self.last_user_key = Some(key.user_key.clone());
            self.current = Some(IteratorItem {
                key: key.user_key.clone(),
                value,
            });

            // 推进迭代器并重新入堆
            top.advance()?;
            if top.valid() {
                self.heap.push(top);
            }

            return Ok(true);
        }
    }
}

impl KVIterator for MergingIterator {
    fn seek(&mut self, target: &InternalKey) -> BoxKVResult<()> {
        // 清空堆，重新 seek 所有迭代器
        let mut iters = Vec::new();
        while let Some(mut wrapper) = self.heap.pop() {
            wrapper.iter.seek(target)?;
            iters.push(wrapper.iter);
        }

        // 重新初始化堆
        self.heap.clear();
        for (idx, iter) in iters.drain(..).enumerate() {
            if iter.valid() {
                let wrapper = IteratorWrapper::new(idx, iter);
                self.heap.push(wrapper);
            }
        }

        self.last_user_key = None;
        self.find_next_valid()?;
        Ok(())
    }

    fn seek_to_first(&mut self) -> BoxKVResult<()> {
        // Seek 所有迭代器到第一个位置
        let mut iters = Vec::new();
        while let Some(mut wrapper) = self.heap.pop() {
            wrapper.iter.seek_to_first()?;
            iters.push(wrapper.iter);
        }

        // 重新初始化堆
        self.heap.clear();
        for (idx, iter) in iters.drain(..).enumerate() {
            if iter.valid() {
                let wrapper = IteratorWrapper::new(idx, iter);
                self.heap.push(wrapper);
            }
        }

        self.last_user_key = None;
        self.find_next_valid()?;
        Ok(())
    }

    fn seek_to_last(&mut self) -> BoxKVResult<()> {
        // MergingIterator 不支持反向遍历
        self.current = None;
        Ok(())
    }

    fn next(&mut self) -> BoxKVResult<()> {
        self.find_next_valid()?;
        Ok(())
    }

    fn prev(&mut self) -> BoxKVResult<()> {
        // MergingIterator 不支持反向遍历
        Ok(())
    }

    fn valid(&self) -> bool {
        self.current.is_some()
    }

    fn key(&self) -> Option<InternalKey> {
        self.current
            .as_ref()
            .map(|item| InternalKey::new(item.key.clone(), self.read_sequence))
    }

    fn value(&self) -> Option<Bytes> {
        self.current.as_ref().map(|item| item.value.clone())
    }

    fn status(&self) -> BoxKVResult<()> {
        Ok(())
    }
}

/// 用户级迭代器接口
impl MergingIterator {
    /// 获取当前条目（用户视图）
    pub fn current_item(&self) -> Option<&IteratorItem> {
        self.current.as_ref()
    }

    /// 收集指定数量的条目
    pub fn collect(&mut self, limit: usize) -> BoxKVResult<Vec<IteratorItem>> {
        let mut results = Vec::new();

        while self.valid() && results.len() < limit {
            if let Some(item) = self.current_item() {
                results.push(item.clone());
            }
            self.next()?;
        }

        Ok(results)
    }
}
