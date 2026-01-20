use bytes::Bytes;
use std::cmp::Ordering;
use std::collections::BinaryHeap;

/// 多路归并的单个来源
///
/// 包含一个 SSTable 迭代器和当前 head 元素（避免重复 next 调用）
pub struct MergeSource {
    pub iter: crate::sstable::SSTableIterator,
    pub current: Option<boxkv_common::types::Entry>,
}

#[derive(Clone)]
/// 堆中元素
///
/// 排序语义：
/// - user_key 升序
/// - 同一 user_key 内 sequence 降序（最新版本优先）
pub struct HeapEntry {
    pub key: Bytes,
    pub sequence: u64,
    pub value: boxkv_common::types::ValueType,
    pub src_idx: usize,
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.sequence == other.sequence
    }
}
impl Eq for HeapEntry {}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.key.as_ref().cmp(other.key.as_ref()) {
            Ordering::Less => Ordering::Greater,
            Ordering::Greater => Ordering::Less,
            Ordering::Equal => other.sequence.cmp(&self.sequence),
        }
    }
}

/// 多路归并堆
///
/// 维护所有来源的当前 head 元素，每次 pop 取出最小的 head（user_key 升序，seq 降序）。
/// 使用 push_next_from_source 推进某个来源的迭代器。
pub struct MergeHeap {
    pub sources: Vec<MergeSource>,
    pub heap: BinaryHeap<HeapEntry>,
}

impl MergeHeap {
    /// 构建归并堆，将每个来源的 head 入堆
    pub fn new(mut sources: Vec<MergeSource>) -> Self {
        let mut heap = BinaryHeap::new();
        for (i, s) in sources.iter_mut().enumerate() {
            if let Some(e) = s.current.take() {
                heap.push(HeapEntry {
                    key: e.key,
                    sequence: e.sequence,
                    value: e.value,
                    src_idx: i,
                });
            }
        }
        Self { sources, heap }
    }

    /// 弹出当前堆顶（可能为 None）
    pub fn pop(&mut self) -> Option<HeapEntry> {
        self.heap.pop()
    }

    /// 推进指定来源的迭代器，将其下一项压入堆（若存在）
    pub fn push_next_from_source(
        &mut self,
        src_idx: usize,
    ) -> Result<(), crate::compaction::types::CompactionError> {
        let src = &mut self.sources[src_idx];
        if let Some(next) = src.iter.next()? {
            self.heap.push(HeapEntry {
                key: next.key,
                sequence: next.sequence,
                value: next.value,
                src_idx,
            });
        }
        Ok(())
    }

    /// 丢弃所有与给定 user_key 相同的堆顶项，并各自推进其来源至下一项
    pub fn discard_same_user_key(
        &mut self,
        user_key: &[u8],
    ) -> Result<(), crate::compaction::types::CompactionError> {
        loop {
            match self.heap.peek() {
                Some(top) if top.key.as_ref() == user_key => {
                    let src_idx = top.src_idx;
                    let _ = self.heap.pop();
                    self.push_next_from_source(src_idx)?;
                }
                _ => break,
            }
        }
        Ok(())
    }
}
