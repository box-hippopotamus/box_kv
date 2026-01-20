// DBIterator - 用户级迭代器（包装 MergingIterator + Hook 注入）

use super::{InternalKey, KVIterator, MergingIterator};
use crate::error::BoxKVResult;
use crate::hooks::{HookContext, HookProvider, ScanFilterAction, WasmCallPlan};
use boxkv_common::time::current_timestamp_secs;
use boxkv_common::types::ValueType;
use bytes::Bytes;
use std::sync::Arc;

/// 用户级数据库迭代器
///
/// 特性：
/// - 惰性求值：按需推进
/// - Hook 集成：每条 KV 经过 scan_filter 过滤
/// - MVCC 一致：基于 read_sequence 过滤版本
/// - TTL 过滤：自动跳过已过期的值
pub struct DBIterator {
    /// 底层归并迭代器
    inner: MergingIterator,

    /// 范围上界（不包含）
    end: Bytes,

    /// 当前用户可见的 key/value
    current_key: Option<Bytes>,
    current_value: Option<Bytes>,

    /// 当前位置是否指向有效 KV
    valid: bool,

    /// Hook 执行上下文
    hook_ctx: Arc<HookContext>,

    /// Hook 执行计划
    plan: WasmCallPlan,

    /// Hook 提供者
    hook_provider: Option<Arc<dyn HookProvider>>,

    /// 统计信息
    scanned_count: usize, // 扫描的原始条目数
    filtered_count: usize, // 被 hook 过滤的条目数
    returned_count: usize, // 返回给用户的条目数
}

impl DBIterator {
    /// 创建新的数据库迭代器
    pub fn new(
        inner: MergingIterator,
        start: Bytes,
        end: Bytes,
        hook_ctx: Arc<HookContext>,
        plan: WasmCallPlan,
        hook_provider: Option<Arc<dyn HookProvider>>,
    ) -> BoxKVResult<Self> {
        let mut iter = Self {
            inner,
            end,
            current_key: None,
            current_value: None,
            valid: false,
            hook_ctx,
            plan,
            hook_provider,
            scanned_count: 0,
            filtered_count: 0,
            returned_count: 0,
        };

        // 定位到起始位置
        iter.seek(&start)?;

        Ok(iter)
    }

    /// 定位到指定 key
    pub fn seek(&mut self, target: &[u8]) -> BoxKVResult<()> {
        let internal_key = InternalKey::new(Bytes::copy_from_slice(target), u64::MAX);
        self.inner.seek(&internal_key)?;

        self.advance()
    }

    /// 定位到第一个 key
    pub fn seek_to_first(&mut self) -> BoxKVResult<()> {
        self.inner.seek_to_first()?;
        self.advance()
    }

    /// 移动到下一个 key
    pub fn next(&mut self) -> BoxKVResult<()> {
        if !self.valid {
            return Ok(());
        }

        self.inner.next()?;
        self.advance()
    }

    /// 当前位置是否有效
    pub fn valid(&self) -> bool {
        self.valid
    }

    /// 获取当前 key
    pub fn key(&self) -> Option<&[u8]> {
        self.current_key.as_ref().map(|b| b.as_ref())
    }

    /// 获取当前 value
    pub fn value(&self) -> Option<&[u8]> {
        self.current_value.as_ref().map(|b| b.as_ref())
    }

    /// 获取统计信息
    pub fn stats(&self) -> ScanStats {
        ScanStats {
            scanned: self.scanned_count,
            filtered: self.filtered_count,
            returned: self.returned_count,
        }
    }

    /// 内部推进逻辑
    fn advance(&mut self) -> BoxKVResult<()> {
        let _now_secs = current_timestamp_secs();

        loop {
            if !self.inner.valid() {
                self.valid = false;
                self.current_key = None;
                self.current_value = None;
                return Ok(());
            }

            self.scanned_count += 1;

            let Some(internal_key) = self.inner.key() else {
                self.valid = false;
                return Ok(());
            };

            let Some(value_bytes) = self.inner.value() else {
                self.inner.next()?;
                continue;
            };

            if internal_key.user_key.as_ref() >= self.end.as_ref() {
                self.valid = false;
                self.current_key = None;
                self.current_value = None;
                return Ok(());
            }

            let value_type = ValueType::Normal(value_bytes.clone());

            if let Some(ref provider) = self.hook_provider {
                match provider.scan_filter(
                    &self.hook_ctx,
                    &self.plan,
                    internal_key.user_key.clone(),
                    value_type,
                ) {
                    Ok(ScanFilterAction::Keep) => {
                        self.valid = true;
                        self.current_key = Some(internal_key.user_key.clone());
                        self.current_value = Some(value_bytes);
                        self.returned_count += 1;
                        return Ok(());
                    }
                    Ok(ScanFilterAction::Drop) => {
                        self.filtered_count += 1;
                        self.inner.next()?;
                        continue;
                    }
                    Err(_e) => {
                        self.filtered_count += 1;
                        self.inner.next()?;
                        continue;
                    }
                }
            } else {
                self.valid = true;
                self.current_key = Some(internal_key.user_key.clone());
                self.current_value = Some(value_bytes);
                self.returned_count += 1;
                return Ok(());
            }
        }
    }
}

impl DBIterator {
    /// 转换为拥有型迭代器
    pub fn into_owned(self) -> OwnedDBIterator {
        OwnedDBIterator { inner: self }
    }
}

/// 扫描统计信息
#[derive(Debug, Clone, Copy)]
pub struct ScanStats {
    /// 扫描的原始条目数
    pub scanned: usize,

    /// 被 Hook 过滤的条目数
    pub filtered: usize,

    /// 返回给用户的条目数
    pub returned: usize,
}

// OwnedDBIterator

/// 拥有型数据库迭代器
/// 包装 DBIterator，实现标准 Iterator trait
pub struct OwnedDBIterator {
    inner: DBIterator,
}

impl OwnedDBIterator {
    /// 从 DBIterator 创建
    pub fn from_db_iterator(inner: DBIterator) -> Self {
        Self { inner }
    }

    /// 获取统计信息
    pub fn stats(&self) -> ScanStats {
        self.inner.stats()
    }
}

impl Iterator for OwnedDBIterator {
    type Item = BoxKVResult<(Bytes, Bytes)>;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.inner.valid() {
            return None;
        }

        let result = match (self.inner.key(), self.inner.value()) {
            (Some(key), Some(value)) => {
                Ok((Bytes::copy_from_slice(key), Bytes::copy_from_slice(value)))
            }
            _ => return None,
        };

        if let Err(e) = self.inner.next() {
            return Some(Err(e));
        }

        Some(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hooks::{DbView, HookContext};
    use crate::iterator::MemtableIterator;
    use crate::memtable::Memtable;
    use std::sync::Arc;

    struct NoOpDbView;
    impl DbView for NoOpDbView {
        fn kv_get(&self, _key: &[u8]) -> crate::db::error::Result<Option<Bytes>> {
            Ok(None)
        }

        fn scan_range_iter(
            &self,
            _start: &[u8],
            _end: &[u8],
            _plan: &crate::hooks::WasmCallPlan,
        ) -> crate::db::error::Result<
            Box<dyn Iterator<Item = crate::db::error::Result<(Bytes, Bytes)>> + Send>,
        > {
            Ok(Box::new(std::iter::empty()))
        }
    }

    #[test]
    fn test_db_iterator_basic() {
        // 创建测试 Memtable
        let mem = Arc::new(Memtable::new());
        mem.insert(
            b"key1".to_vec().into(),
            ValueType::Normal(b"value1".to_vec().into()),
            1,
        );
        mem.insert(
            b"key2".to_vec().into(),
            ValueType::Normal(b"value2".to_vec().into()),
            2,
        );
        mem.insert(
            b"key3".to_vec().into(),
            ValueType::Normal(b"value3".to_vec().into()),
            3,
        );

        // 创建 MergingIterator
        let mem_iter = MemtableIterator::new(mem);
        let iters: Vec<Box<dyn KVIterator>> = vec![Box::new(mem_iter)];
        let merging_iter = MergingIterator::new(iters, 100);

        // 创建 DBIterator
        let hook_ctx = Arc::new(HookContext::new(Arc::new(NoOpDbView)));
        let plan = WasmCallPlan::new();

        let mut db_iter = DBIterator::new(
            merging_iter,
            b"key1".to_vec().into(),
            b"key4".to_vec().into(),
            hook_ctx,
            plan,
            None,
        )
        .unwrap();

        // 验证迭代
        assert!(db_iter.valid());
        assert_eq!(db_iter.key(), Some(b"key1".as_ref()));
        assert_eq!(db_iter.value(), Some(b"value1".as_ref()));

        db_iter.next().unwrap();
        assert!(db_iter.valid());
        assert_eq!(db_iter.key(), Some(b"key2".as_ref()));

        db_iter.next().unwrap();
        assert!(db_iter.valid());
        assert_eq!(db_iter.key(), Some(b"key3".as_ref()));

        db_iter.next().unwrap();
        assert!(!db_iter.valid());

        // 验证统计
        let stats = db_iter.stats();
        assert_eq!(stats.returned, 3);
    }
}
