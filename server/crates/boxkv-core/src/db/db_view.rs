//! DbView 实现：供 Wasm 插件使用的只读数据库视图

use crate::cache::TableCache;
use crate::db::error::Result;
use crate::db::reader;
use crate::db::types::SuperVersion;
use crate::hooks::{DbView, HookProvider};
use crate::sequence::SequenceGenerator;
use arc_swap::ArcSwap;
use bytes::Bytes;
use std::sync::Arc;

/// BoxKV 的只读视图实现（供 Hook 使用）
pub struct BoxKVDbView {
    super_version: Arc<ArcSwap<SuperVersion>>,
    table_cache: Arc<TableCache>,
    sequence: Arc<SequenceGenerator>,
    hook_provider: Arc<dyn HookProvider>,
}

impl BoxKVDbView {
    pub fn new(
        super_version: Arc<ArcSwap<SuperVersion>>,
        table_cache: Arc<TableCache>,
        sequence: Arc<SequenceGenerator>,
        hook_provider: Arc<dyn HookProvider>,
    ) -> Self {
        Self {
            super_version,
            table_cache,
            sequence,
            hook_provider,
        }
    }
}

impl DbView for BoxKVDbView {
    fn kv_get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let key = Bytes::copy_from_slice(key);
        let read_sequence = self.sequence.current().saturating_sub(1);

        reader::get(
            key,
            &self.super_version,
            &self.table_cache,
            &std::sync::atomic::AtomicBool::new(false),
            read_sequence,
        )
    }

    /// 范围扫描（惰性迭代器）
    fn scan_range_iter(
        &self,
        start: &[u8],
        end: &[u8],
        plan: &crate::hooks::WasmCallPlan,
    ) -> Result<Box<dyn Iterator<Item = Result<(Bytes, Bytes)>> + Send>> {
        use crate::hooks::HookContext;
        use crate::iterator::{
            DBIterator, KVIterator, LevelIterator, MemtableIterator, MergingIterator,
            SSTableIterator,
        };
        use crate::version::FileMeta;

        let start_key = Bytes::copy_from_slice(start);
        let end_key = Bytes::copy_from_slice(end);
        let read_sequence = self.sequence.current().saturating_sub(1);

        // 收集所有迭代器
        let mut iters: Vec<Box<dyn KVIterator>> = Vec::new();

        let sv = self.super_version.load();

        // 1. Memtable 迭代器（最新数据）
        let mem_iter = MemtableIterator::new(Arc::clone(&sv.mem));
        iters.push(Box::new(mem_iter));

        // 2. Immutable Memtables 迭代器
        for imm in sv.imm.iter() {
            let imm_iter = MemtableIterator::new(Arc::clone(imm));
            iters.push(Box::new(imm_iter));
        }

        // 3. SST files 迭代器
        // Level 0: 每个文件一个迭代器（文件可能重叠）
        if let Some(l0) = sv.version.level(0) {
            for file in l0.iter() {
                // 检查文件是否与范围重叠
                if file.largest.user_key.as_ref() < start || file.smallest.user_key.as_ref() >= end
                {
                    continue;
                }

                if let Ok(reader) = self.table_cache.get_reader(file.file_number) {
                    if let Ok(sst_iter) = SSTableIterator::new(reader) {
                        iters.push(Box::new(sst_iter));
                    }
                }
            }
        }

        // Level 1+: 每个 Level 一个 LevelIterator（文件不重叠）
        for level_idx in 1..sv.version.num_levels() {
            if let Some(level) = sv.version.level(level_idx) {
                let files: Vec<Arc<FileMeta>> = level
                    .iter()
                    .filter(|file| {
                        // 过滤出与范围重叠的文件
                        file.largest.user_key.as_ref() >= start
                            && file.smallest.user_key.as_ref() < end
                    })
                    .map(|f| Arc::new(f.clone()))
                    .collect();

                if !files.is_empty() {
                    let level_iter = LevelIterator::new(files, Arc::clone(&self.table_cache));
                    iters.push(Box::new(level_iter));
                }
            }
        }

        // 创建 MergingIterator
        let merging_iter = MergingIterator::new(iters, read_sequence);

        // 创建 HookContext
        let db_view_arc = Arc::new(BoxKVDbView::new(
            Arc::clone(&self.super_version),
            Arc::clone(&self.table_cache),
            Arc::clone(&self.sequence),
            Arc::clone(&self.hook_provider),
        )) as Arc<dyn crate::hooks::DbView>;

        let hook_ctx = Arc::new(HookContext::new(db_view_arc).with_read_sequence(read_sequence));

        // 创建 DBIterator（惰性 + Hook 注入）
        let db_iter = DBIterator::new(
            merging_iter,
            start_key.clone(),
            end_key.clone(),
            hook_ctx,
            plan.clone(),
            Some(Arc::clone(&self.hook_provider)),
        )?;

        // 转换为拥有型迭代器（实现标准 Iterator trait）
        let owned_iter = db_iter.into_owned();
        Ok(Box::new(owned_iter))
    }
}
