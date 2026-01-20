use crate::db::BoxKV;
use crate::hooks::WasmCallPlan;
use boxkv_executor::{ExecutorError, Scope, SizeHint, TaskHandle, TaskSpec, WorkClass};
use bytes::Bytes;
/// BoxKV 异步接口适配器
use std::sync::Arc;

/// 同步 BoxKV 的异步封装。
///
/// 任务统一提交到 GlobalScheduler，返回 TaskHandle 供调用方选择 await / join / timeout。
pub struct BoxKVAsync {
    inner: Arc<BoxKV>,
}

impl BoxKVAsync {
    pub fn new(db: Arc<BoxKV>) -> Self {
        Self { inner: db }
    }

    /// 提交写入任务。
    ///
    /// TaskHandle 支持 await / join / join_timeout，具体由调用方选择等待策略。
    pub fn put_async(&self, key: Bytes, value: Bytes, plan: &WasmCallPlan) -> TaskHandle<()> {
        let db = Arc::clone(&self.inner);
        let write_bytes = (key.len() + value.len() + 16) as u64;
        let plan = plan.clone();

        let spec = TaskSpec::new(WorkClass::FrontendWrite, SizeHint::Bytes(write_bytes))
            .with_tag("put_async");

        let executor = Arc::clone(db.executor());
        executor.spawn_with_spec_handle(spec, move |_cancel| {
            db.put(key, value, &plan)
                .map_err(|e| ExecutorError::Internal(e.to_string()))
        })
    }

    /// 提交读取任务。
    pub fn get_async(&self, key: Bytes, plan: &WasmCallPlan) -> TaskHandle<Option<Bytes>> {
        let db = Arc::clone(&self.inner);
        let key_bytes = key.len() as u64;
        let plan = plan.clone();

        let spec = TaskSpec::new(
            WorkClass::FrontendReadSmall,
            SizeHint::ReadKey {
                key_bytes,
                scope: Scope::Global,
            },
        )
        .with_tag("get_async");

        let executor = Arc::clone(db.executor());
        executor.spawn_with_spec_handle(spec, move |_cancel| {
            db.get(key, &plan)
                .map_err(|e| ExecutorError::Internal(e.to_string()))
        })
    }

    /// 提交删除任务。
    pub fn delete_async(&self, key: Bytes, plan: &WasmCallPlan) -> TaskHandle<()> {
        let db = Arc::clone(&self.inner);
        let key_bytes = (key.len() + 16) as u64;
        let plan = plan.clone();

        let spec = TaskSpec::new(WorkClass::FrontendWrite, SizeHint::Bytes(key_bytes))
            .with_tag("delete_async");

        let executor = Arc::clone(db.executor());
        executor.spawn_with_spec_handle(spec, move |_cancel| {
            db.delete(key, &plan)
                .map_err(|e| ExecutorError::Internal(e.to_string()))
        })
    }

    /// 提交 flush 任务。
    pub fn flush_async(&self) -> TaskHandle<()> {
        let db = Arc::clone(&self.inner);

        let spec = TaskSpec::new(
            WorkClass::Durability,
            SizeHint::Bytes(1024 * 1024), // 粗略的 I/O 体量估计，用于调度 size hint
        )
        .with_tag("flush_async");

        let executor = Arc::clone(db.executor());
        executor.spawn_with_spec_handle(spec, move |_cancel| {
            db.flush()
                .map_err(|e| ExecutorError::Internal(e.to_string()))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hooks::{
        HookContext, HookProvider, OnReadAction, PreWriteAction, ScanFilterAction, WasmCallPlan,
        WriteContext,
    };
    use boxkv_common::config::GlobalConfig;
    use boxkv_common::types::ValueType;
    use boxkv_executor::{GlobalScheduler, SchedulerConfig};
    use tempfile::TempDir;

    #[derive(Debug, Clone)]
    struct MockHookProvider;

    impl HookProvider for MockHookProvider {
        fn pre_write(
            &self,
            _ctx: &HookContext,
            _plan: &WasmCallPlan,
            _write_ctx: &WriteContext,
        ) -> crate::db::error::Result<PreWriteAction> {
            Ok(PreWriteAction::Accept)
        }

        fn post_write(
            &self,
            _ctx: &HookContext,
            _plan: &WasmCallPlan,
            _write_ctx: &WriteContext,
            _sequence: u64,
        ) {
        }

        fn on_read(
            &self,
            _ctx: &HookContext,
            _plan: &WasmCallPlan,
            _key: Bytes,
            value: ValueType,
        ) -> crate::db::error::Result<OnReadAction> {
            Ok(OnReadAction::Accept(value))
        }

        fn scan_filter(
            &self,
            _ctx: &HookContext,
            _plan: &WasmCallPlan,
            _key: Bytes,
            _value: ValueType,
        ) -> crate::db::error::Result<ScanFilterAction> {
            Ok(ScanFilterAction::Keep)
        }
    }

    #[tokio::test]
    async fn test_async_put_and_get() {
        let _ = GlobalConfig::init(GlobalConfig::default());

        let tmp = TempDir::new().unwrap();
        let scheduler = Arc::new(GlobalScheduler::new(SchedulerConfig::default()).unwrap());
        let hook_provider = Arc::new(MockHookProvider) as Arc<dyn HookProvider>;

        let db = Arc::new(BoxKV::open(tmp.path(), scheduler, hook_provider).unwrap());
        let db_async = BoxKVAsync::new(db);

        let handle = db_async.put_async(
            Bytes::from("key1"),
            Bytes::from("value1"),
            &WasmCallPlan::new(),
        );
        handle.await.unwrap();

        let handle = db_async.get_async(Bytes::from("key1"), &WasmCallPlan::new());
        let value = handle.await.unwrap();

        assert_eq!(value, Some(Bytes::from("value1")));
    }

    #[tokio::test]
    async fn test_async_delete() {
        let _ = GlobalConfig::init(GlobalConfig::default());

        let tmp = TempDir::new().unwrap();
        let scheduler = Arc::new(GlobalScheduler::new(SchedulerConfig::default()).unwrap());
        let hook_provider = Arc::new(MockHookProvider) as Arc<dyn HookProvider>;
        let db = Arc::new(BoxKV::open(tmp.path(), scheduler, hook_provider).unwrap());
        let db_async = BoxKVAsync::new(db);

        let handle = db_async.put_async(
            Bytes::from("key1"),
            Bytes::from("value1"),
            &WasmCallPlan::new(),
        );
        handle.await.unwrap();

        let handle = db_async.delete_async(Bytes::from("key1"), &WasmCallPlan::new());
        handle.await.unwrap();

        let handle = db_async.get_async(Bytes::from("key1"), &WasmCallPlan::new());
        let value = handle.await.unwrap();

        assert_eq!(value, None);
    }
}
