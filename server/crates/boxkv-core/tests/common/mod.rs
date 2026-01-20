use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use boxkv_common::types::ValueType;
use bytes::Bytes;
use tempfile::TempDir;

use boxkv_common::config::GlobalConfig;
use boxkv_core::hooks::{OnReadAction, PreWriteAction, ScanFilterAction, WriteContext};
use boxkv_core::{BoxKV, HookContext, HookProvider, WasmCallPlan};
use boxkv_executor::{GlobalScheduler, SchedulerConfig};

/// 创建空 plan（测试用）
pub fn empty_plan() -> WasmCallPlan {
    WasmCallPlan::new()
}

/// 测试用 Mock HookProvider（直通，不执行任何变换）
#[derive(Debug, Clone)]
pub struct MockHookProvider;

impl HookProvider for MockHookProvider {
    fn pre_write(
        &self,
        _ctx: &HookContext,
        _plan: &WasmCallPlan,
        _write_ctx: &WriteContext,
    ) -> boxkv_core::db::error::Result<PreWriteAction> {
        Ok(PreWriteAction::Accept)
    }

    fn post_write(
        &self,
        _ctx: &HookContext,
        _plan: &WasmCallPlan,
        _write_ctx: &WriteContext,
        _sequence: u64,
    ) {
        // No-op
    }

    fn on_read(
        &self,
        _ctx: &HookContext,
        _plan: &WasmCallPlan,
        _key: Bytes,
        value: ValueType,
    ) -> boxkv_core::db::error::Result<OnReadAction> {
        Ok(OnReadAction::Accept(value))
    }

    fn scan_filter(
        &self,
        _ctx: &HookContext,
        _plan: &WasmCallPlan,
        _key: Bytes,
        _value: ValueType,
    ) -> boxkv_core::db::error::Result<ScanFilterAction> {
        Ok(ScanFilterAction::Keep)
    }
}

pub fn init_global() {
    let _ = GlobalConfig::init(GlobalConfig::default());
}

pub fn new_tmp() -> TempDir {
    TempDir::new().expect("create temp dir")
}

pub fn open_db(p: &Path) -> BoxKV {
    let scheduler =
        Arc::new(GlobalScheduler::new(SchedulerConfig::default()).expect("create scheduler"));
    let hook_provider = Arc::new(MockHookProvider) as Arc<dyn HookProvider>;
    BoxKV::open(p, scheduler, hook_provider).expect("open db")
}

pub fn wait_until<F: Fn() -> bool>(timeout: Duration, f: F) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if f() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    false
}

pub fn list_files(dir: &Path) -> usize {
    std::fs::read_dir(dir).map(|r| r.count()).unwrap_or(0)
}

pub fn data_subdir(root: &Path, name: &str) -> PathBuf {
    root.join(name)
}

pub fn sleep_ms(ms: u64) {
    std::thread::sleep(Duration::from_millis(ms));
}
