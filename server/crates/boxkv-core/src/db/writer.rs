use crate::db::error::{DBError, Result};
use crate::db::types::WriteStallCondition;
use crate::memtable::Memtable;
use crate::sequence::SequenceGenerator;
use crate::version::VersionSet;
use crate::wal::Wal;
use crate::{
    HookContext, HookProvider, WasmCallPlan,
    hooks::{PreWriteAction, WriteCommand, WriteContext},
};
use arc_swap::ArcSwap;
use boxkv_common::config::GlobalConfig;
use boxkv_common::types::ValueType;
use boxkv_executor::GlobalScheduler;
use boxkv_storage::FileSystem;
use bytes::Bytes;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
/// 写入路径模块
/// - write_internal：单条写入逻辑
/// - maybe_switch_memtable：Memtable 切换与 WAL 轮转
/// - check_write_stall：检查并执行写停止策略
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// 单条写入逻辑
/// 流程：校验大小 → PreWrite Hook → 分配序列号 → 写 WAL → 写 Memtable → PostWrite Hook（异步）
pub fn write_internal<FS: FileSystem>(
    key: Bytes,
    value: ValueType,
    wal: &Arc<Mutex<Wal<FS>>>,
    mem: &Arc<ArcSwap<Memtable>>,
    sequence: &SequenceGenerator,
    closed: &AtomicBool,
    hook_provider: &Arc<dyn HookProvider>,
    hook_context: &HookContext,
    plan: &WasmCallPlan,
    executor: &Arc<GlobalScheduler>,
) -> Result<()> {
    let cfg = GlobalConfig::get();

    // 检查是否关闭
    if closed.load(Ordering::Acquire) {
        return Err(DBError::Closed);
    }

    // 校验 key/value 大小
    let key_len = key.len();
    let max_key = cfg.limits.max_key_size_kb * 1024;
    if key_len > max_key {
        return Err(DBError::KeyTooLarge(key_len, max_key));
    }

    let value_len = match &value {
        ValueType::Normal(v) => v.len(),
        ValueType::Expiring { data, .. } => data.len(),
        ValueType::Tombstone => 0,
    };

    let max_value = cfg.limits.max_value_size_mb * 1024 * 1024;
    if value_len > max_value {
        return Err(DBError::ValueTooLarge(value_len, max_value));
    }

    // PreWrite Hook：在 WAL 前执行
    let (final_key, final_value) = {
        let write_ctx = WriteContext::new(key.clone(), value.clone());
        let action = hook_provider.pre_write(hook_context, plan, &write_ctx)?;

        match action {
            PreWriteAction::Accept => (key.clone(), value.clone()),
            PreWriteAction::Reject(reason) => {
                tracing::warn!("PreWrite rejected: key={:?} reason={}", key, reason);
                return Err(DBError::PluginRejected(reason));
            }
            PreWriteAction::Transform(commands) => {
                tracing::debug!("PreWrite transform: {} commands", commands.len());
                apply_write_commands(key.clone(), value.clone(), &commands)
            }
        }
    };

    // 分配序列号
    let seq = sequence.next();

    // 写 WAL
    {
        let mut wal_guard = wal.lock().map_err(|e| {
            tracing::error!("WAL lock poisoned: {}", e);
            DBError::Internal(format!("WAL lock poisoned: {}", e))
        })?;
        match &final_value {
            ValueType::Normal(v) => wal_guard.append_normal(seq, final_key.clone(), v.clone())?,
            ValueType::Tombstone => wal_guard.append_tombstone(seq, final_key.clone())?,
            ValueType::Expiring { data, expire_at } => {
                wal_guard.append_expire(seq, final_key.clone(), data.clone(), *expire_at)?
            }
        }
    }

    // 写 Memtable
    {
        let current_mem = mem.load();
        current_mem.insert(final_key.clone(), final_value.clone(), seq);
    }

    // PostWrite Hook：异步执行
    if !plan.is_empty() {
        let hook_provider = Arc::clone(hook_provider);
        let hook_context = HookContext {
            read_sequence: None,
            db_view: Arc::clone(&hook_context.db_view),
        };
        let plan = plan.clone();
        let post_ctx = WriteContext::new(final_key.clone(), final_value.clone());

        // 提交到调度器异步执行
        let spec = boxkv_executor::TaskSpec::new(
            boxkv_executor::WorkClass::Durability,
            boxkv_executor::SizeHint::Bytes(64),
        )
        .with_tag("post_write_hook");

        let _ = executor.spawn_with_spec_async(spec, move |_cancel| {
            hook_provider.post_write(&hook_context, &plan, &post_ctx, seq);
            None
        });
    }

    Ok(())
}

/// 应用写入变更指令列表
fn apply_write_commands(
    mut key: Bytes,
    value: ValueType,
    commands: &[WriteCommand],
) -> (Bytes, ValueType) {
    let (mut data, mut expires_at) = match &value {
        ValueType::Normal(d) => (d.clone(), None),
        ValueType::Expiring { data: d, expire_at } => (d.clone(), Some(*expire_at)),
        ValueType::Tombstone => (Bytes::new(), None),
    };
    let now_secs = boxkv_common::time::current_timestamp_secs();
    for cmd in commands {
        match cmd {
            WriteCommand::SetKey(new_key) => {
                key = new_key.clone();
                tracing::debug!("Applied SetKey: {:?}", key);
            }
            WriteCommand::SetValue(new_value) => {
                data = new_value.clone();
                tracing::debug!("Applied SetValue: {} bytes", data.len());
            }
            WriteCommand::SetTTL(ttl_secs) => {
                expires_at = Some(now_secs + ttl_secs);
                tracing::debug!("Applied SetTTL: {} secs", ttl_secs);
            }
            WriteCommand::SetExpiresAt(ts) => {
                expires_at = Some(*ts);
                tracing::debug!("Applied SetExpiresAt: {}", ts);
            }
            WriteCommand::ClearTTL => {
                expires_at = None;
                tracing::debug!("Applied ClearTTL");
            }
        }
    }

    let final_value = match expires_at {
        Some(expire_at) => ValueType::Expiring { data, expire_at },
        None => ValueType::Normal(data),
    };

    (key, final_value)
}

/// 检查写停止条件
/// Normal: 直接返回 / SoftStall: 睡眠 1ms / HardStall: 返回错误
pub fn check_write_stall(
    imm: &Arc<Mutex<VecDeque<Arc<Memtable>>>>,
    versions: &VersionSet,
) -> Result<()> {
    let cfg = GlobalConfig::get();

    let imm_count = {
        let imm_guard = imm.lock().map_err(|e| {
            tracing::error!("Immutable memtable lock poisoned: {}", e);
            DBError::Internal(format!("Immutable memtable lock poisoned: {}", e))
        })?;
        imm_guard.len()
    };

    let l0_count = if let Some(l0) = versions.current().level(0) {
        l0.len()
    } else {
        0
    };

    let soft_limit = cfg.storage.max_write_buffer_number;
    let hard_limit = soft_limit + 2;

    let condition = WriteStallCondition::compute(imm_count, l0_count, soft_limit, hard_limit);

    match condition {
        WriteStallCondition::Normal => Ok(()),
        WriteStallCondition::SoftStall => {
            tracing::warn!(
                "Soft write stall: imm_count={}, l0_count={}, soft_limit={}",
                imm_count,
                l0_count,
                soft_limit
            );
            std::thread::sleep(Duration::from_millis(1));
            Ok(())
        }
        WriteStallCondition::HardStall => {
            tracing::error!(
                "Hard write stall: imm_count={}, l0_count={}, hard_limit={}",
                imm_count,
                l0_count,
                hard_limit
            );
            Err(DBError::WriteStalled)
        }
    }
}

/// 检查 Memtable 是否需要切换
/// 返回：是否切换了 Memtable
pub fn maybe_switch_memtable<FS: FileSystem>(
    mem: &Arc<ArcSwap<Memtable>>,
    imm: &Arc<Mutex<VecDeque<Arc<Memtable>>>>,
    wal: &Arc<Mutex<Wal<FS>>>,
    wal_file_id: &AtomicU64,
    fs: &FS,
    wal_dir: &std::path::Path,
) -> Result<bool> {
    let cfg = GlobalConfig::get();
    let threshold = cfg.storage.memtable_size_mb * 1024 * 1024;

    let current_mem = mem.load();
    if current_mem.size() < threshold {
        return Ok(false);
    }

    let new_mem = Arc::new(Memtable::new());
    let old_mem = mem.swap(new_mem);

    {
        let mut imm_guard = imm.lock().map_err(|e| {
            tracing::error!("Immutable memtable lock poisoned: {}", e);
            DBError::Internal(format!("Immutable memtable lock poisoned: {}", e))
        })?;
        imm_guard.push_back(old_mem);
        if imm_guard.len() >= cfg.storage.max_write_buffer_number {
            tracing::warn!("Too many immutable memtables: {}", imm_guard.len());
        }
    }

    let new_id = wal_file_id.fetch_add(1, Ordering::SeqCst) + 1;
    wal.lock()
        .map_err(|e| {
            tracing::error!("WAL lock poisoned during rotation: {}", e);
            DBError::Internal(format!("WAL lock poisoned: {}", e))
        })?
        .rotate(fs, wal_dir, new_id)?;

    Ok(true)
}
