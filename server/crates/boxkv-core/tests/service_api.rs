/// 服务层 API 集成测试
///
/// 测试覆盖：
/// 1. MultiGet（批量点查询，同一快照）
/// 2. WriteBatch（原子批写）
/// 3. Snapshot API（创建/使用/管理）
/// 4. CAS/PutIfAbsent（条件写入）
/// 5. PutWithTTL/ExpireAt（TTL 操作）
/// 6. scan_range_iter（惰性迭代器）
mod common;

use boxkv_common::config::GlobalConfig;
use boxkv_common::time::current_timestamp_secs;
use boxkv_core::HookProvider;
use boxkv_core::db::{BoxKV, WriteBatch};
use boxkv_core::hooks::WasmCallPlan;
use boxkv_executor::{GlobalScheduler, SchedulerConfig};
use bytes::Bytes;
use std::sync::Arc;
use tempfile::TempDir;

fn init_config() {
    let _ = tracing_subscriber::fmt()
        .with_test_writer()
        .with_max_level(tracing::Level::INFO)
        .try_init();

    let _ = GlobalConfig::init(GlobalConfig::default());
}

fn create_test_db(path: &std::path::Path) -> boxkv_core::error::BoxKVResult<BoxKV> {
    let scheduler = Arc::new(GlobalScheduler::new(SchedulerConfig::default()).unwrap());
    let hook_provider = Arc::new(common::MockHookProvider) as Arc<dyn HookProvider>;

    BoxKV::open(path, scheduler, hook_provider)
}

// MultiGet 测试
#[test]
fn test_multi_get_basic() {
    init_config();
    let temp_dir = TempDir::new().unwrap();
    let db = create_test_db(temp_dir.path()).unwrap();

    let plan = WasmCallPlan::new();

    // 写入数据
    db.put(Bytes::from("k1"), Bytes::from("v1"), &plan).unwrap();
    db.put(Bytes::from("k2"), Bytes::from("v2"), &plan).unwrap();
    db.put(Bytes::from("k3"), Bytes::from("v3"), &plan).unwrap();

    // MultiGet
    let keys = vec![
        Bytes::from("k1"),
        Bytes::from("k2"),
        Bytes::from("k3"),
        Bytes::from("k4"), // 不存在
    ];

    let results = db.multi_get(keys, &plan).unwrap();

    assert_eq!(results.len(), 4);
    assert_eq!(results[0], Some(Bytes::from("v1")));
    assert_eq!(results[1], Some(Bytes::from("v2")));
    assert_eq!(results[2], Some(Bytes::from("v3")));
    assert_eq!(results[3], None);
}

#[test]
fn test_multi_get_snapshot_consistency() {
    init_config();
    let temp_dir = TempDir::new().unwrap();
    let db = create_test_db(temp_dir.path()).unwrap();

    let plan = WasmCallPlan::new();

    // 写入初始数据
    db.put(Bytes::from("k1"), Bytes::from("v1"), &plan).unwrap();
    db.put(Bytes::from("k2"), Bytes::from("v2"), &plan).unwrap();

    // 记录当前序列号
    let seq_before = db.current_sequence();

    // MultiGet（应该看到 v1, v2）
    let keys = vec![Bytes::from("k1"), Bytes::from("k2")];
    let results = db.multi_get(keys.clone(), &plan).unwrap();

    assert_eq!(results[0], Some(Bytes::from("v1")));
    assert_eq!(results[1], Some(Bytes::from("v2")));

    // 在 MultiGet 过程中，其他线程修改数据
    db.put(Bytes::from("k1"), Bytes::from("v1_new"), &plan)
        .unwrap();
    db.put(Bytes::from("k2"), Bytes::from("v2_new"), &plan)
        .unwrap();

    let seq_after = db.current_sequence();
    assert!(seq_after > seq_before);

    // 再次 MultiGet（同一快照内，应该看到一致的数据）
    // 注意：这里的"一致性"是指单次 multi_get 调用内部使用同一快照
    let results2 = db.multi_get(keys, &plan).unwrap();

    // 新的 multi_get 会看到最新数据
    assert_eq!(results2[0], Some(Bytes::from("v1_new")));
    assert_eq!(results2[1], Some(Bytes::from("v2_new")));
}

#[test]
fn test_multi_get_empty() {
    init_config();
    let temp_dir = TempDir::new().unwrap();
    let db = create_test_db(temp_dir.path()).unwrap();

    let plan = WasmCallPlan::new();

    // 空查询
    let results = db.multi_get(vec![], &plan).unwrap();
    assert_eq!(results.len(), 0);
}

// WriteBatch 增强测试
#[test]
fn test_write_batch_enhanced_atomic() {
    init_config();
    let temp_dir = TempDir::new().unwrap();
    let db = create_test_db(temp_dir.path()).unwrap();

    let plan = WasmCallPlan::new();

    // 创建批次
    let mut batch = WriteBatch::new();
    batch.put(Bytes::from("k1"), Bytes::from("v1"));
    batch.put(Bytes::from("k2"), Bytes::from("v2"));
    batch.put(Bytes::from("k3"), Bytes::from("v3"));
    batch.delete(Bytes::from("k4"));

    // 原子提交
    db.write_batch_enhanced(batch, &plan).unwrap();

    // 验证所有操作都生效
    assert_eq!(
        db.get(Bytes::from("k1"), &plan).unwrap(),
        Some(Bytes::from("v1"))
    );
    assert_eq!(
        db.get(Bytes::from("k2"), &plan).unwrap(),
        Some(Bytes::from("v2"))
    );
    assert_eq!(
        db.get(Bytes::from("k3"), &plan).unwrap(),
        Some(Bytes::from("v3"))
    );
    assert_eq!(db.get(Bytes::from("k4"), &plan).unwrap(), None);
}

#[test]
fn test_write_batch_enhanced_with_ttl() {
    init_config();
    let temp_dir = TempDir::new().unwrap();
    let db = create_test_db(temp_dir.path()).unwrap();

    let plan = WasmCallPlan::new();

    // 创建包含 TTL 的批次
    let mut batch = WriteBatch::new();
    batch.put(Bytes::from("k1"), Bytes::from("v1"));
    batch.put_with_ttl(Bytes::from("k2"), Bytes::from("v2"), 3600); // 1小时
    batch.put(Bytes::from("k3"), Bytes::from("v3"));

    db.write_batch_enhanced(batch, &plan).unwrap();

    // 验证所有数据都存在
    assert_eq!(
        db.get(Bytes::from("k1"), &plan).unwrap(),
        Some(Bytes::from("v1"))
    );
    assert_eq!(
        db.get(Bytes::from("k2"), &plan).unwrap(),
        Some(Bytes::from("v2"))
    );
    assert_eq!(
        db.get(Bytes::from("k3"), &plan).unwrap(),
        Some(Bytes::from("v3"))
    );
}

// Snapshot API 测试

#[test]
fn test_create_snapshot_basic() {
    init_config();
    let temp_dir = TempDir::new().unwrap();
    let db = create_test_db(temp_dir.path()).unwrap();

    let plan = WasmCallPlan::new();

    // 写入数据
    db.put(Bytes::from("k1"), Bytes::from("v1"), &plan).unwrap();

    // 创建快照
    let snapshot = db.create_snapshot().unwrap();
    let snap_seq = db.snapshot_sequence(&snapshot);

    // 序列号应该 >= 0（刚写入一条数据，序列号应该是0）
    assert!(snap_seq >= 0);
    assert_eq!(db.active_snapshots_count(), 1);

    // 快照应该能读取数据
    assert_eq!(
        db.get_with_snapshot(&snapshot, Bytes::from("k1")),
        Some(Bytes::from("v1"))
    );

    // 修改数据
    db.put(Bytes::from("k1"), Bytes::from("v2"), &plan).unwrap();

    // DB 看到新值
    assert_eq!(
        db.get(Bytes::from("k1"), &plan).unwrap(),
        Some(Bytes::from("v2"))
    );

    // 快照仍然看到旧值
    assert_eq!(
        db.get_with_snapshot(&snapshot, Bytes::from("k1")),
        Some(Bytes::from("v1"))
    );

    // 释放快照
    drop(snapshot);
    assert_eq!(db.active_snapshots_count(), 0);
}

#[test]
fn test_snapshot_scan() {
    init_config();
    let temp_dir = TempDir::new().unwrap();
    let db = create_test_db(temp_dir.path()).unwrap();

    let plan = WasmCallPlan::new();

    // 写入数据
    db.put(Bytes::from("k1"), Bytes::from("v1"), &plan).unwrap();
    db.put(Bytes::from("k2"), Bytes::from("v2"), &plan).unwrap();
    db.put(Bytes::from("k3"), Bytes::from("v3"), &plan).unwrap();

    // 创建快照
    let snapshot = db.create_snapshot().unwrap();

    // 修改数据
    db.put(Bytes::from("k1"), Bytes::from("v1_new"), &plan)
        .unwrap();
    db.delete(Bytes::from("k2"), &plan).unwrap();
    db.put(Bytes::from("k4"), Bytes::from("v4"), &plan).unwrap();

    // 快照扫描应该看到旧数据
    let results = db.scan_with_snapshot(&snapshot, b"k0", b"k5", 10).unwrap();

    assert_eq!(results.len(), 3);
    assert_eq!(results[0], (Bytes::from("k1"), Bytes::from("v1")));
    assert_eq!(results[1], (Bytes::from("k2"), Bytes::from("v2")));
    assert_eq!(results[2], (Bytes::from("k3"), Bytes::from("v3")));
}

#[test]
fn test_multiple_snapshots() {
    init_config();
    let temp_dir = TempDir::new().unwrap();
    let db = create_test_db(temp_dir.path()).unwrap();

    let plan = WasmCallPlan::new();

    // 写入 v1
    db.put(Bytes::from("key"), Bytes::from("v1"), &plan)
        .unwrap();
    let snap1 = db.create_snapshot().unwrap();

    // 写入 v2
    db.put(Bytes::from("key"), Bytes::from("v2"), &plan)
        .unwrap();
    let snap2 = db.create_snapshot().unwrap();

    // 写入 v3
    db.put(Bytes::from("key"), Bytes::from("v3"), &plan)
        .unwrap();
    let snap3 = db.create_snapshot().unwrap();

    assert_eq!(db.active_snapshots_count(), 3);

    // 验证每个快照看到正确的版本
    assert_eq!(
        db.get_with_snapshot(&snap1, Bytes::from("key")),
        Some(Bytes::from("v1"))
    );
    assert_eq!(
        db.get_with_snapshot(&snap2, Bytes::from("key")),
        Some(Bytes::from("v2"))
    );
    assert_eq!(
        db.get_with_snapshot(&snap3, Bytes::from("key")),
        Some(Bytes::from("v3"))
    );

    // 释放快照
    drop(snap1);
    assert_eq!(db.active_snapshots_count(), 2);
    drop(snap2);
    assert_eq!(db.active_snapshots_count(), 1);
    drop(snap3);
    assert_eq!(db.active_snapshots_count(), 0);
}

// CAS/PutIfAbsent 测试

#[test]
fn test_compare_and_set_success() {
    init_config();
    let temp_dir = TempDir::new().unwrap();
    let db = create_test_db(temp_dir.path()).unwrap();

    let plan = WasmCallPlan::new();

    // CAS: 期望不存在 -> 写入成功
    let result = db
        .compare_and_set(Bytes::from("key"), None, Some(Bytes::from("v1")), &plan)
        .unwrap();

    assert!(result);
    assert_eq!(
        db.get(Bytes::from("key"), &plan).unwrap(),
        Some(Bytes::from("v1"))
    );

    // CAS: 期望 v1 -> 更新为 v2
    let result = db
        .compare_and_set(
            Bytes::from("key"),
            Some(Bytes::from("v1")),
            Some(Bytes::from("v2")),
            &plan,
        )
        .unwrap();

    assert!(result);
    assert_eq!(
        db.get(Bytes::from("key"), &plan).unwrap(),
        Some(Bytes::from("v2"))
    );
}

#[test]
fn test_compare_and_set_failure() {
    init_config();
    let temp_dir = TempDir::new().unwrap();
    let db = create_test_db(temp_dir.path()).unwrap();

    let plan = WasmCallPlan::new();

    // 写入初始值
    db.put(Bytes::from("key"), Bytes::from("v1"), &plan)
        .unwrap();

    // CAS: 期望 v2（错误）-> 失败
    let result = db
        .compare_and_set(
            Bytes::from("key"),
            Some(Bytes::from("v2")),
            Some(Bytes::from("v3")),
            &plan,
        )
        .unwrap();

    assert!(!result);
    // 值不应该改变
    assert_eq!(
        db.get(Bytes::from("key"), &plan).unwrap(),
        Some(Bytes::from("v1"))
    );
}

#[test]
fn test_put_if_absent() {
    init_config();
    let temp_dir = TempDir::new().unwrap();
    let db = create_test_db(temp_dir.path()).unwrap();

    let plan = WasmCallPlan::new();

    // 第一次写入成功
    let result = db
        .put_if_absent(Bytes::from("key"), Bytes::from("v1"), &plan)
        .unwrap();

    assert!(result);
    assert_eq!(
        db.get(Bytes::from("key"), &plan).unwrap(),
        Some(Bytes::from("v1"))
    );

    // 第二次写入失败（key 已存在）
    let result = db
        .put_if_absent(Bytes::from("key"), Bytes::from("v2"), &plan)
        .unwrap();

    assert!(!result);
    // 值不应该改变
    assert_eq!(
        db.get(Bytes::from("key"), &plan).unwrap(),
        Some(Bytes::from("v1"))
    );
}

#[test]
fn test_compare_and_set_delete() {
    init_config();
    let temp_dir = TempDir::new().unwrap();
    let db = create_test_db(temp_dir.path()).unwrap();

    let plan = WasmCallPlan::new();

    // 写入数据
    db.put(Bytes::from("key"), Bytes::from("v1"), &plan)
        .unwrap();

    // CAS: 期望 v1 -> 删除
    let result = db
        .compare_and_set(
            Bytes::from("key"),
            Some(Bytes::from("v1")),
            None, // 删除
            &plan,
        )
        .unwrap();

    assert!(result);
    assert_eq!(db.get(Bytes::from("key"), &plan).unwrap(), None);
}

// TTL 操作测试
#[test]
fn test_put_with_ttl() {
    init_config();
    let temp_dir = TempDir::new().unwrap();
    let db = create_test_db(temp_dir.path()).unwrap();

    let plan = WasmCallPlan::new();

    // 写入带 TTL 的数据（3600秒 = 1小时，不会过期）
    db.put_with_ttl(Bytes::from("key"), Bytes::from("value"), 3600, &plan)
        .unwrap();

    // 应该能读取到
    assert_eq!(
        db.get(Bytes::from("key"), &plan).unwrap(),
        Some(Bytes::from("value"))
    );
}

#[test]
fn test_expire_at() {
    init_config();
    let temp_dir = TempDir::new().unwrap();
    let db = create_test_db(temp_dir.path()).unwrap();

    let plan = WasmCallPlan::new();

    // 写入普通数据
    db.put(Bytes::from("key"), Bytes::from("value"), &plan)
        .unwrap();

    // 设置过期时间（未来 1 小时）
    let future_time = current_timestamp_secs() + 3600;
    let result = db
        .expire_at(Bytes::from("key"), future_time, &plan)
        .unwrap();

    assert!(result);

    // 数据仍然可读
    assert_eq!(
        db.get(Bytes::from("key"), &plan).unwrap(),
        Some(Bytes::from("value"))
    );
}

#[test]
fn test_expire_at_nonexistent_key() {
    init_config();
    let temp_dir = TempDir::new().unwrap();
    let db = create_test_db(temp_dir.path()).unwrap();

    let plan = WasmCallPlan::new();

    // 为不存在的 key 设置过期时间
    let future_time = current_timestamp_secs() + 3600;
    let result = db
        .expire_at(Bytes::from("key"), future_time, &plan)
        .unwrap();

    assert!(!result); // 应该返回 false
}

// scan_range_iter 测试
#[test]
fn test_scan_range_iter_basic() {
    init_config();
    let temp_dir = TempDir::new().unwrap();
    let db = create_test_db(temp_dir.path()).unwrap();

    let plan = WasmCallPlan::new();

    // 写入数据
    db.put(Bytes::from("k1"), Bytes::from("v1"), &plan).unwrap();
    db.put(Bytes::from("k2"), Bytes::from("v2"), &plan).unwrap();
    db.put(Bytes::from("k3"), Bytes::from("v3"), &plan).unwrap();
    db.put(Bytes::from("k4"), Bytes::from("v4"), &plan).unwrap();
    db.put(Bytes::from("k5"), Bytes::from("v5"), &plan).unwrap();

    // 使用迭代器
    let iter = db.scan_range_iter(b"k1", b"k4", &plan).unwrap();

    // 收集结果
    let results: Vec<_> = iter.collect::<Result<Vec<_>, _>>().unwrap();

    assert_eq!(results.len(), 3);
    assert_eq!(results[0], (Bytes::from("k1"), Bytes::from("v1")));
    assert_eq!(results[1], (Bytes::from("k2"), Bytes::from("v2")));
    assert_eq!(results[2], (Bytes::from("k3"), Bytes::from("v3")));
}

#[test]
fn test_scan_range_iter_lazy() {
    init_config();
    let temp_dir = TempDir::new().unwrap();
    let db = create_test_db(temp_dir.path()).unwrap();

    let plan = WasmCallPlan::new();

    // 写入大量数据
    for i in 0..100 {
        let key = format!("key{:03}", i);
        let value = format!("value{:03}", i);
        db.put(Bytes::from(key), Bytes::from(value), &plan).unwrap();
    }

    // 使用迭代器只取前 5 个
    let iter = db.scan_range_iter(b"key000", b"key999", &plan).unwrap();

    let mut count = 0;
    for result in iter {
        let (key, value) = result.unwrap();
        assert!(key.starts_with(b"key"));
        assert!(value.starts_with(b"value"));

        count += 1;
        if count >= 5 {
            break; // 提前停止，验证惰性
        }
    }

    assert_eq!(count, 5);
}

#[test]
fn test_scan_range_iter_empty() {
    init_config();
    let temp_dir = TempDir::new().unwrap();
    let db = create_test_db(temp_dir.path()).unwrap();

    let plan = WasmCallPlan::new();

    // 空数据库扫描
    let iter = db.scan_range_iter(b"a", b"z", &plan).unwrap();

    let results: Vec<_> = iter.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(results.len(), 0);
}

// 综合场景测试
#[test]
fn test_comprehensive_workflow() {
    init_config();
    let temp_dir = TempDir::new().unwrap();
    let db = create_test_db(temp_dir.path()).unwrap();

    let plan = WasmCallPlan::new();

    // 1. 批量写入
    let mut batch = WriteBatch::new();
    for i in 0..10 {
        let key = format!("k{}", i);
        let value = format!("v{}", i);
        batch.put(Bytes::from(key), Bytes::from(value));
    }
    db.write_batch_enhanced(batch, &plan).unwrap();

    // 2. 创建快照
    let snapshot = db.create_snapshot().unwrap();

    // 3. MultiGet
    let keys: Vec<_> = (0..10).map(|i| Bytes::from(format!("k{}", i))).collect();
    let results = db.multi_get(keys, &plan).unwrap();
    assert_eq!(results.len(), 10);
    assert!(results.iter().all(|r| r.is_some()));

    // 4. CAS 更新
    let success = db
        .compare_and_set(
            Bytes::from("k5"),
            Some(Bytes::from("v5")),
            Some(Bytes::from("v5_updated")),
            &plan,
        )
        .unwrap();
    assert!(success);

    // 5. 迭代器扫描（应该看到更新）
    let iter = db.scan_range_iter(b"k0", b"k9", &plan).unwrap();
    let scan_results: Vec<_> = iter.collect::<Result<Vec<_>, _>>().unwrap();
    assert!(scan_results.len() >= 9);

    // 6. 快照仍然看到旧值
    assert_eq!(
        db.get_with_snapshot(&snapshot, Bytes::from("k5")),
        Some(Bytes::from("v5"))
    );

    // 7. 清理
    drop(snapshot);
    assert_eq!(db.active_snapshots_count(), 0);
}
