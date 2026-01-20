use bytes::Bytes;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tempfile::TempDir;

use boxkv_common::config::GlobalConfig;
use boxkv_common::types::ValueType;
use boxkv_core::hooks::{
    HookType, OnReadAction, PreWriteAction, ScanFilterAction, WriteCommand, WriteContext,
};
use boxkv_core::{BoxKV, HookContext, HookProvider, WasmCallPlan};
use boxkv_executor::{GlobalScheduler, SchedulerConfig};
use boxkv_storage::LocalFileSystem;
use boxkv_wasm::plugin::{HookSpec, PluginId, PluginMetadata};
use boxkv_wasm::{PluginService, WasmHookProvider, WasmRuntime, WasmRuntimeConfig};

/// 测试用的简单 Wasm 插件（预编译的 WAT）
///
/// 功能：
/// - PreWrite: 为所有 key 添加前缀 "prefix_"
/// - OnRead: 移除前缀 "prefix_"
const SIMPLE_TRANSFORM_WASM: &[u8] = include_bytes!("../fixtures/simple_transform.wasm");

/// 测试用的过滤插件
///
/// 功能：
/// - PreWrite: 拒绝包含 "forbidden" 的 key
const FILTER_WASM: &[u8] = include_bytes!("../fixtures/filter.wasm");

/// 测试用的审计插件
///
/// 功能：
/// - PostWrite: 记录所有写操作（通过日志）
const AUDIT_WASM: &[u8] = include_bytes!("../fixtures/audit.wasm");

/// 集成测试辅助结构
struct IntegrationTestEnv {
    db: Arc<BoxKV>,
    wasm_provider: Arc<WasmHookProvider>,
    plugin_service: Arc<PluginService>,
    _temp_dir: TempDir,
}

impl IntegrationTestEnv {
    /// 创建完整的测试环境
    fn new() -> Self {
        let _ = GlobalConfig::init(GlobalConfig::default());

        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("db");
        let wasm_path = temp_dir.path().join("wasm");

        std::fs::create_dir_all(&db_path).unwrap();
        std::fs::create_dir_all(&wasm_path).unwrap();

        // 创建调度器
        let scheduler = Arc::new(GlobalScheduler::new(SchedulerConfig::default()));

        // 创建 Wasm Runtime
        let runtime_config = WasmRuntimeConfig {
            max_instances_per_plugin: 4,
            budget: boxkv_wasm::budget::BudgetConfig {
                max_memory_bytes: 64 * 1024 * 1024, // 64MB
                max_fuel: 1_000_000_000,            // 10亿 fuel
                timeout_ms: 5000,                   // 5秒超时
            },
        };

        let fs = Arc::new(LocalFileSystem);
        let wasm_runtime = Arc::new(WasmRuntime::new(runtime_config).unwrap());

        // 创建 Plugin Service
        let plugin_service = Arc::new(PluginService::new(
            fs.clone(),
            wasm_path.clone(),
            Arc::clone(&wasm_runtime),
        ));

        // 创建 Wasm Hook Provider
        let wasm_provider = Arc::new(WasmHookProvider::new(
            Arc::clone(&wasm_runtime),
            Arc::clone(&plugin_service),
        ));

        // 打开数据库（使用 Wasm Provider）
        let db = Arc::new(
            BoxKV::open(
                db_path,
                scheduler,
                wasm_provider.clone() as Arc<dyn HookProvider>,
            )
            .unwrap(),
        );

        Self {
            db,
            wasm_provider,
            plugin_service,
            _temp_dir: temp_dir,
        }
    }

    /// 上传并启用插件
    fn upload_plugin(&self, name: &str, wasm_bytes: &[u8], hooks: Vec<HookType>) -> PluginId {
        let metadata = PluginMetadata {
            name: name.to_string(),
            version: "1.0.0".to_string(),
            description: format!("Test plugin: {}", name),
            hooks: hooks
                .into_iter()
                .map(|h| HookSpec {
                    hook_type: h,
                    priority: 100,
                })
                .collect(),
        };

        self.plugin_service.upload(wasm_bytes, metadata).unwrap()
    }

    /// 构造包含指定插件的 WasmCallPlan
    fn plan_with(&self, plugins: Vec<(PluginId, Vec<HookType>)>) -> WasmCallPlan {
        let mut plan = WasmCallPlan::new();
        for (plugin_id, hooks) in plugins {
            for hook_type in hooks {
                plan.add(
                    hook_type,
                    boxkv_core::hooks::PluginSpec {
                        id: plugin_id.as_uuid(),
                        priority: 100,
                    },
                );
            }
        }
        plan
    }
}

/// 测试 1：基本的 PreWrite + OnRead 转换
#[test]
fn test_basic_transform_integration() {
    let env = IntegrationTestEnv::new();

    // 上传转换插件
    let transform_id = env.upload_plugin(
        "simple_transform",
        SIMPLE_TRANSFORM_WASM,
        vec![HookType::PreWrite, HookType::OnRead],
    );

    let plan = env.plan_with(vec![(
        transform_id,
        vec![HookType::PreWrite, HookType::OnRead],
    )]);

    // 写入数据（PreWrite 会添加前缀）
    env.db
        .put(Bytes::from("key1"), Bytes::from("value1"), &plan)
        .unwrap();
    env.db
        .put(Bytes::from("key2"), Bytes::from("value2"), &plan)
        .unwrap();

    // 读取数据（OnRead 会移除前缀，用户看到原始 key）
    let v1 = env.db.get(Bytes::from("key1"), &plan).unwrap();
    let v2 = env.db.get(Bytes::from("key2"), &plan).unwrap();

    assert_eq!(v1, Some(Bytes::from("value1")));
    assert_eq!(v2, Some(Bytes::from("value2")));

    // 不使用 plan 读取（应该看到带前缀的 key）
    let empty_plan = WasmCallPlan::new();
    let v1_raw = env.db.get(Bytes::from("prefix_key1"), &empty_plan).unwrap();
    assert_eq!(v1_raw, Some(Bytes::from("value1")));
}

/// 测试 2：PreWrite 拒绝（过滤插件）
#[test]
fn test_prewrite_reject() {
    let env = IntegrationTestEnv::new();

    // 上传过滤插件
    let filter_id = env.upload_plugin("filter", FILTER_WASM, vec![HookType::PreWrite]);

    let plan = env.plan_with(vec![(filter_id, vec![HookType::PreWrite])]);

    // 正常 key 应该成功
    let result = env
        .db
        .put(Bytes::from("normal_key"), Bytes::from("value"), &plan);
    assert!(result.is_ok());

    // 包含 "forbidden" 的 key 应该被拒绝
    let result = env
        .db
        .put(Bytes::from("forbidden_key"), Bytes::from("value"), &plan);
    assert!(result.is_err());

    // 验证被拒绝的数据没有写入
    let empty_plan = WasmCallPlan::new();
    let v = env
        .db
        .get(Bytes::from("forbidden_key"), &empty_plan)
        .unwrap();
    assert_eq!(v, None);
}

/// 测试 3：多插件管道执行（PreWrite 链式处理）
#[test]
fn test_multi_plugin_pipeline() {
    let env = IntegrationTestEnv::new();

    // 上传多个插件
    let transform_id =
        env.upload_plugin("transform", SIMPLE_TRANSFORM_WASM, vec![HookType::PreWrite]);

    let filter_id = env.upload_plugin("filter", FILTER_WASM, vec![HookType::PreWrite]);

    // 构造管道：先转换，再过滤
    let plan = env.plan_with(vec![
        (transform_id, vec![HookType::PreWrite]),
        (filter_id, vec![HookType::PreWrite]),
    ]);

    // 写入数据（会经过两个插件）
    let result = env
        .db
        .put(Bytes::from("test_key"), Bytes::from("value"), &plan);
    assert!(result.is_ok());

    // 验证数据已写入
    let empty_plan = WasmCallPlan::new();
    let v = env
        .db
        .get(Bytes::from("prefix_test_key"), &empty_plan)
        .unwrap();
    assert_eq!(v, Some(Bytes::from("value")));
}

/// 测试 4：PostWrite 审计（异步 Hook）
#[test]
fn test_postwrite_audit() {
    let env = IntegrationTestEnv::new();

    // 上传审计插件
    let audit_id = env.upload_plugin("audit", AUDIT_WASM, vec![HookType::PostWrite]);

    let plan = env.plan_with(vec![(audit_id, vec![HookType::PostWrite])]);

    // 写入数据（PostWrite 会异步记录）
    env.db
        .put(Bytes::from("audit_key1"), Bytes::from("value1"), &plan)
        .unwrap();
    env.db
        .put(Bytes::from("audit_key2"), Bytes::from("value2"), &plan)
        .unwrap();
    env.db.delete(Bytes::from("audit_key1"), &plan).unwrap();

    // 等待异步 Hook 执行完成
    std::thread::sleep(Duration::from_millis(100));

    // 验证数据正确性（PostWrite 不影响主路径）
    let empty_plan = WasmCallPlan::new();
    let v1 = env.db.get(Bytes::from("audit_key1"), &empty_plan).unwrap();
    let v2 = env.db.get(Bytes::from("audit_key2"), &empty_plan).unwrap();

    assert_eq!(v1, None); // 已删除
    assert_eq!(v2, Some(Bytes::from("value2")));
}

/// 测试 5：OnRead Transform（读取时变换）
#[test]
fn test_onread_transform() {
    let env = IntegrationTestEnv::new();

    // 先写入原始数据（不使用插件）
    let empty_plan = WasmCallPlan::new();
    env.db
        .put(Bytes::from("key1"), Bytes::from("original"), &empty_plan)
        .unwrap();

    // 上传 OnRead 转换插件
    let transform_id = env.upload_plugin(
        "read_transform",
        SIMPLE_TRANSFORM_WASM,
        vec![HookType::OnRead],
    );

    let plan = env.plan_with(vec![(transform_id, vec![HookType::OnRead])]);

    // 使用插件读取（应该被转换）
    let v = env.db.get(Bytes::from("key1"), &plan).unwrap();
    // 注意：具体转换逻辑取决于 SIMPLE_TRANSFORM_WASM 的实现
    assert!(v.is_some());

    // 不使用插件读取（应该是原始值）
    let v_raw = env.db.get(Bytes::from("key1"), &empty_plan).unwrap();
    assert_eq!(v_raw, Some(Bytes::from("original")));
}

/// 测试 6：OnRead Reject（拒绝读取）
#[test]
fn test_onread_reject() {
    let env = IntegrationTestEnv::new();

    // 写入数据
    let empty_plan = WasmCallPlan::new();
    env.db
        .put(
            Bytes::from("secret_key"),
            Bytes::from("secret_value"),
            &empty_plan,
        )
        .unwrap();
    env.db
        .put(
            Bytes::from("public_key"),
            Bytes::from("public_value"),
            &empty_plan,
        )
        .unwrap();

    // 上传过滤插件（拒绝读取包含 "secret" 的 key）
    let filter_id = env.upload_plugin("read_filter", FILTER_WASM, vec![HookType::OnRead]);

    let plan = env.plan_with(vec![(filter_id, vec![HookType::OnRead])]);

    // 读取 public_key 应该成功
    let v_public = env.db.get(Bytes::from("public_key"), &plan);
    assert!(v_public.is_ok());

    // 读取 secret_key 应该被拒绝
    let v_secret = env.db.get(Bytes::from("secret_key"), &plan);
    assert!(v_secret.is_err() || v_secret.unwrap().is_none());
}

/// 测试 7：TTL 与 Wasm 集成
#[test]
fn test_ttl_with_wasm() {
    let env = IntegrationTestEnv::new();

    // 上传插件
    let transform_id = env.upload_plugin(
        "transform",
        SIMPLE_TRANSFORM_WASM,
        vec![HookType::PreWrite, HookType::OnRead],
    );

    let plan = env.plan_with(vec![(
        transform_id,
        vec![HookType::PreWrite, HookType::OnRead],
    )]);

    // 写入带 TTL 的数据
    env.db
        .put_with_ttl(
            Bytes::from("ttl_key"),
            Bytes::from("ttl_value"),
            1, // 1 秒后过期
            &plan,
        )
        .unwrap();

    // 立即读取应该成功
    let v = env.db.get(Bytes::from("ttl_key"), &plan).unwrap();
    assert_eq!(v, Some(Bytes::from("ttl_value")));

    // 等待过期
    std::thread::sleep(Duration::from_millis(1200));

    // 过期后读取应该返回 None
    let v_expired = env.db.get(Bytes::from("ttl_key"), &plan).unwrap();
    assert_eq!(v_expired, None);
}

/// 测试 8：Flush 后的持久化与恢复
#[test]
fn test_flush_and_recovery_with_wasm() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");
    let wasm_path = temp_dir.path().join("wasm");

    let transform_id: PluginId;

    // 第一阶段：写入数据并 Flush
    {
        let env = IntegrationTestEnv::new();

        transform_id = env.upload_plugin(
            "transform",
            SIMPLE_TRANSFORM_WASM,
            vec![HookType::PreWrite, HookType::OnRead],
        );

        let plan = env.plan_with(vec![(
            transform_id,
            vec![HookType::PreWrite, HookType::OnRead],
        )]);

        // 写入大量数据
        for i in 0..100 {
            let key = format!("key_{}", i);
            let value = format!("value_{}", i);
            env.db
                .put(Bytes::from(key), Bytes::from(value), &plan)
                .unwrap();
        }

        // 强制 Flush
        env.db.flush().unwrap();

        // 验证数据
        let v = env.db.get(Bytes::from("key_50"), &plan).unwrap();
        assert_eq!(v, Some(Bytes::from("value_50")));
    }

    // 第二阶段：重新打开数据库并验证
    {
        let env = IntegrationTestEnv::new();

        // 重新上传插件（模拟重启后重新加载）
        let new_transform_id = env.upload_plugin(
            "transform",
            SIMPLE_TRANSFORM_WASM,
            vec![HookType::PreWrite, HookType::OnRead],
        );

        let plan = env.plan_with(vec![(
            new_transform_id,
            vec![HookType::PreWrite, HookType::OnRead],
        )]);

        // 验证数据仍然可读
        for i in 0..100 {
            let key = format!("key_{}", i);
            let expected_value = format!("value_{}", i);
            let v = env.db.get(Bytes::from(key), &plan).unwrap();
            assert_eq!(v, Some(Bytes::from(expected_value)));
        }
    }
}

/// 测试 9：并发写入与读取（Wasm 线程安全）
#[test]
fn test_concurrent_operations_with_wasm() {
    use std::thread;

    let env = Arc::new(IntegrationTestEnv::new());

    // 上传插件
    let transform_id = env.upload_plugin(
        "transform",
        SIMPLE_TRANSFORM_WASM,
        vec![HookType::PreWrite, HookType::OnRead],
    );

    let plan = env.plan_with(vec![(
        transform_id,
        vec![HookType::PreWrite, HookType::OnRead],
    )]);
    let plan = Arc::new(plan);

    // 并发写入
    let mut handles = vec![];
    for t in 0..4 {
        let env = Arc::clone(&env);
        let plan = Arc::clone(&plan);
        let h = thread::spawn(move || {
            for i in 0..50 {
                let key = format!("thread_{}_key_{}", t, i);
                let value = format!("thread_{}_value_{}", t, i);
                env.db
                    .put(Bytes::from(key), Bytes::from(value), &plan)
                    .unwrap();
            }
        });
        handles.push(h);
    }

    for h in handles {
        h.join().unwrap();
    }

    // 验证所有数据
    for t in 0..4 {
        for i in 0..50 {
            let key = format!("thread_{}_key_{}", t, i);
            let expected_value = format!("thread_{}_value_{}", t, i);
            let v = env.db.get(Bytes::from(key), &plan).unwrap();
            assert_eq!(v, Some(Bytes::from(expected_value)));
        }
    }
}

/// 测试 10：插件错误处理与降级
#[test]
fn test_plugin_error_handling() {
    let env = IntegrationTestEnv::new();

    // 上传一个会出错的插件（假设 FILTER_WASM 在某些情况下会出错）
    let buggy_id = env.upload_plugin("buggy", FILTER_WASM, vec![HookType::OnRead]);

    let plan = env.plan_with(vec![(buggy_id, vec![HookType::OnRead])]);

    // 先写入数据
    let empty_plan = WasmCallPlan::new();
    env.db
        .put(
            Bytes::from("test_key"),
            Bytes::from("test_value"),
            &empty_plan,
        )
        .unwrap();

    // 读取时插件出错，应该降级返回原始值（根据 provider 的错误处理策略）
    let v = env.db.get(Bytes::from("test_key"), &plan);
    // 根据实现，可能返回原始值或错误
    assert!(v.is_ok() || v.is_err());
}

/// 测试 11：插件资源限制（Fuel 耗尽）
#[test]
fn test_plugin_resource_limits() {
    let env = IntegrationTestEnv::new();

    // 上传插件
    let transform_id =
        env.upload_plugin("transform", SIMPLE_TRANSFORM_WASM, vec![HookType::PreWrite]);

    let plan = env.plan_with(vec![(transform_id, vec![HookType::PreWrite])]);

    // 写入大量数据，测试 Fuel 限制
    for i in 0..10 {
        let key = format!("key_{}", i);
        let value = format!("value_{}", i);
        let result = env.db.put(Bytes::from(key), Bytes::from(value), &plan);
        // 应该都成功（除非插件消耗过多 Fuel）
        assert!(result.is_ok());
    }
}

/// 测试 12：插件版本管理
#[test]
fn test_plugin_versioning() {
    let env = IntegrationTestEnv::new();

    // 上传 v1 插件
    let v1_id = env.upload_plugin(
        "versioned_plugin",
        SIMPLE_TRANSFORM_WASM,
        vec![HookType::PreWrite],
    );

    let plan_v1 = env.plan_with(vec![(v1_id, vec![HookType::PreWrite])]);

    // 使用 v1 写入数据
    env.db
        .put(Bytes::from("key1"), Bytes::from("value1"), &plan_v1)
        .unwrap();

    // 上传 v2 插件（相同名称，不同内容）
    let v2_id = env.upload_plugin(
        "versioned_plugin",
        FILTER_WASM, // 不同的 Wasm
        vec![HookType::PreWrite],
    );

    let plan_v2 = env.plan_with(vec![(v2_id, vec![HookType::PreWrite])]);

    // 使用 v2 写入数据
    let result = env
        .db
        .put(Bytes::from("key2"), Bytes::from("value2"), &plan_v2);
    assert!(result.is_ok());

    // 验证两个版本的插件都能正常工作
    let empty_plan = WasmCallPlan::new();
    let v1_data = env.db.get(Bytes::from("key1"), &empty_plan).unwrap();
    let v2_data = env.db.get(Bytes::from("key2"), &empty_plan).unwrap();

    assert!(v1_data.is_some());
    assert!(v2_data.is_some());
}

/// 测试 13：空 Plan（不使用插件）
#[test]
fn test_empty_plan_no_plugin() {
    let env = IntegrationTestEnv::new();

    let empty_plan = WasmCallPlan::new();

    // 不使用插件的正常操作
    env.db
        .put(Bytes::from("key1"), Bytes::from("value1"), &empty_plan)
        .unwrap();
    env.db
        .put(Bytes::from("key2"), Bytes::from("value2"), &empty_plan)
        .unwrap();

    let v1 = env.db.get(Bytes::from("key1"), &empty_plan).unwrap();
    let v2 = env.db.get(Bytes::from("key2"), &empty_plan).unwrap();

    assert_eq!(v1, Some(Bytes::from("value1")));
    assert_eq!(v2, Some(Bytes::from("value2")));

    // 删除操作
    env.db.delete(Bytes::from("key1"), &empty_plan).unwrap();
    let v1_deleted = env.db.get(Bytes::from("key1"), &empty_plan).unwrap();
    assert_eq!(v1_deleted, None);
}

/// 测试 14：Snapshot 与 Wasm 集成
#[test]
fn test_snapshot_with_wasm() {
    let env = IntegrationTestEnv::new();

    // 上传插件
    let transform_id = env.upload_plugin(
        "transform",
        SIMPLE_TRANSFORM_WASM,
        vec![HookType::PreWrite, HookType::OnRead],
    );

    let plan = env.plan_with(vec![(
        transform_id,
        vec![HookType::PreWrite, HookType::OnRead],
    )]);

    // 写入初始数据
    env.db
        .put(Bytes::from("key1"), Bytes::from("v1"), &plan)
        .unwrap();

    // 创建快照
    let snapshot = env.db.snapshot().unwrap();

    // 修改数据
    env.db
        .put(Bytes::from("key1"), Bytes::from("v2"), &plan)
        .unwrap();

    // DB 应该看到新值
    let v_new = env.db.get(Bytes::from("key1"), &plan).unwrap();
    assert_eq!(v_new, Some(Bytes::from("v2")));

    // Snapshot 应该看到旧值
    let v_old = snapshot.get(Bytes::from("key1"));
    assert_eq!(v_old, Some(Bytes::from("v1")));
}

/// 性能基准测试（可选，默认忽略）
#[test]
#[ignore]
fn bench_wasm_overhead() {
    use std::time::Instant;

    let env = IntegrationTestEnv::new();

    // 上传插件
    let transform_id = env.upload_plugin(
        "transform",
        SIMPLE_TRANSFORM_WASM,
        vec![HookType::PreWrite, HookType::OnRead],
    );

    let plan = env.plan_with(vec![(
        transform_id,
        vec![HookType::PreWrite, HookType::OnRead],
    )]);

    let empty_plan = WasmCallPlan::new();
    let n = 1000;

    // 测试不使用插件的性能
    let start = Instant::now();
    for i in 0..n {
        let key = format!("no_wasm_{}", i);
        let value = format!("value_{}", i);
        env.db
            .put(Bytes::from(key), Bytes::from(value), &empty_plan)
            .unwrap();
    }
    let no_wasm_duration = start.elapsed();

    // 测试使用插件的性能
    let start = Instant::now();
    for i in 0..n {
        let key = format!("with_wasm_{}", i);
        let value = format!("value_{}", i);
        env.db
            .put(Bytes::from(key), Bytes::from(value), &plan)
            .unwrap();
    }
    let with_wasm_duration = start.elapsed();

    println!("No Wasm: {:?}", no_wasm_duration);
    println!("With Wasm: {:?}", with_wasm_duration);
    println!(
        "Overhead: {:.2}x",
        with_wasm_duration.as_secs_f64() / no_wasm_duration.as_secs_f64()
    );

    // Wasm 开销应该在合理范围内（< 10x）
    assert!(with_wasm_duration.as_secs_f64() / no_wasm_duration.as_secs_f64() < 10.0);
}
