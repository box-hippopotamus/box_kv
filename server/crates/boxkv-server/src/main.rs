/// BoxKV gRPC 服务器
///
/// 说明：启动 gRPC（BoxKV 与 Plugin）、健康检查与反射；初始化日志、配置、执行器、Wasm 运行时；支持优雅关闭。
///
/// 启动：`cargo run --release -p boxkv-server`
mod error;
mod generated;
mod service_boxkv;
mod service_plugin;

use std::sync::Arc;
use tonic::transport::Server;
use tracing::{error, info};

use boxkv_common::config::GlobalConfig;
use boxkv_core::BoxKV;
use boxkv_executor::{GlobalScheduler, SchedulerConfig};
use boxkv_wasm::plugin::{FsBlobStore, FsRegistry, PluginService};
use boxkv_wasm::{WasmHookProvider, WasmRuntime};

use generated::boxkv::box_kv_server::BoxKvServer;
use generated::boxkv::plugin_server::PluginServer;
use service_boxkv::BoxKvService;
use service_plugin::PluginServiceImpl;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. 初始化日志（文件 + 控制台）。先读取默认配置以获得日志目录
    let config = GlobalConfig::default();
    let log_dir = &config.server.log_dir;
    let log_prefix = &config.server.log_file_prefix;

    // 创建日志目录
    std::fs::create_dir_all(log_dir)?;

    // 文件日志：按日滚动
    let file_appender = tracing_appender::rolling::daily(log_dir, log_prefix);
    let (non_blocking_file, _file_guard) = tracing_appender::non_blocking(file_appender);

    // 控制台日志
    let (non_blocking_stdout, _stdout_guard) = tracing_appender::non_blocking(std::io::stdout());

    // 保持 guard 存活到进程结束，防止日志丢失
    std::mem::forget(_file_guard);
    std::mem::forget(_stdout_guard);

    // 注册日志订阅：同时输出到文件与控制台
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

    tracing_subscriber::registry()
        .with(env_filter)
        .with(
            tracing_subscriber::fmt::layer()
                .with_writer(non_blocking_file)
                .with_target(true)
                .with_thread_ids(true)
                .with_thread_names(true)
                .with_line_number(true)
                .with_file(true)
                .json(),
        )
        .with(
            tracing_subscriber::fmt::layer()
                .with_writer(non_blocking_stdout)
                .with_target(false)
                .with_thread_ids(false)
                .with_ansi(true)
                .compact(),
        )
        .init();

    info!("==================== BoxKV gRPC Server ====================");
    info!("Version: v0.1.0");
    info!("Build: {}", env!("CARGO_PKG_VERSION"));
    info!("Log directory: {}", log_dir);
    info!("============================================================");

    // 2. 初始化全局配置
    GlobalConfig::init(GlobalConfig::default());
    let global_config = GlobalConfig::get();
    info!("Global configuration initialized");
    info!(
        "  Listen address: {}:{}",
        global_config.server.host, global_config.server.port
    );
    info!("  Database path: {}", global_config.storage.data_dir);
    info!("  WAL path: {}", global_config.storage.wal_dir);
    info!(
        "  Wasm blobs path: {}",
        global_config.wasm.plugin.blobs_path
    );
    info!(
        "  Wasm registry path: {}",
        global_config.wasm.plugin.registry_path
    );

    // 3. 创建数据目录
    std::fs::create_dir_all(&global_config.storage.data_dir)?;
    std::fs::create_dir_all(&global_config.storage.wal_dir)?;
    std::fs::create_dir_all(&global_config.wasm.plugin.blobs_path)?;
    std::fs::create_dir_all(&global_config.wasm.plugin.registry_path)?;
    info!("Data directories created");

    // 4. 创建全局调度器
    let scheduler_config = SchedulerConfig {
        worker_threads: global_config.executor.max_worker_threads,
        channel_capacity: global_config.executor.task_queue_capacity,
        ..SchedulerConfig::default()
    };
    let scheduler = Arc::new(GlobalScheduler::new(scheduler_config)?);
    info!("Global scheduler initialized");

    // 5. 初始化 Wasm 运行时与插件服务
    let blobs = Arc::new(FsBlobStore::new(&global_config.wasm.plugin.blobs_path)?);
    let registry = Arc::new(FsRegistry::new(&global_config.wasm.plugin.registry_path)?);
    let plugin_service = Arc::new(PluginService::new(blobs, registry));

    let wasm_runtime = Arc::new(WasmRuntime::from_global_config(Arc::clone(
        &plugin_service,
    ))?);
    let wasm_provider = Arc::new(WasmHookProvider::new(Arc::clone(&wasm_runtime)));
    info!(
        "Wasm runtime initialized (enabled={})",
        global_config.wasm.enabled
    );

    // 6. 打开数据库
    info!("Opening database...");
    let db = Arc::new(BoxKV::open(
        &global_config.storage.data_dir,
        Arc::clone(&scheduler),
        wasm_provider.clone() as Arc<dyn boxkv_core::HookProvider>,
    )?);
    info!("Database opened successfully");

    // 7. 构造 gRPC 服务
    let boxkv_service = BoxKvService::new(Arc::clone(&db));
    let plugin_service_impl = PluginServiceImpl::new(Arc::clone(&plugin_service));
    info!("gRPC services created");

    // 8. 健康检查
    let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter
        .set_serving::<BoxKvServer<BoxKvService>>()
        .await;
    health_reporter
        .set_serving::<PluginServer<PluginServiceImpl>>()
        .await;
    info!("Health check service initialized");

    // 9. 服务反射
    let reflection_service = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(generated::FILE_DESCRIPTOR_SET)
        .build_v1()
        .map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to build reflection service: {}", e),
            )
        })?;
    info!("Service reflection initialized");

    // 10. 解析监听地址
    let listen_addr = format!(
        "{}:{}",
        global_config.server.host, global_config.server.port
    );
    let addr = listen_addr.parse()?;
    info!("============================================================");
    info!("Server started successfully! Listening on: {}", addr);
    info!("============================================================");

    // 11. 启动 gRPC 服务器
    let result = Server::builder()
        .add_service(BoxKvServer::new(boxkv_service))
        .add_service(PluginServer::new(plugin_service_impl))
        .add_service(health_service)
        .add_service(reflection_service)
        .serve_with_shutdown(addr, async {
            tokio::signal::ctrl_c()
                .await
                .expect("Failed to listen for shutdown signal");
            info!("Received shutdown signal, starting graceful shutdown...");
        })
        .await;

    // 12. 优雅关闭
    info!("Closing database...");
    if let Err(e) = db.close() {
        error!("Failed to close database: {}", e);
    } else {
        info!("Database closed successfully");
    }

    info!("==================== Server stopped ====================");

    result.map_err(|e| {
        std::io::Error::new(std::io::ErrorKind::Other, format!("Server error: {}", e)).into()
    })
}
