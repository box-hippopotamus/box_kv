/// boxkv-server crate 导出与测试辅助
///
/// 说明：对外仅导出错误类型与两个服务实现模块；
/// 测试场景下提供轻量的 gRPC 启动与关闭工具，便于集成测试。
pub mod error;
pub mod generated;
pub mod service_boxkv;
pub mod service_plugin;

pub mod test_support {
    use std::net::SocketAddr;
    use std::sync::{Arc, mpsc};

    use boxkv_common::config::GlobalConfig;
    use boxkv_core::BoxKV;
    use boxkv_executor::{GlobalScheduler, SchedulerConfig};
    use boxkv_wasm::plugin::{FsBlobStore, FsRegistry, PluginService};
    use boxkv_wasm::{RuntimeConfig, WasmHookProvider, WasmRuntime};
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::transport::Server;

    use crate::generated::boxkv::box_kv_server::BoxKvServer;
    use crate::generated::boxkv::plugin_server::PluginServer;
    use crate::service_boxkv::BoxKvService;
    use crate::service_plugin::PluginServiceImpl;

    /// 测试用服务器句柄：记录监听地址与优雅关闭通道
    pub struct TestServerHandle {
        pub addr: SocketAddr,
        pub shutdown_tx: tokio::sync::oneshot::Sender<()>,
    }

    /// 在独立运行时中启动一个最小化的 gRPC 服务器，返回可关闭句柄
    pub async fn spawn_server_for_test()
    -> Result<TestServerHandle, Box<dyn std::error::Error + Send + Sync>> {
        let (tx, rx) = mpsc::channel();

        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("runtime");

            let _ = rt.block_on(async move {
                let _ = tracing_subscriber::fmt()
                    .with_env_filter(
                        tracing_subscriber::EnvFilter::try_from_default_env()
                            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
                    )
                    .with_target(false)
                    .with_thread_ids(true)
                    .with_line_number(true)
                    .try_init();

                let _ = GlobalConfig::init(GlobalConfig::default());
                let scheduler = Arc::new(
                    GlobalScheduler::new(SchedulerConfig::default())
                        .expect("Failed to create scheduler"),
                );

                let tmpdir = tempfile::tempdir().expect("tmpdir");
                let db_path = tmpdir.path().join("db");
                let wasm_path = tmpdir.path().join("wasm");
                std::fs::create_dir_all(&db_path).expect("db dir");
                std::fs::create_dir_all(wasm_path.join("blobs")).expect("blobs");
                std::fs::create_dir_all(wasm_path.join("registry")).expect("registry");

                let blobs = Arc::new(FsBlobStore::new(wasm_path.join("blobs")).expect("blobs new"));
                let registry =
                    Arc::new(FsRegistry::new(wasm_path.join("registry")).expect("reg new"));
                let plugin_service = Arc::new(PluginService::new(blobs, registry));

                let runtime_config = RuntimeConfig::default();
                let wasm_runtime = Arc::new(
                    WasmRuntime::new(runtime_config, Arc::clone(&plugin_service)).expect("wasm rt"),
                );
                let wasm_provider = Arc::new(WasmHookProvider::new(Arc::clone(&wasm_runtime)));

                let db = Arc::new(
                    BoxKV::open(
                        &db_path,
                        Arc::clone(&scheduler),
                        wasm_provider.clone() as Arc<dyn boxkv_core::HookProvider>,
                    )
                    .expect("db open"),
                );

                let boxkv_service = BoxKvService::new(Arc::clone(&db));
                let plugin_service_impl = PluginServiceImpl::new(Arc::clone(&plugin_service));

                let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
                let reflection_service = tonic_reflection::server::Builder::configure()
                    .register_encoded_file_descriptor_set(crate::generated::FILE_DESCRIPTOR_SET)
                    .build_v1()
                    .expect("reflect");

                let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
                let addr = listener.local_addr().expect("addr");
                let incoming = TcpListenerStream::new(listener);
                let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

                let _ = tx.send((addr, shutdown_tx));

                let _ = health_reporter
                    .set_serving::<BoxKvServer<BoxKvService>>()
                    .await;
                let _ = health_reporter
                    .set_serving::<PluginServer<PluginServiceImpl>>()
                    .await;
                let _ = Server::builder()
                    .add_service(BoxKvServer::new(boxkv_service))
                    .add_service(PluginServer::new(plugin_service_impl))
                    .add_service(health_service)
                    .add_service(reflection_service)
                    .serve_with_incoming_shutdown(incoming, async move {
                        let _ = shutdown_rx.await;
                    })
                    .await;
                let _ = db.close();
            });
        });

        let (addr, shutdown_tx) = rx.recv().expect("recv");
        Ok(TestServerHandle { addr, shutdown_tx })
    }
}
use std::net::SocketAddr;
use std::sync::Arc;

use boxkv_common::config::GlobalConfig;
use boxkv_core::BoxKV;
use boxkv_executor::{GlobalScheduler, SchedulerConfig};
use boxkv_wasm::plugin::{FsBlobStore, FsRegistry, PluginService};
use boxkv_wasm::{RuntimeConfig, WasmHookProvider, WasmRuntime};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tracing::info;

use generated::boxkv::box_kv_server::BoxKvServer;
use generated::boxkv::plugin_server::PluginServer;
use service_boxkv::BoxKvService;
use service_plugin::PluginServiceImpl;

/// 测试用服务器句柄：记录监听地址与优雅关闭通道
pub struct TestServerHandle {
    pub addr: SocketAddr,
    pub shutdown_tx: oneshot::Sender<()>,
}

/// 在当前异步运行时中启动一个最小化的 gRPC 服务器，返回可关闭句柄
pub async fn spawn_server_for_test()
-> Result<TestServerHandle, Box<dyn std::error::Error + Send + Sync>> {
    // 初始化日志(测试场景简化)
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_target(false)
        .with_thread_ids(true)
        .with_line_number(true)
        .try_init();

    // 全局配置
    let _ = GlobalConfig::init(GlobalConfig::default());

    // 运行时与依赖
    let scheduler = Arc::new(GlobalScheduler::new(SchedulerConfig::default())?);

    // 使用临时目录
    let tmpdir = tempfile::tempdir()?;
    let db_path = tmpdir.path().join("db");
    let wasm_path = tmpdir.path().join("wasm");
    std::fs::create_dir_all(&db_path)?;
    std::fs::create_dir_all(wasm_path.join("blobs"))?;
    std::fs::create_dir_all(wasm_path.join("registry"))?;

    let blobs = Arc::new(FsBlobStore::new(wasm_path.join("blobs"))?);
    let registry = Arc::new(FsRegistry::new(wasm_path.join("registry"))?);
    let plugin_service = Arc::new(PluginService::new(blobs, registry));

    let runtime_config = RuntimeConfig::default();
    let wasm_runtime = Arc::new(WasmRuntime::new(
        runtime_config,
        Arc::clone(&plugin_service),
    )?);
    let wasm_provider = Arc::new(WasmHookProvider::new(Arc::clone(&wasm_runtime)));

    let db = Arc::new(BoxKV::open(
        &db_path,
        Arc::clone(&scheduler),
        wasm_provider.clone() as Arc<dyn boxkv_core::HookProvider>,
    )?);

    // gRPC 服务
    let boxkv_service = BoxKvService::new(Arc::clone(&db));
    let plugin_service_impl = PluginServiceImpl::new(Arc::clone(&plugin_service));

    let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
    // 反射
    let reflection_service = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(generated::FILE_DESCRIPTOR_SET)
        .build_v1()?;

    // 绑定 127.0.0.1:0 动态端口
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let incoming = TcpListenerStream::new(listener);

    // 优雅关闭通道
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    // 启动 server
    tokio::spawn(async move {
        let _ = health_reporter
            .set_serving::<BoxKvServer<BoxKvService>>()
            .await;
        let _ = health_reporter
            .set_serving::<PluginServer<PluginServiceImpl>>()
            .await;

        let _ = Server::builder()
            .add_service(BoxKvServer::new(boxkv_service))
            .add_service(PluginServer::new(plugin_service_impl))
            .add_service(health_service)
            .add_service(reflection_service)
            .serve_with_incoming_shutdown(incoming, async move {
                let _ = shutdown_rx.await;
            })
            .await;

        let _ = db.close();
    });

    Ok(TestServerHandle { addr, shutdown_tx })
}
