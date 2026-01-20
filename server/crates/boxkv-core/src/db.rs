pub mod async_adapter;
pub mod batch;
pub mod db_view;
pub mod engine;
/// DB 模块
/// - 模块化组织 BoxKV 引擎实现
/// - error: 错误类型定义
/// - types: 内部类型（SuperVersion、WriteStallCondition）
/// - engine: BoxKV 核心引擎
/// - writer: 写入路径（含 write stall）
/// - batch: WriteBatch 批量写（含 WAL 组提交）
/// - reader: 读取路径
/// - snapshot: Snapshot 快照
/// - flusher: Flush 后台任务
/// - file_cleaner: 过期文件清理（SST/WAL/Manifest）
pub mod error;
pub mod file_cleaner;
pub mod flusher;
pub mod reader;
pub mod session;
pub mod snapshot;
pub mod types;
pub mod writer;

// 重新导出公共 API
pub use async_adapter::BoxKVAsync;
pub use batch::WriteBatch;
pub use engine::BoxKV;
pub use error::{DBError, Result};
pub use session::{SessionId, SessionManager};
pub use snapshot::Snapshot;
