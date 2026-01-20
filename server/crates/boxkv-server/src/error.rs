/// 服务端错误类型与 gRPC 映射
///
/// 统一错误出口、清晰的分类，与 tonic::Status 的稳定映射，便于客户端感知与排错。
use tonic::Status;

#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    #[error("Database error: {0}")]
    Database(#[from] boxkv_core::db::error::DBError),

    #[error("Wasm error: {0}")]
    Wasm(#[from] boxkv_wasm::error::WasmError),

    #[error("Invalid argument: {0}")]
    InvalidArgument(String),

    #[error("Internal error: {0}")]
    Internal(String),

    #[error("Resource exhausted: {0}")]
    ResourceExhausted(String),
}

impl From<ServerError> for Status {
    fn from(err: ServerError) -> Self {
        // 将内部错误统一映射为 gRPC Status，保持返回码与语义的一致性
        match err {
            ServerError::Database(db_err) => match db_err {
                boxkv_core::db::error::DBError::PluginRejected(reason) => {
                    Status::failed_precondition(format!("Plugin rejected: {}", reason))
                }
                boxkv_core::db::error::DBError::KeyTooLarge(actual, max) => {
                    Status::invalid_argument(format!("Key too large: {} > {}", actual, max))
                }
                boxkv_core::db::error::DBError::ValueTooLarge(actual, max) => {
                    Status::invalid_argument(format!("Value too large: {} > {}", actual, max))
                }
                boxkv_core::db::error::DBError::WriteStalled => {
                    Status::resource_exhausted("Write stalled")
                }
                boxkv_core::db::error::DBError::Closed => Status::unavailable("Database closed"),
                _ => Status::internal(format!("DB error: {}", db_err)),
            },
            ServerError::Wasm(wasm_err) => match wasm_err {
                boxkv_wasm::error::WasmError::PluginIdNotFound(_) => {
                    Status::not_found("Plugin not found")
                }
                boxkv_wasm::error::WasmError::Trap(_) => Status::aborted("Plugin trapped"),
                _ => Status::internal(format!("Wasm error: {}", wasm_err)),
            },
            ServerError::InvalidArgument(msg) => Status::invalid_argument(msg),
            ServerError::Internal(msg) => Status::internal(msg),
            ServerError::ResourceExhausted(msg) => Status::resource_exhausted(msg),
        }
    }
}

pub type Result<T> = std::result::Result<T, ServerError>;
