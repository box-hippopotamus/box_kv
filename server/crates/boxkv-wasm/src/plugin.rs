//! Plugin Management Module
pub mod blob;
pub mod registry;
pub mod service;
pub mod types;

// 重新导出核心类型
pub use blob::{BlobStore, FsBlobStore, SharedBlobStore};
pub use registry::{FsRegistry, Registry, SharedRegistry};
pub use service::{
    EnsureResponse, GetLatestResponse, PluginService, PurgeResponse, UploadResponse,
};
pub use types::{Fingerprint, HookType, PluginId, PluginKey, PluginRecord};
