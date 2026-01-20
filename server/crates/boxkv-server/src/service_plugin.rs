use bytes::Bytes;
/// 插件管理 gRPC 服务实现
///
/// 说明：负责插件的上传、校验、查询与清理。严格做参数检查与错误映射，
/// 关键操作打点便于排查与审计。
use std::sync::Arc;
use tonic::{Request, Response, Status};
use tracing::{error, info, instrument, warn};

use crate::error::{Result, ServerError};
use crate::generated::boxkv::plugin_server::Plugin;
use crate::generated::boxkv::*;
use boxkv_wasm::plugin::{HookType, PluginService};

/// 插件服务实现。包装底层 `PluginService`，
/// 统一处理参数解析、hook 类型校验与错误返回码。
pub struct PluginServiceImpl {
    plugin_service: Arc<PluginService>,
}

impl PluginServiceImpl {
    pub fn new(plugin_service: Arc<PluginService>) -> Self {
        Self { plugin_service }
    }

    /// 将字符串形式的 hook 类型解析为枚举。限定四种合法值，其他情况返回参数错误。
    fn parse_hook_type(hook: &str) -> Result<HookType> {
        match hook {
            "PreWrite" => Ok(HookType::PreWrite),
            "PostWrite" => Ok(HookType::PostWrite),
            "OnRead" => Ok(HookType::OnRead),
            "ScanFilter" => Ok(HookType::ScanFilter),
            _ => Err(ServerError::InvalidArgument(format!(
                "Invalid hook type: {}. Must be PreWrite/PostWrite/OnRead/ScanFilter",
                hook
            ))),
        }
    }
}

#[tonic::async_trait]
impl Plugin for PluginServiceImpl {
    #[instrument(skip(self, request), fields(name = %request.get_ref().name, version = %request.get_ref().version))]
    async fn upload(
        &self,
        request: Request<UploadRequest>,
    ) -> std::result::Result<Response<UploadResponse>, Status> {
        let req = request.into_inner();

        if req.name.is_empty() {
            return Err(Status::invalid_argument("Plugin name cannot be empty"));
        }
        if req.version.is_empty() {
            return Err(Status::invalid_argument("Plugin version cannot be empty"));
        }
        if req.wasm.is_empty() {
            return Err(Status::invalid_argument("Wasm binary cannot be empty"));
        }

        let hook_type = Self::parse_hook_type(&req.hook).map_err(Status::from)?;

        let result = self
            .plugin_service
            .upload(req.name, req.version, hook_type, Bytes::from(req.wasm))
            .map_err(|e| Status::from(ServerError::Wasm(e)))?;

        info!(plugin_id = %result.id, fingerprint = %result.fingerprint, "Plugin uploaded");

        Ok(Response::new(UploadResponse {
            plugin_id: result.id.to_string(),
            fingerprint: result.fingerprint.to_string(),
        }))
    }

    #[instrument(skip(self, request), fields(name = %request.get_ref().name, version = %request.get_ref().version))]
    async fn ensure(
        &self,
        request: Request<EnsureRequest>,
    ) -> std::result::Result<Response<EnsureResponse>, Status> {
        let req = request.into_inner();

        if req.name.is_empty() {
            return Err(Status::invalid_argument("Plugin name cannot be empty"));
        }
        if req.version.is_empty() {
            return Err(Status::invalid_argument("Plugin version cannot be empty"));
        }
        if req.fingerprint.is_empty() {
            return Err(Status::invalid_argument("Fingerprint cannot be empty"));
        }

        let hook_type = Self::parse_hook_type(&req.hook).map_err(Status::from)?;
        let fingerprint = boxkv_wasm::plugin::Fingerprint::from_hex(req.fingerprint)
            .map_err(|e| Status::invalid_argument(format!("Invalid fingerprint: {}", e)))?;

        let result = self
            .plugin_service
            .ensure(req.name, req.version, hook_type, fingerprint)
            .map_err(|e| Status::from(ServerError::Wasm(e)))?;

        if result.found {
            info!(plugin_id = ?result.id, is_latest = result.is_latest, "Plugin ensured");
        } else {
            warn!("Plugin not found during ensure");
        }

        Ok(Response::new(EnsureResponse {
            exists: result.found,
            plugin_id: result.id.map(|id| id.to_string()).unwrap_or_default(),
            fingerprint_match: result.is_latest,
        }))
    }

    #[instrument(skip(self, request), fields(name = %request.get_ref().name))]
    async fn get_latest(
        &self,
        request: Request<GetLatestRequest>,
    ) -> std::result::Result<Response<GetLatestResponse>, Status> {
        let req = request.into_inner();

        if req.name.is_empty() {
            return Err(Status::invalid_argument("Plugin name cannot be empty"));
        }

        let hook_type = Self::parse_hook_type(&req.hook).map_err(Status::from)?;

        let result = self
            .plugin_service
            .get_latest(req.name.clone(), "*".to_string(), hook_type)
            .map_err(|e| Status::from(ServerError::Wasm(e)))?;

        info!(latest_id = %result.latest_id, "Got latest plugin");

        Ok(Response::new(GetLatestResponse {
            found: true,
            plugin_id: result.latest_id.to_string(),
            version: String::new(),
            fingerprint: result.latest_fingerprint.to_string(),
        }))
    }

    #[instrument(skip(self, request), fields(name = %request.get_ref().name, major_version = request.get_ref().major_version))]
    async fn purge(
        &self,
        request: Request<PurgeRequest>,
    ) -> std::result::Result<Response<PurgeResponse>, Status> {
        let req = request.into_inner();

        if req.name.is_empty() {
            return Err(Status::invalid_argument("Plugin name cannot be empty"));
        }

        let version_str = if req.major_version == 0 {
            "*".to_string()
        } else {
            format!("{}.0.0", req.major_version)
        };

        let mut total_deleted = 0u32;

        for hook in [
            HookType::PreWrite,
            HookType::PostWrite,
            HookType::OnRead,
            HookType::ScanFilter,
        ] {
            if let Ok(result) =
                self.plugin_service
                    .purge(req.name.clone(), version_str.clone(), hook)
            {
                total_deleted += result.deleted_plugin_count as u32;
            }
        }

        info!(purged_count = total_deleted, "Plugins purged");

        Ok(Response::new(PurgeResponse {
            purged_count: total_deleted,
        }))
    }
}
