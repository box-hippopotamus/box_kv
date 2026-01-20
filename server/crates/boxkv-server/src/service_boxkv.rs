use bytes::Bytes;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
/// BoxKV gRPC 服务实现
///
/// 说明：完成参数校验、错误映射、追踪埋点，将请求转发到核心引擎；
/// CPU 密集型操作用 spawn_blocking，避免阻塞异步运行时。
use std::sync::Arc;
use tonic::{Request, Response, Status, transport::Server};
use tracing::{error, info, instrument};

use crate::error::{Result, ServerError};
use crate::generated::boxkv::box_kv_server::BoxKv;
use crate::generated::boxkv::*;
use boxkv_core::db::async_adapter::BoxKVAsync;
use boxkv_core::{BoxKV, WasmCallPlan, hooks::HookType};
use boxkv_executor::{ExecutorError, SizeHint, TaskSpec, WorkClass};

/// BoxKV 的 gRPC 实现。负责把 protobuf 请求转换为核心引擎可用的参数，
/// 并在必要时执行计划转换与并发调度。
pub struct BoxKvService {
    db: Arc<BoxKV>,
    db_async: BoxKVAsync,
}

impl BoxKvService {
    pub fn new(db: Arc<BoxKV>) -> Self {
        let db_async = BoxKVAsync::new(Arc::clone(&db));
        Self { db, db_async }
    }

    fn build_wasm_plan(proto_plan: Option<WasmPlan>) -> Result<WasmCallPlan> {
        let mut plan = WasmCallPlan::new();

        if let Some(p) = proto_plan {
            for uuid_str in p.pre_write {
                let uuid = uuid::Uuid::parse_str(&uuid_str).map_err(|e| {
                    ServerError::InvalidArgument(format!("Invalid PreWrite UUID: {}", e))
                })?;
                plan.add(HookType::PreWrite, boxkv_core::hooks::PluginSpec::new(uuid));
            }

            for uuid_str in p.post_write {
                let uuid = uuid::Uuid::parse_str(&uuid_str).map_err(|e| {
                    ServerError::InvalidArgument(format!("Invalid PostWrite UUID: {}", e))
                })?;
                plan.add(
                    HookType::PostWrite,
                    boxkv_core::hooks::PluginSpec::new(uuid),
                );
            }

            for uuid_str in p.on_read {
                let uuid = uuid::Uuid::parse_str(&uuid_str).map_err(|e| {
                    ServerError::InvalidArgument(format!("Invalid OnRead UUID: {}", e))
                })?;
                plan.add(HookType::OnRead, boxkv_core::hooks::PluginSpec::new(uuid));
            }

            for uuid_str in p.scan_filter {
                let uuid = uuid::Uuid::parse_str(&uuid_str).map_err(|e| {
                    ServerError::InvalidArgument(format!("Invalid ScanFilter UUID: {}", e))
                })?;
                plan.add(
                    HookType::ScanFilter,
                    boxkv_core::hooks::PluginSpec::new(uuid),
                );
            }
        }

        Ok(plan)
    }

    /// 计算 WasmPlan 的确定性哈希
    ///
    /// 作用：SessionManager 校验游标是否匹配同一计划，避免跨计划误用。
    fn compute_plan_hash(proto_plan: Option<&WasmPlan>) -> u64 {
        let mut hasher = DefaultHasher::new();

        if let Some(plan) = proto_plan {
            // 对所有 hook 名称做排序后再 hash，保证不同输入顺序得到相同结果
            let mut all_hooks = Vec::new();

            for hook in &plan.pre_write {
                all_hooks.push(("pre_write", hook.as_str()));
            }
            for hook in &plan.post_write {
                all_hooks.push(("post_write", hook.as_str()));
            }
            for hook in &plan.on_read {
                all_hooks.push(("on_read", hook.as_str()));
            }
            for hook in &plan.scan_filter {
                all_hooks.push(("scan_filter", hook.as_str()));
            }

            // 排序保证确定性
            all_hooks.sort();

            // Hash
            for (hook_type, uuid) in all_hooks {
                hook_type.hash(&mut hasher);
                uuid.hash(&mut hasher);
            }
        }

        hasher.finish()
    }
}

#[tonic::async_trait]
impl BoxKv for BoxKvService {
    #[instrument(skip(self, request), fields(key_len = request.get_ref().key.len()))]
    async fn put(
        &self,
        request: Request<PutRequest>,
    ) -> std::result::Result<Response<PutResponse>, Status> {
        let req = request.into_inner();

        if req.key.is_empty() {
            return Err(Status::invalid_argument("Key cannot be empty"));
        }

        let key = Bytes::from(req.key);
        let value = Bytes::from(req.value);
        let plan = Self::build_wasm_plan(req.plan).map_err(Status::from)?;

        match req.expiry {
            Some(put_request::Expiry::TtlSecs(ttl)) => {
                let db = Arc::clone(&self.db);
                let plan_c = plan.clone();
                let key_c = key.clone();
                let value_c = value.clone();
                tokio::task::spawn_blocking(move || db.put_with_ttl(key_c, value_c, ttl, &plan_c))
                    .await
                    .map_err(|e| Status::internal(format!("Join error: {}", e)))?
                    .map_err(|e| Status::internal(e.to_string()))?;
            }
            Some(put_request::Expiry::ExpiresAt(expire_at)) => {
                let now = boxkv_common::time::current_timestamp_secs();
                if expire_at <= now {
                    return Err(Status::invalid_argument("Expiration time is in the past"));
                }
                let ttl = expire_at - now;
                let db = Arc::clone(&self.db);
                let plan_c = plan.clone();
                let key_c = key.clone();
                let value_c = value.clone();
                tokio::task::spawn_blocking(move || db.put_with_ttl(key_c, value_c, ttl, &plan_c))
                    .await
                    .map_err(|e| Status::internal(format!("Join error: {}", e)))?
                    .map_err(|e| Status::internal(e.to_string()))?;
            }
            None => {
                self.db_async
                    .put_async(key, value, &plan)
                    .await
                    .map_err(|e| Status::internal(e.to_string()))?;
            }
        }

        info!("PUT success");
        Ok(Response::new(PutResponse {}))
    }

    #[instrument(skip(self, request), fields(key_len = request.get_ref().key.len()))]
    async fn get(
        &self,
        request: Request<GetRequest>,
    ) -> std::result::Result<Response<GetResponse>, Status> {
        let req = request.into_inner();

        if req.key.is_empty() {
            return Err(Status::invalid_argument("Key cannot be empty"));
        }

        let plan = Self::build_wasm_plan(req.plan).map_err(Status::from)?;

        let result = self
            .db_async
            .get_async(Bytes::from(req.key), &plan)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let response = match result {
            Some(value) => {
                info!(value_len = value.len(), "GET success");
                GetResponse {
                    found: true,
                    value: value.to_vec(),
                }
            }
            None => {
                info!("GET not found");
                GetResponse {
                    found: false,
                    value: Vec::new(),
                }
            }
        };

        Ok(Response::new(response))
    }

    #[instrument(skip(self, request), fields(key_len = request.get_ref().key.len()))]
    async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> std::result::Result<Response<DeleteResponse>, Status> {
        let req = request.into_inner();

        if req.key.is_empty() {
            return Err(Status::invalid_argument("Key cannot be empty"));
        }

        let plan = Self::build_wasm_plan(req.plan).map_err(Status::from)?;

        self.db_async
            .delete_async(Bytes::from(req.key), &plan)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        info!("DELETE success");
        Ok(Response::new(DeleteResponse {}))
    }

    #[instrument(skip(self, request), fields(ops_count = request.get_ref().ops.len()))]
    async fn write_batch(
        &self,
        request: Request<WriteBatchRequest>,
    ) -> std::result::Result<Response<WriteBatchResponse>, Status> {
        let req = request.into_inner();

        if req.ops.is_empty() {
            return Err(Status::invalid_argument("Operations cannot be empty"));
        }

        let plan = Self::build_wasm_plan(req.plan).map_err(Status::from)?;
        let mut batch = boxkv_core::db::batch::WriteBatch::new();

        for op in req.ops {
            if op.key.is_empty() {
                return Err(Status::invalid_argument("Key in batch cannot be empty"));
            }

            match OpType::try_from(op.op) {
                Ok(OpType::OpPut) => {
                    let key = Bytes::from(op.key);
                    let value = Bytes::from(op.value);

                    match op.expiry {
                        Some(write_batch_op::Expiry::TtlSecs(ttl)) => {
                            batch.put_with_ttl(key, value, ttl);
                        }
                        Some(write_batch_op::Expiry::ExpiresAt(expire_at)) => {
                            let now = boxkv_common::time::current_timestamp_secs();
                            if expire_at > now {
                                batch.put_with_ttl(key, value, expire_at - now);
                            } else {
                                batch.delete(key);
                            }
                        }
                        None => {
                            batch.put(key, value);
                        }
                    }
                }
                Ok(OpType::OpDelete) => {
                    batch.delete(Bytes::from(op.key));
                }
                Err(_) => {
                    return Err(Status::invalid_argument(format!(
                        "Invalid operation type: {}",
                        op.op
                    )));
                }
            }
        }

        let db = Arc::clone(&self.db);
        let plan_c = plan.clone();
        tokio::task::spawn_blocking(move || db.write(batch, &plan_c))
            .await
            .map_err(|e| Status::internal(format!("Join error: {}", e)))?
            .map_err(|e| Status::internal(e.to_string()))?;

        info!("WRITE_BATCH success");
        Ok(Response::new(WriteBatchResponse {}))
    }

    #[instrument(skip(self, request), fields(keys_count = request.get_ref().keys.len()))]
    async fn multi_get(
        &self,
        request: Request<MultiGetRequest>,
    ) -> std::result::Result<Response<MultiGetResponse>, Status> {
        let req = request.into_inner();

        if req.keys.is_empty() {
            return Err(Status::invalid_argument("Keys cannot be empty"));
        }

        let keys: Vec<Bytes> = req.keys.into_iter().map(Bytes::from).collect();
        let plan = Self::build_wasm_plan(req.plan).map_err(Status::from)?;

        let db = Arc::clone(&self.db);
        let results = tokio::task::spawn_blocking(move || {
            // 内部使用同一 snapshot 保证一致性
            db.multi_get(keys, &plan)
        })
        .await
        .map_err(|e| Status::internal(format!("Join error: {}", e)))?
        .map_err(|e| Status::internal(e.to_string()))?;

        let proto_results = results
            .into_iter()
            .map(|opt| GetResult {
                found: opt.is_some(),
                value: opt.map(|v| v.to_vec()).unwrap_or_default(),
            })
            .collect();

        info!("MULTI_GET success");
        Ok(Response::new(MultiGetResponse {
            results: proto_results,
        }))
    }

    #[instrument(skip(self, request), fields(key_len = request.get_ref().key.len()))]
    async fn compare_and_set(
        &self,
        request: Request<CompareAndSetRequest>,
    ) -> std::result::Result<Response<CompareAndSetResponse>, Status> {
        let req = request.into_inner();

        if req.key.is_empty() {
            return Err(Status::invalid_argument("Key cannot be empty"));
        }

        let key = Bytes::from(req.key);
        let expected = req.expected_value.map(Bytes::from);
        let new_value = req.new_value.map(Bytes::from);
        let plan = Self::build_wasm_plan(req.plan).map_err(Status::from)?;

        let db = Arc::clone(&self.db);
        let success = tokio::task::spawn_blocking(move || {
            db.compare_and_set(key, expected, new_value, &plan)
        })
        .await
        .map_err(|e| Status::internal(format!("Join error: {}", e)))?
        .map_err(|e| Status::internal(e.to_string()))?;

        info!(success = success, "CAS completed");
        Ok(Response::new(CompareAndSetResponse { success }))
    }

    #[instrument(skip(self, request), fields(key_len = request.get_ref().key.len()))]
    async fn put_if_absent(
        &self,
        request: Request<PutIfAbsentRequest>,
    ) -> std::result::Result<Response<PutIfAbsentResponse>, Status> {
        let req = request.into_inner();

        if req.key.is_empty() {
            return Err(Status::invalid_argument("Key cannot be empty"));
        }

        let key = Bytes::from(req.key);
        let value = Bytes::from(req.value);
        let plan = Self::build_wasm_plan(req.plan).map_err(Status::from)?;

        let db = Arc::clone(&self.db);
        let success = tokio::task::spawn_blocking(move || db.put_if_absent(key, value, &plan))
            .await
            .map_err(|e| Status::internal(format!("Join error: {}", e)))?
            .map_err(|e| Status::internal(e.to_string()))?;

        info!(success = success, "PUT_IF_ABSENT completed");
        Ok(Response::new(PutIfAbsentResponse { success }))
    }

    #[instrument(skip(self, request), fields(key_len = request.get_ref().key.len()))]
    async fn expire_at(
        &self,
        request: Request<ExpireAtRequest>,
    ) -> std::result::Result<Response<ExpireAtResponse>, Status> {
        let req = request.into_inner();

        if req.key.is_empty() {
            return Err(Status::invalid_argument("Key cannot be empty"));
        }

        let key = Bytes::from(req.key);
        let expire_at = req.expire_at;
        let plan = Self::build_wasm_plan(req.plan).map_err(Status::from)?;

        let db = Arc::clone(&self.db);
        let success = tokio::task::spawn_blocking(move || db.expire_at(key, expire_at, &plan))
            .await
            .map_err(|e| Status::internal(format!("Join error: {}", e)))?
            .map_err(|e| Status::internal(e.to_string()))?;

        info!(success = success, "EXPIRE_AT completed");
        Ok(Response::new(ExpireAtResponse { success }))
    }

    #[instrument(skip(self, request))]
    async fn scan(
        &self,
        request: Request<ScanRequest>,
    ) -> std::result::Result<Response<ScanResponse>, Status> {
        let req = request.into_inner();

        let start = Bytes::from(req.start_key);
        let end = Bytes::from(req.end_key);
        let limit = req.limit as usize;
        let cursor = req.cursor;

        // 计算 plan_hash
        let plan_hash = Self::compute_plan_hash(req.plan.as_ref());
        let plan = Self::build_wasm_plan(req.plan).map_err(Status::from)?;

        if limit == 0 || limit > 10000 {
            return Err(Status::invalid_argument(
                "Limit must be between 1 and 10000",
            ));
        }

        let db = Arc::clone(&self.db);

        let result = tokio::task::spawn_blocking(move || {
            if let Some(cursor_token) = cursor {
                // 恢复扫描（使用 SessionManager）
                let session_mgr = db.session_manager();
                let (pairs, next_cursor) = session_mgr
                    .scan_next(&cursor_token)
                    .map_err(|e| boxkv_core::error::BoxKVError::Internal(e.to_string()))?;

                let kv_pairs: Vec<KeyValue> = pairs
                    .into_iter()
                    .map(|(k, v)| KeyValue {
                        key: k.to_vec(),
                        value: v.to_vec(),
                    })
                    .collect();

                let has_more = next_cursor.is_some();
                Ok::<_, boxkv_core::error::BoxKVError>((kv_pairs, next_cursor, has_more))
            } else {
                // 首次扫描（创建会话）
                let snapshot = db.snapshot()?;
                let read_sequence = snapshot.sequence();

                // 创建 OwnedDBIterator
                let iter = db.create_owned_iterator(&start, &end, read_sequence, &plan)?;

                // 创建会话
                let session_mgr = db.session_manager();
                let (_session_id, cursor_token) = session_mgr.create_scan_session(
                    iter,
                    end.clone(),
                    limit,
                    read_sequence,
                    plan_hash,
                )?;

                // 立即读取第一批数据
                let (pairs, next_cursor) = session_mgr
                    .scan_next(&cursor_token)
                    .map_err(|e| boxkv_core::error::BoxKVError::Internal(e.to_string()))?;

                let kv_pairs: Vec<KeyValue> = pairs
                    .into_iter()
                    .map(|(k, v)| KeyValue {
                        key: k.to_vec(),
                        value: v.to_vec(),
                    })
                    .collect();

                let has_more = next_cursor.is_some();
                Ok::<_, boxkv_core::error::BoxKVError>((kv_pairs, next_cursor, has_more))
            }
        })
        .await
        .map_err(|e| Status::internal(format!("Join error: {}", e)))?
        .map_err(|e| Status::internal(e.to_string()))?;

        info!(
            pairs_count = result.0.len(),
            has_more = result.2,
            "SCAN completed"
        );
        Ok(Response::new(ScanResponse {
            pairs: result.0,
            next_cursor: result.1,
            has_more: result.2,
        }))
    }

    type ScanStreamStream = std::pin::Pin<
        Box<
            dyn tokio_stream::Stream<Item = std::result::Result<ScanStreamResponse, Status>>
                + Send
                + 'static,
        >,
    >;

    #[instrument(skip(self, request))]
    async fn scan_stream(
        &self,
        request: Request<ScanRequest>,
    ) -> std::result::Result<Response<Self::ScanStreamStream>, Status> {
        let req = request.into_inner();

        let start = Bytes::from(req.start_key);
        let end = Bytes::from(req.end_key);

        // 计算 plan_hash
        let plan_hash = Self::compute_plan_hash(req.plan.as_ref());
        let plan = Self::build_wasm_plan(req.plan).map_err(Status::from)?;

        let db = Arc::clone(&self.db);

        // 使用 SessionManager 管理迭代器
        let stream = async_stream::try_stream! {
            // 创建快照和迭代器
            let snapshot = db.snapshot().map_err(|e| Status::internal(e.to_string()))?;
            let read_sequence = snapshot.sequence();

            let iter = db.create_owned_iterator(&start, &end, read_sequence, &plan)
                .map_err(|e| Status::internal(e.to_string()))?;

            let session_mgr = db.session_manager();

            // 创建会话
            let (_session_id, mut cursor) = session_mgr.create_scan_session(
                iter,
                end.clone(),
                100,  // 每批 100 个
                read_sequence,
                plan_hash,
            ).map_err(|e| Status::internal(e.to_string()))?;

            // 持续读取直到没有更多数据
            loop {
                let (pairs, next_cursor) = session_mgr.scan_next(&cursor)
                    .map_err(|e| Status::internal(e.to_string()))?;

                if pairs.is_empty() {
                    break;
                }

                let kv_pairs: Vec<KeyValue> = pairs.into_iter()
                    .map(|(k, v)| KeyValue { key: k.to_vec(), value: v.to_vec() })
                    .collect();

                yield ScanStreamResponse { pairs: kv_pairs };

                if let Some(next) = next_cursor {
                    cursor = next;
                } else {
                    break;
                }
            }
        };

        Ok(Response::new(Box::pin(stream) as Self::ScanStreamStream))
    }
}
