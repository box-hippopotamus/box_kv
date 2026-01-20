use crate::error::BoxKVResult;
use crate::iterator::OwnedDBIterator;
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use bytes::Bytes;
use hmac::{Hmac, Mac};
use sha2::Sha256;
/// Session 和 Cursor 管理模块
///
/// - 轻状态会话管理：内存中管理活跃的扫描会话
/// - TTL 自动清理：避免会话泄漏
/// - Cursor 防篡改：HMAC 签名防止客户端伪造
/// - 零额外存储：Session 纯内存，重启后失效（符合分页扫描场景）
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

type HmacSha256 = Hmac<Sha256>;

/// Session 唯一标识符
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SessionId(u64);

impl SessionId {
    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

/// Cursor 编码数据
#[derive(Debug, Clone)]
pub struct CursorData {
    pub session_id: SessionId,
    pub last_key: Option<Bytes>,
    pub end_key: Bytes,
    pub limit: usize,
    pub read_sequence: u64,
    pub plan_hash: u64,
    pub expires_at: Instant,
}

/// 扫描会话
struct ScanSession {
    id: SessionId,
    iterator: OwnedDBIterator,
    end_key: Bytes,
    limit: usize,
    read_sequence: u64,
    plan_hash: u64,
    created_at: Instant,
    expires_at: Instant,
    last_access: Instant,
    /// 缓存的预读元素（用于检查 has_more 而不丢失元素）
    buffered_item: Option<BoxKVResult<(Bytes, Bytes)>>,
}

/// SessionManager - 扫描会话管理器
///
/// **功能**：
/// - 创建/关闭扫描会话
/// - 生成/验证 Cursor（HMAC 签名）
/// - TTL 自动清理
/// - LRU 驱逐（可选）
pub struct SessionManager {
    /// 活跃会话
    sessions: Arc<RwLock<HashMap<SessionId, ScanSession>>>,

    /// Session ID 生成器
    next_id: Arc<std::sync::atomic::AtomicU64>,

    /// HMAC 密钥（启动时随机生成，重启后失效所有 cursor）
    hmac_key: [u8; 32],

    /// 默认会话 TTL
    default_ttl: Duration,

    /// 最大活跃会话数
    max_sessions: usize,
}

impl SessionManager {
    /// 创建 SessionManager
    pub fn new(default_ttl: Duration, max_sessions: usize) -> Self {
        // 生成随机 HMAC 密钥
        let mut hmac_key = [0u8; 32];
        getrandom::getrandom(&mut hmac_key).expect("Failed to generate HMAC key");

        Self {
            sessions: Arc::new(RwLock::new(HashMap::new())),
            next_id: Arc::new(std::sync::atomic::AtomicU64::new(1)),
            hmac_key,
            default_ttl,
            max_sessions,
        }
    }

    /// 创建新的扫描会话
    ///
    /// # 返回
    /// - (session_id, cursor)
    pub fn create_scan_session(
        &self,
        iterator: OwnedDBIterator,
        end_key: Bytes,
        limit: usize,
        read_sequence: u64,
        plan_hash: u64,
    ) -> BoxKVResult<(SessionId, String)> {
        // 清理过期会话
        self.cleanup_expired();

        // 检查会话数量限制
        {
            let sessions = self.sessions.read().unwrap();
            if sessions.len() >= self.max_sessions {
                // LRU 驱逐最早的会话
                self.evict_oldest();
            }
        }

        // 生成会话 ID
        let session_id = SessionId(
            self.next_id
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst),
        );

        let now = Instant::now();
        let expires_at = now + self.default_ttl;

        // 创建会话
        let session = ScanSession {
            id: session_id,
            iterator,
            end_key: end_key.clone(),
            limit,
            read_sequence,
            plan_hash,
            created_at: now,
            expires_at: now + self.default_ttl,
            last_access: now,
            buffered_item: None,
        };

        // 存储会话
        {
            let mut sessions = self.sessions.write().unwrap();
            sessions.insert(session_id, session);
        }

        // 生成初始 cursor
        let cursor_data = CursorData {
            session_id,
            last_key: None,
            end_key, // 这里使用原始的 end_key
            limit,
            read_sequence,
            plan_hash,
            expires_at,
        };

        let cursor = self.encode_cursor(&cursor_data)?;

        Ok((session_id, cursor))
    }

    /// 使用 cursor 继续扫描
    ///
    /// # 返回
    /// - (results, next_cursor?)
    pub fn scan_next(&self, cursor: &str) -> BoxKVResult<(Vec<(Bytes, Bytes)>, Option<String>)> {
        // 解码并验证 cursor
        let cursor_data = self.decode_cursor(cursor)?;

        // 检查过期
        if Instant::now() > cursor_data.expires_at {
            return Err(crate::error::BoxKVError::Internal(
                "Cursor expired".to_string(),
            ));
        }

        // 获取会话
        let mut sessions = self.sessions.write().unwrap();
        let session = sessions
            .get_mut(&cursor_data.session_id)
            .ok_or_else(|| crate::error::BoxKVError::Internal("Session not found".to_string()))?;

        // 验证会话参数
        if session.read_sequence != cursor_data.read_sequence
            || session.plan_hash != cursor_data.plan_hash
            || session.end_key != cursor_data.end_key
        {
            return Err(crate::error::BoxKVError::Internal(
                "Cursor validation failed".to_string(),
            ));
        }

        // 更新最后访问时间
        session.last_access = Instant::now();

        // 收集结果
        let mut results = Vec::new();

        // 1. 先检查是否有缓存的元素（上次预读的）
        if let Some(buffered) = session.buffered_item.take() {
            match buffered {
                Ok((k, v)) => results.push((k, v)),
                Err(e) => return Err(e),
            }
        }

        // 2. 读取剩余元素直到达到 limit
        while results.len() < cursor_data.limit {
            match session.iterator.next() {
                Some(Ok((key, value))) => {
                    results.push((key, value));
                }
                Some(Err(e)) => {
                    return Err(e);
                }
                None => {
                    // 迭代器耗尽
                    break;
                }
            }
        }

        // 3. 预读一个元素来判断是否还有更多数据
        let has_more = if results.len() == cursor_data.limit {
            match session.iterator.next() {
                Some(item) => {
                    // 有下一个元素，缓存起来
                    session.buffered_item = Some(item);
                    true
                }
                None => {
                    // 迭代器耗尽
                    false
                }
            }
        } else {
            // 没读满 limit，说明已经没有更多数据了
            false
        };

        // 如果还有数据，生成新 cursor
        let next_cursor = if has_more && !results.is_empty() {
            let last_key = results.last().map(|(k, _)| k.clone());
            let new_cursor_data = CursorData {
                session_id: cursor_data.session_id,
                last_key,
                end_key: cursor_data.end_key.clone(),
                limit: cursor_data.limit,
                read_sequence: cursor_data.read_sequence,
                plan_hash: cursor_data.plan_hash,
                expires_at: cursor_data.expires_at,
            };
            Some(self.encode_cursor(&new_cursor_data)?)
        } else {
            // 没有更多数据，关闭会话
            sessions.remove(&cursor_data.session_id);
            None
        };

        Ok((results, next_cursor))
    }

    /// 关闭会话
    pub fn close_session(&self, session_id: SessionId) -> BoxKVResult<()> {
        let mut sessions = self.sessions.write().unwrap();
        sessions.remove(&session_id);
        Ok(())
    }

    /// 编码 cursor（带 HMAC 签名）
    fn encode_cursor(&self, data: &CursorData) -> BoxKVResult<String> {
        // 序列化数据
        let mut payload = Vec::new();

        // session_id (8 bytes)
        payload.extend_from_slice(&data.session_id.as_u64().to_le_bytes());

        // last_key (length + data)
        if let Some(ref key) = data.last_key {
            payload.extend_from_slice(&(key.len() as u32).to_le_bytes());
            payload.extend_from_slice(key);
        } else {
            payload.extend_from_slice(&0u32.to_le_bytes());
        }

        // end_key (length + data)
        payload.extend_from_slice(&(data.end_key.len() as u32).to_le_bytes());
        payload.extend_from_slice(&data.end_key);

        // limit (4 bytes)
        payload.extend_from_slice(&(data.limit as u32).to_le_bytes());

        // read_sequence (8 bytes)
        payload.extend_from_slice(&data.read_sequence.to_le_bytes());

        // plan_hash (8 bytes)
        payload.extend_from_slice(&data.plan_hash.to_le_bytes());

        // expires_at (12 bytes: secs + nanos)
        let duration = data.expires_at.duration_since(
            Instant::now()
                .checked_sub(Duration::from_secs(3600 * 24 * 365))
                .unwrap_or_else(|| Instant::now()),
        );
        payload.extend_from_slice(&duration.as_secs().to_le_bytes());
        payload.extend_from_slice(&duration.subsec_nanos().to_le_bytes());

        // 计算 HMAC
        let mut mac = HmacSha256::new_from_slice(&self.hmac_key)
            .map_err(|e| crate::error::BoxKVError::Internal(format!("HMAC init failed: {}", e)))?;
        mac.update(&payload);
        let signature = mac.finalize().into_bytes();

        // payload + signature
        payload.extend_from_slice(&signature);

        // Base64 编码
        Ok(URL_SAFE_NO_PAD.encode(&payload))
    }

    /// 解码 cursor（验证 HMAC）
    fn decode_cursor(&self, cursor: &str) -> BoxKVResult<CursorData> {
        // Base64 解码
        let data = URL_SAFE_NO_PAD
            .decode(cursor)
            .map_err(|e| crate::error::BoxKVError::Internal(format!("Invalid cursor: {}", e)))?;

        if data.len() < 32 {
            return Err(crate::error::BoxKVError::Internal(
                "Cursor too short".to_string(),
            ));
        }

        // 分离 payload 和 signature
        let (payload, signature) = data.split_at(data.len() - 32);

        // 验证 HMAC
        let mut mac = HmacSha256::new_from_slice(&self.hmac_key)
            .map_err(|e| crate::error::BoxKVError::Internal(format!("HMAC init failed: {}", e)))?;
        mac.update(payload);
        mac.verify_slice(signature).map_err(|_| {
            crate::error::BoxKVError::Internal("Cursor signature verification failed".to_string())
        })?;

        // 解析 payload
        let mut offset = 0;

        // session_id
        let session_id = u64::from_le_bytes(payload[offset..offset + 8].try_into().unwrap());
        offset += 8;

        // last_key
        let last_key_len =
            u32::from_le_bytes(payload[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;

        let last_key = if last_key_len > 0 {
            let key = Bytes::copy_from_slice(&payload[offset..offset + last_key_len]);
            offset += last_key_len;
            Some(key)
        } else {
            None
        };

        // end_key
        let end_key_len =
            u32::from_le_bytes(payload[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;

        let end_key = Bytes::copy_from_slice(&payload[offset..offset + end_key_len]);
        offset += end_key_len;

        // limit
        let limit = u32::from_le_bytes(payload[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;

        // read_sequence
        let read_sequence = u64::from_le_bytes(payload[offset..offset + 8].try_into().unwrap());
        offset += 8;

        // plan_hash
        let plan_hash = u64::from_le_bytes(payload[offset..offset + 8].try_into().unwrap());
        offset += 8;

        // expires_at
        let secs = u64::from_le_bytes(payload[offset..offset + 8].try_into().unwrap());
        offset += 8;

        let nanos = u32::from_le_bytes(payload[offset..offset + 4].try_into().unwrap());

        let base = Instant::now()
            .checked_sub(Duration::from_secs(3600 * 24 * 365))
            .unwrap_or_else(|| Instant::now());
        let expires_at = base + Duration::new(secs, nanos);

        Ok(CursorData {
            session_id: SessionId(session_id),
            last_key,
            end_key,
            limit,
            read_sequence,
            plan_hash,
            expires_at,
        })
    }

    /// 清理过期会话
    fn cleanup_expired(&self) {
        let mut sessions = self.sessions.write().unwrap();
        let now = Instant::now();
        sessions.retain(|_, session| session.expires_at > now);
    }

    /// LRU 驱逐最早的会话
    fn evict_oldest(&self) {
        let mut sessions = self.sessions.write().unwrap();
        if let Some((&id, _)) = sessions.iter().min_by_key(|(_, s)| s.created_at) {
            sessions.remove(&id);
        }
    }

    /// 获取活跃会话数量
    pub fn active_count(&self) -> usize {
        self.sessions.read().unwrap().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cursor_encode_decode() {
        let manager = SessionManager::new(Duration::from_secs(300), 1000);

        let cursor_data = CursorData {
            session_id: SessionId(123),
            last_key: Some(Bytes::from("test_key")),
            end_key: Bytes::from("end_key"),
            limit: 100,
            read_sequence: 456,
            plan_hash: 789,
            expires_at: Instant::now() + Duration::from_secs(300),
        };

        let encoded = manager.encode_cursor(&cursor_data).unwrap();
        let decoded = manager.decode_cursor(&encoded).unwrap();

        assert_eq!(decoded.session_id, cursor_data.session_id);
        assert_eq!(decoded.last_key, cursor_data.last_key);
        assert_eq!(decoded.end_key, cursor_data.end_key);
        assert_eq!(decoded.limit, cursor_data.limit);
        assert_eq!(decoded.read_sequence, cursor_data.read_sequence);
        assert_eq!(decoded.plan_hash, cursor_data.plan_hash);
    }

    #[test]
    fn test_cursor_tampering_detection() {
        let manager = SessionManager::new(Duration::from_secs(300), 1000);

        let cursor_data = CursorData {
            session_id: SessionId(123),
            last_key: None,
            end_key: Bytes::from("end"),
            limit: 100,
            read_sequence: 456,
            plan_hash: 789,
            expires_at: Instant::now() + Duration::from_secs(300),
        };

        let mut encoded = manager.encode_cursor(&cursor_data).unwrap();

        // 篡改 cursor
        encoded.push('X');

        // 应该验证失败
        assert!(manager.decode_cursor(&encoded).is_err());
    }
}
