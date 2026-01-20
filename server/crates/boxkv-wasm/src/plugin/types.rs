//! Plugin Management Types
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fmt;
use uuid::Uuid;

/// 注入点类型（与 boxkv-core::HookType 对齐）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HookType {
    PreWrite,
    PostWrite,
    OnRead,
    ScanFilter,
}

impl fmt::Display for HookType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HookType::PreWrite => write!(f, "pre_write"),
            HookType::PostWrite => write!(f, "post_write"),
            HookType::OnRead => write!(f, "on_read"),
            HookType::ScanFilter => write!(f, "scan_filter"),
        }
    }
}

impl std::str::FromStr for HookType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "pre_write" => Ok(HookType::PreWrite),
            "post_write" => Ok(HookType::PostWrite),
            "on_read" => Ok(HookType::OnRead),
            "scan_filter" => Ok(HookType::ScanFilter),
            _ => Err(format!("Invalid hook type: {}", s)),
        }
    }
}

/// 插件定位键 (name, version, hook)
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct PluginKey {
    pub name: String,
    pub version: String,
    pub hook: HookType,
}

impl PluginKey {
    pub fn new(name: String, version: String, hook: HookType) -> Self {
        Self {
            name,
            version,
            hook,
        }
    }
}

impl fmt::Display for PluginKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}:{}", self.name, self.version, self.hook)
    }
}

/// 指纹（sha256 of wasm bytes）
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Fingerprint(String);

impl Fingerprint {
    /// 从 wasm 字节计算指纹
    pub fn compute(wasm_bytes: &Bytes) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(wasm_bytes.as_ref());
        let result = hasher.finalize();
        Self(hex::encode(result))
    }

    pub fn from_hex(hex: String) -> Result<Self, String> {
        if hex.len() != 64 {
            return Err("Fingerprint must be 64 hex chars".to_string());
        }
        Ok(Self(hex))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Fingerprint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// 插件 ID（服务端生成的执行句柄）
/// id = UUIDv5(namespace, "{name}:{version}:{hook}:{fingerprint}")
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct PluginId(Uuid);

impl PluginId {
    /// BoxKV Plugins 专用命名空间（固定 UUID）
    const NAMESPACE: Uuid = Uuid::from_bytes([
        0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4, 0x30,
        0xc8,
    ]);

    /// 从四元组生成 ID（可计算，幂等）
    pub fn generate(key: &PluginKey, fingerprint: &Fingerprint) -> Self {
        let input = format!("{}:{}", key, fingerprint);
        let uuid = Uuid::new_v5(&Self::NAMESPACE, input.as_bytes());
        Self(uuid)
    }

    pub fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }

    pub fn as_uuid(&self) -> &Uuid {
        &self.0
    }
}

impl fmt::Display for PluginId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// 插件记录
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginRecord {
    pub id: PluginId,
    pub key: PluginKey,
    pub fingerprint: Fingerprint,
    pub size: u64,
    pub revision: u64,
}

impl PluginRecord {
    pub fn new(
        id: PluginId,
        key: PluginKey,
        fingerprint: Fingerprint,
        size: u64,
        revision: u64,
    ) -> Self {
        Self {
            id,
            key,
            fingerprint,
            size,
            revision,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn test_fingerprint_compute() {
        let bytes = Bytes::from_static(b"test wasm binary");
        let fp = Fingerprint::compute(&bytes);
        assert_eq!(fp.as_str().len(), 64);

        // 同一输入产生同一指纹
        let fp2 = Fingerprint::compute(&bytes);
        assert_eq!(fp, fp2);

        // 不同输入产生不同指纹
        let fp3 = Fingerprint::compute(&Bytes::from_static(b"different"));
        assert_ne!(fp, fp3);
    }

    #[test]
    fn test_plugin_id_generate() {
        let key = PluginKey::new("test".to_string(), "1".to_string(), HookType::PreWrite);
        let fp = Fingerprint::compute(&Bytes::from_static(b"test"));

        let id1 = PluginId::generate(&key, &fp);
        let id2 = PluginId::generate(&key, &fp);

        // 同一四元组产生同一 ID（幂等）
        assert_eq!(id1, id2);

        // 不同指纹产生不同 ID
        let fp_diff = Fingerprint::compute(&Bytes::from_static(b"different"));
        let id3 = PluginId::generate(&key, &fp_diff);
        assert_ne!(id1, id3);
    }

    #[test]
    fn test_hook_type_serde() {
        let hook = HookType::PreWrite;
        let json = serde_json::to_string(&hook).expect("Failed to serialize HookType");
        assert_eq!(json, r#""pre_write""#);

        let hook2: HookType = serde_json::from_str(&json).expect("Failed to deserialize HookType");
        assert_eq!(hook, hook2);
    }
}
