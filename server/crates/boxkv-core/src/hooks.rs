//! Wasm Hook 系统核心抽象

use crate::db::error::Result;
use boxkv_common::types::ValueType;
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::Arc;

/// 值访问器
pub trait ValueAccessor: Send + Sync {
    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// 读取指定范围字节
    fn read_at(&self, offset: usize, len: usize) -> Bytes;

    fn read_all(&self) -> Bytes {
        self.read_at(0, self.len())
    }

    fn starts_with(&self, prefix: &[u8]) -> bool {
        if prefix.len() > self.len() {
            return false;
        }
        self.read_at(0, prefix.len()) == prefix
    }
}

/// 写入上下文
pub struct WriteContext {
    key: Bytes,
    value: ValueType,
}

impl WriteContext {
    pub fn new(key: Bytes, value: ValueType) -> Self {
        Self { key, value }
    }

    pub fn key(&self) -> &dyn ValueAccessor {
        &self.key as &dyn ValueAccessor
    }

    pub fn value(&self) -> &dyn ValueAccessor {
        &self.value as &dyn ValueAccessor
    }

    pub(crate) fn key_bytes(&self) -> &Bytes {
        &self.key
    }

    pub(crate) fn value_type(&self) -> &ValueType {
        &self.value
    }

    pub fn is_normal(&self) -> bool {
        matches!(self.value, ValueType::Normal(_))
    }

    pub fn is_tombstone(&self) -> bool {
        self.value.is_tombstone()
    }

    pub fn is_expiring(&self) -> bool {
        matches!(self.value, ValueType::Expiring { .. })
    }

    pub fn expires_at(&self) -> Option<u64> {
        self.value.expire_at()
    }

    pub fn ttl_remaining(&self, now_secs: u64) -> Option<u64> {
        self.value.expire_at().and_then(|exp| {
            if exp > now_secs {
                Some(exp - now_secs)
            } else {
                Some(0)
            }
        })
    }
}

impl ValueAccessor for Bytes {
    fn len(&self) -> usize {
        Bytes::len(self)
    }

    fn read_at(&self, offset: usize, len: usize) -> Bytes {
        let end = std::cmp::min(offset.saturating_add(len), self.len());
        if offset >= self.len() {
            return Bytes::new();
        }
        self.slice(offset..end)
    }
}

impl ValueAccessor for ValueType {
    fn len(&self) -> usize {
        match self {
            ValueType::Normal(data) => data.len(),
            ValueType::Expiring { data, .. } => data.len(),
            ValueType::Tombstone => 0,
        }
    }

    fn read_at(&self, offset: usize, len: usize) -> Bytes {
        match self {
            ValueType::Normal(data) | ValueType::Expiring { data, .. } => {
                let end = std::cmp::min(offset.saturating_add(len), data.len());
                if offset >= data.len() {
                    return Bytes::new();
                }
                data.slice(offset..end)
            }
            ValueType::Tombstone => Bytes::new(),
        }
    }
}

/// 写入变更指令
#[derive(Debug, Clone)]
pub enum WriteCommand {
    SetKey(Bytes),
    SetValue(Bytes),
    SetTTL(u64),
    SetExpiresAt(u64),
    ClearTTL,
}

/// 插件标识
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PluginSpec {
    pub id: uuid::Uuid,
}

impl PluginSpec {
    pub fn new(id: uuid::Uuid) -> Self {
        Self { id }
    }
}

/// Hook 类型
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HookType {
    PreWrite,
    PostWrite,
    OnRead,
    ScanFilter,
}

/// Wasm 插件执行计划
#[derive(Debug, Clone)]
pub struct WasmCallPlan {
    hooks: HashMap<HookType, Vec<PluginSpec>>,
}

impl WasmCallPlan {
    pub fn new() -> Self {
        Self {
            hooks: HashMap::new(),
        }
    }

    pub fn add(&mut self, hook_type: HookType, plugin: PluginSpec) {
        self.hooks.entry(hook_type).or_default().push(plugin);
    }

    pub fn get(&self, hook_type: HookType) -> Option<&[PluginSpec]> {
        self.hooks
            .get(&hook_type)
            .map(|v: &Vec<PluginSpec>| v.as_slice())
    }

    pub fn is_empty(&self) -> bool {
        self.hooks.is_empty()
    }
}

/// PreWrite Hook 返回动作
#[derive(Debug, Clone)]
pub enum PreWriteAction {
    Accept,
    Reject(String),
    Transform(Vec<WriteCommand>),
}

/// 写入属性
#[derive(Debug, Clone, Default)]
pub struct WriteAttrs {
    /// 相对 TTL（秒）
    pub ttl_secs: Option<u64>,
    /// 绝对过期时间（Unix 秒）
    pub expires_at: Option<u64>,
}

impl WriteAttrs {
    pub fn to_commands(&self) -> Vec<WriteCommand> {
        let mut cmds = Vec::new();
        if let Some(ttl) = self.ttl_secs {
            cmds.push(WriteCommand::SetTTL(ttl));
        } else if let Some(expires_at) = self.expires_at {
            cmds.push(WriteCommand::SetExpiresAt(expires_at));
        }
        cmds
    }
}

pub type PostWriteAction = ();

/// OnRead Hook 返回值
#[derive(Debug, Clone)]
pub enum OnReadAction {
    Accept(ValueType),
    Transform(ValueType),
    Reject(String),
}

/// ScanFilter Hook 返回值
#[derive(Debug, Clone)]
pub enum ScanFilterAction {
    Keep,
    Drop,
}

/// 只读数据库视图
pub trait DbView: Send + Sync {
    fn kv_get(&self, key: &[u8]) -> Result<Option<Bytes>>;

    /// 范围扫描（迭代器）
    fn scan_range_iter(
        &self,
        start: &[u8],
        end: &[u8],
        plan: &WasmCallPlan,
    ) -> Result<Box<dyn Iterator<Item = Result<(Bytes, Bytes)>> + Send>>;

    /// 范围扫描
    fn scan_range(
        &self,
        start: &[u8],
        end: &[u8],
        limit: usize,
        plan: &WasmCallPlan,
    ) -> Result<Vec<(Bytes, Bytes)>> {
        self.scan_range_iter(start, end, plan)?
            .take(limit)
            .collect()
    }
}

/// Hook 执行上下文
pub struct HookContext {
    /// 读取序列号（MVCC 隔离，PreWrite 时为 None）
    pub read_sequence: Option<u64>,

    /// 只读数据库视图
    pub db_view: Arc<dyn DbView>,
}

impl HookContext {
    pub fn new(db_view: Arc<dyn DbView>) -> Self {
        Self {
            read_sequence: None,
            db_view,
        }
    }

    pub fn with_read_sequence(mut self, seq: u64) -> Self {
        self.read_sequence = Some(seq);
        self
    }
}

/// Hook 执行器抽象
pub trait HookProvider: Send + Sync {
    /// PreWrite Hook
    fn pre_write(
        &self,
        ctx: &HookContext,
        plan: &WasmCallPlan,
        write_ctx: &WriteContext,
    ) -> Result<PreWriteAction>;

    /// PostWrite Hook
    fn post_write(
        &self,
        ctx: &HookContext,
        plan: &WasmCallPlan,
        write_ctx: &WriteContext,
        sequence: u64,
    );

    /// OnRead Hook
    fn on_read(
        &self,
        ctx: &HookContext,
        plan: &WasmCallPlan,
        key: Bytes,
        value: ValueType,
    ) -> Result<OnReadAction>;

    /// ScanFilter Hook
    fn scan_filter(
        &self,
        ctx: &HookContext,
        plan: &WasmCallPlan,
        key: Bytes,
        value: ValueType,
    ) -> Result<ScanFilterAction>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wasm_call_plan() {
        use uuid::Uuid;

        let mut plan = WasmCallPlan::new();
        assert!(plan.is_empty());

        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();
        let id3 = Uuid::new_v4();

        plan.add(HookType::PreWrite, PluginSpec::new(id1));
        plan.add(HookType::PreWrite, PluginSpec::new(id2));
        plan.add(HookType::OnRead, PluginSpec::new(id3));

        let pre_write = plan.get(HookType::PreWrite).unwrap();
        assert_eq!(pre_write.len(), 2);
        assert_eq!(pre_write[0], PluginSpec::new(id1));

        let on_read = plan.get(HookType::OnRead).unwrap();
        assert_eq!(on_read.len(), 1);

        assert!(plan.get(HookType::PostWrite).is_none());
    }
}
