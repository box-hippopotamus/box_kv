//! 调用期上下文
mod command;
mod handle;

pub use command::CommandBuffer;
pub use handle::HandleTable;

use crate::BudgetConfig;
use crate::budget::MemoryLimiter;
use boxkv_core::{DbView, hooks::WriteCommand};
use bytes::Bytes;
use std::sync::Arc;

/// 调用期上下文（PreWrite/PostWrite/OnRead/ScanFilter）
pub struct CallContext {
    /// 句柄表
    pub(crate) handle_table: HandleTable,

    /// 命令缓冲
    command_buffer: CommandBuffer,

    /// 读路径结果缓冲
    read_result: ReadResultBuffer,

    /// 只读 DB 视图
    pub(crate) db_view: Arc<dyn DbView>,

    /// 预算配置
    budget: BudgetConfig,

    /// 统计计数器
    stats: Stats,

    /// 是否只读模式
    readonly: bool,

    /// Sequence（PostWrite 时有效）
    sequence: Option<u64>,

    /// 内存限制器（由 BudgetManager 设置）
    pub(crate) memory_limiter: Option<MemoryLimiter>,
}

/// 读路径结果缓冲
#[derive(Debug, Default)]
struct ReadResultBuffer {
    /// 变换后的值
    transformed_value: Option<Bytes>,

    /// 拒绝原因
    reject_reason: Option<String>,

    /// ScanFilter 结果
    drop_entry: bool,
}

/// 统计信息
#[derive(Debug, Default)]
struct Stats {
    kv_get_count: u32,
    bytes_read_total: usize,
}

impl CallContext {
    /// 创建可写上下文（PreWrite）
    pub fn new_writable(
        key: Bytes,
        value: Bytes,
        expires_at: Option<u64>,
        db_view: Arc<dyn DbView>,
        budget: BudgetConfig,
    ) -> Self {
        let mut handle_table = HandleTable::new();
        handle_table.register_key(key);
        handle_table.register_value(value);
        if let Some(ts) = expires_at {
            handle_table.set_expires_at(ts);
        }

        Self {
            handle_table,
            command_buffer: CommandBuffer::new(),
            read_result: ReadResultBuffer::default(),
            db_view,
            budget,
            stats: Stats::default(),
            readonly: false,
            sequence: None,
            memory_limiter: None,
        }
    }

    /// 创建只读上下文（PostWrite）
    pub fn new_readonly(
        key: Bytes,
        value: Bytes,
        expires_at: Option<u64>,
        db_view: Arc<dyn DbView>,
        budget: BudgetConfig,
        sequence: u64,
    ) -> Self {
        let mut handle_table = HandleTable::new();
        handle_table.register_key(key);
        handle_table.register_value(value);
        if let Some(ts) = expires_at {
            handle_table.set_expires_at(ts);
        }

        Self {
            handle_table,
            command_buffer: CommandBuffer::new(),
            read_result: ReadResultBuffer::default(),
            db_view,
            budget,
            stats: Stats::default(),
            readonly: true,
            sequence: Some(sequence),
            memory_limiter: None,
        }
    }

    // ========== 上下文查询 ==========

    pub fn ctx_key_handle(&self) -> u32 {
        self.handle_table.key_handle()
    }

    pub fn ctx_value_handle(&self) -> u32 {
        self.handle_table.value_handle()
    }

    pub fn ctx_value_kind(&self) -> u32 {
        self.handle_table.value_kind()
    }

    pub fn ctx_expires_at(&self) -> u64 {
        self.handle_table.expires_at()
    }

    pub fn ctx_sequence(&self) -> u64 {
        self.sequence.unwrap_or(0)
    }

    // ========== 句柄操作 ==========

    pub fn bytes_len(&self, handle: u32) -> Result<u32, i32> {
        self.handle_table.bytes_len(handle)
    }

    pub fn bytes_read(
        &mut self,
        handle: u32,
        offset: u32,
        len: u32,
        dst: &mut [u8],
    ) -> Result<u32, i32> {
        let actual = self.handle_table.bytes_read(handle, offset, len, dst)?;

        // 更新统计并检查限额
        self.stats.bytes_read_total += actual as usize;
        if self.stats.bytes_read_total > self.budget.max_bytes_read_total {
            return Err(-7); // 超出读取限额
        }

        Ok(actual)
    }

    pub fn bytes_starts_with(&self, handle: u32, prefix: &[u8]) -> Result<i32, i32> {
        self.handle_table.bytes_starts_with(handle, prefix)
    }

    pub fn bytes_equals(&self, handle: u32, needle: &[u8]) -> Result<i32, i32> {
        self.handle_table.bytes_equals(handle, needle)
    }

    pub fn bytes_find(&self, handle: u32, needle: &[u8], start_off: u32) -> Result<i32, i32> {
        self.handle_table.bytes_find(handle, needle, start_off)
    }

    pub fn bytes_close(&mut self, handle: u32) -> Result<(), i32> {
        self.handle_table.close(handle)
    }

    // ========== DB 视图 ==========

    pub fn db_open_value_handle(&mut self, key: &[u8]) -> Result<i32, i32> {
        // 限额检查
        if self.stats.kv_get_count >= self.budget.max_kv_get_count {
            return Err(-5); // 超出次数限制
        }
        self.stats.kv_get_count += 1;

        // 查询 DB
        match self.db_view.kv_get(key) {
            Ok(Some(value)) => {
                let handle = self.handle_table.register_dynamic(value)?;
                Ok(handle as i32)
            }
            Ok(None) => Ok(0), // 不存在
            Err(e) => {
                tracing::error!("kv_get failed: {}", e);
                Err(-4) // DB 错误
            }
        }
    }

    // ========== 命令操作 ==========

    pub fn cmd_set_key(&mut self, key: Bytes) -> Result<(), i32> {
        if self.readonly {
            return Err(-6); // 只读模式
        }
        self.command_buffer.push(WriteCommand::SetKey(key));
        Ok(())
    }

    pub fn cmd_set_value(&mut self, value: Bytes) -> Result<(), i32> {
        if self.readonly {
            return Err(-6);
        }
        self.command_buffer.push(WriteCommand::SetValue(value));
        Ok(())
    }

    pub fn cmd_set_ttl(&mut self, ttl_secs: u64) -> Result<(), i32> {
        if self.readonly {
            return Err(-6);
        }
        self.command_buffer.push(WriteCommand::SetTTL(ttl_secs));
        Ok(())
    }

    pub fn cmd_set_expires_at(&mut self, ts: u64) -> Result<(), i32> {
        if self.readonly {
            return Err(-6);
        }
        self.command_buffer.push(WriteCommand::SetExpiresAt(ts));
        Ok(())
    }

    pub fn cmd_clear_ttl(&mut self) -> Result<(), i32> {
        if self.readonly {
            return Err(-6);
        }
        self.command_buffer.push(WriteCommand::ClearTTL);
        Ok(())
    }

    pub fn cmd_set_reason(&mut self, reason: String) {
        self.command_buffer.set_reason(reason);
    }

    // ========== 内部方法 ==========

    /// 应用指令到当前上下文（管道语义）
    pub fn apply_commands(&mut self, commands: &[WriteCommand]) {
        self.handle_table.apply_commands(commands);
    }

    /// 是否有变更
    pub fn has_changes(&self) -> bool {
        self.command_buffer.has_changes()
    }

    /// 收集最终指令（消耗所有权）
    pub fn collect_commands(self) -> Vec<WriteCommand> {
        self.command_buffer.into_commands()
    }

    /// 获取命令引用（不消耗所有权）
    pub fn commands(&self) -> Vec<WriteCommand> {
        self.command_buffer.clone_commands()
    }

    /// 获取拒绝原因
    pub fn reject_reason(&self) -> Option<&str> {
        self.command_buffer.reject_reason()
    }

    /// 获取统计信息
    pub fn stats(&self) -> (u32, usize) {
        (self.stats.kv_get_count, self.stats.bytes_read_total)
    }

    // ========== 读路径结果操作（OnRead/ScanFilter）==========

    /// 设置变换后的值（OnRead Transform）
    pub fn read_set_transformed_value(&mut self, value: Bytes) {
        self.read_result.transformed_value = Some(value);
    }

    /// 设置读拒绝原因（OnRead Reject）
    pub fn read_set_reject_reason(&mut self, reason: String) {
        self.read_result.reject_reason = Some(reason);
    }

    /// 设置 ScanFilter 结果（Drop）
    pub fn read_set_drop(&mut self) {
        self.read_result.drop_entry = true;
    }

    /// 获取变换后的值
    pub fn read_get_transformed_value(&self) -> Option<&Bytes> {
        self.read_result.transformed_value.as_ref()
    }

    /// 获取读拒绝原因
    pub fn read_get_reject_reason(&self) -> Option<&str> {
        self.read_result.reject_reason.as_deref()
    }

    /// 是否 Drop（ScanFilter）
    pub fn read_is_drop(&self) -> bool {
        self.read_result.drop_entry
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    /// Mock DbView for testing
    struct MockDbView {
        data: std::collections::HashMap<Vec<u8>, Bytes>,
    }

    impl MockDbView {
        fn new() -> Self {
            let mut data = std::collections::HashMap::new();
            data.insert(b"exist".to_vec(), Bytes::from("value"));
            Self { data }
        }
    }

    impl boxkv_core::DbView for MockDbView {
        fn kv_get(&self, key: &[u8]) -> boxkv_core::db::error::Result<Option<Bytes>> {
            Ok(self.data.get(key).cloned())
        }
    }

    #[test]
    fn test_readonly_mode_blocks_commands() {
        // 只读模式下所有 cmd_* 应该返回 -6
        let db_view = Arc::new(MockDbView::new());
        let budget = BudgetConfig::default();

        let mut ctx = CallContext::new_readonly(
            Bytes::from("key"),
            Bytes::from("value"),
            None,
            db_view,
            budget,
            42, // sequence
        );

        // 验证序列号正确
        assert_eq!(ctx.ctx_sequence(), 42);

        // 所有命令应该被拒绝
        assert_eq!(ctx.cmd_set_key(Bytes::from("new_key")), Err(-6));
        assert_eq!(ctx.cmd_set_value(Bytes::from("new_value")), Err(-6));
        assert_eq!(ctx.cmd_set_ttl(60), Err(-6));
        assert_eq!(ctx.cmd_set_expires_at(1234567890), Err(-6));
        assert_eq!(ctx.cmd_clear_ttl(), Err(-6));

        // set_reason 不受限制（用于记录审计信息）
        ctx.cmd_set_reason("audit log".to_string());
        assert_eq!(ctx.reject_reason(), Some("audit log"));
    }

    #[test]
    fn test_writable_mode_allows_commands() {
        // 可写模式下命令正常工作
        let db_view = Arc::new(MockDbView::new());
        let budget = BudgetConfig::default();

        let mut ctx = CallContext::new_writable(
            Bytes::from("key"),
            Bytes::from("value"),
            None,
            db_view,
            budget,
        );

        // 验证没有序列号
        assert_eq!(ctx.ctx_sequence(), 0);

        // 所有命令应该成功
        assert_eq!(ctx.cmd_set_key(Bytes::from("new_key")), Ok(()));
        assert_eq!(ctx.cmd_set_value(Bytes::from("new_value")), Ok(()));
        assert_eq!(ctx.cmd_set_ttl(60), Ok(()));
        assert!(ctx.has_changes());

        let commands = ctx.commands();
        assert_eq!(commands.len(), 3);
    }

    #[test]
    fn test_kv_get_quota_enforcement() {
        // kv_get 次数限额测试
        let db_view = Arc::new(MockDbView::new());
        let budget = BudgetConfig {
            max_kv_get_count: 2, // 限制为 2 次
            ..BudgetConfig::default()
        };

        let mut ctx = CallContext::new_writable(
            Bytes::from("key"),
            Bytes::from("value"),
            None,
            db_view,
            budget,
        );

        // 第 1 次成功
        let result1 = ctx.db_open_value_handle(b"exist");
        assert!(result1.is_ok());
        assert!(result1.unwrap() >= 100); // 应返回动态句柄

        // 第 2 次成功
        let result2 = ctx.db_open_value_handle(b"exist");
        assert!(result2.is_ok());

        // 第 3 次应该失败（超限）
        let result3 = ctx.db_open_value_handle(b"exist");
        assert_eq!(result3, Err(-5));

        // 验证统计
        let (kv_get_count, _) = ctx.stats();
        assert_eq!(kv_get_count, 2); // 只统计成功的
    }

    #[test]
    fn test_bytes_read_quota_enforcement() {
        // bytes_read 累计限额测试
        let db_view = Arc::new(MockDbView::new());
        let budget = BudgetConfig {
            max_bytes_read_total: 10, // 限制为 10 字节
            ..BudgetConfig::default()
        };

        let mut ctx = CallContext::new_writable(
            Bytes::from("hello world"), // 11 字节
            Bytes::from("value"),
            None,
            db_view,
            budget,
        );

        let handle = ctx.ctx_key_handle();

        // 读取 5 字节成功
        let mut buf1 = vec![0u8; 5];
        let result1 = ctx.bytes_read(handle, 0, 5, &mut buf1);
        assert_eq!(result1, Ok(5));
        assert_eq!(&buf1, b"hello");

        // 再读取 5 字节成功（累计 10）
        let mut buf2 = vec![0u8; 5];
        let result2 = ctx.bytes_read(handle, 6, 5, &mut buf2);
        assert_eq!(result2, Ok(5));

        // 再读取应该失败（超限）
        // 注意：由于检查在更新后，所以统计会超出 1 字节
        let mut buf3 = vec![0u8; 1];
        let result3 = ctx.bytes_read(handle, 0, 1, &mut buf3);
        assert_eq!(result3, Err(-7));

        // 验证统计（会略微超出限制，因为检查在更新之后）
        let (_, bytes_read_total) = ctx.stats();
        assert_eq!(bytes_read_total, 11); // 10 + 1（最后一次尝试）
    }

    #[test]
    fn test_kv_get_not_found() {
        // kv_get 未找到应返回 0
        let db_view = Arc::new(MockDbView::new());
        let budget = BudgetConfig::default();

        let mut ctx = CallContext::new_writable(
            Bytes::from("key"),
            Bytes::from("value"),
            None,
            db_view,
            budget,
        );

        let result = ctx.db_open_value_handle(b"not_exist");
        assert_eq!(result, Ok(0)); // 不存在返回 0

        // 验证计数增加
        let (kv_get_count, _) = ctx.stats();
        assert_eq!(kv_get_count, 1);
    }

    #[test]
    fn test_handle_operations() {
        // 句柄基础操作测试
        let db_view = Arc::new(MockDbView::new());
        let budget = BudgetConfig::default();

        let ctx = CallContext::new_writable(
            Bytes::from("test_key"),
            Bytes::from("test_value"),
            Some(1234567890),
            db_view,
            budget,
        );

        // 验证句柄
        assert_eq!(ctx.ctx_key_handle(), 1);
        assert_eq!(ctx.ctx_value_handle(), 2);

        // 验证 value_kind（Expiring）
        assert_eq!(ctx.ctx_value_kind(), 1);

        // 验证 expires_at
        assert_eq!(ctx.ctx_expires_at(), 1234567890);

        // 验证 bytes_len
        assert_eq!(ctx.bytes_len(1), Ok(8)); // "test_key"
        assert_eq!(ctx.bytes_len(2), Ok(10)); // "test_value"

        // 验证 bytes_starts_with
        assert_eq!(ctx.bytes_starts_with(1, b"test"), Ok(1));
        assert_eq!(ctx.bytes_starts_with(1, b"fake"), Ok(0));

        // 验证 bytes_equals
        assert_eq!(ctx.bytes_equals(1, b"test_key"), Ok(1));
        assert_eq!(ctx.bytes_equals(1, b"wrong"), Ok(0));
    }

    #[test]
    fn test_read_result_buffer() {
        // 测试读路径结果缓冲（OnRead/ScanFilter）
        let db_view = Arc::new(MockDbView::new());
        let budget = BudgetConfig::default();

        let mut ctx = CallContext::new_readonly(
            Bytes::from("key"),
            Bytes::from("value"),
            None,
            db_view,
            budget,
            0,
        );

        // 测试 Transform 值设置
        ctx.read_set_transformed_value(Bytes::from("new_value"));
        assert_eq!(
            ctx.read_get_transformed_value(),
            Some(&Bytes::from("new_value"))
        );

        // 测试拒绝原因设置
        ctx.read_set_reject_reason("access denied".to_string());
        assert_eq!(ctx.read_get_reject_reason(), Some("access denied"));

        // 测试 Drop 标记
        assert!(!ctx.read_is_drop());
        ctx.read_set_drop();
        assert!(ctx.read_is_drop());
    }
}
