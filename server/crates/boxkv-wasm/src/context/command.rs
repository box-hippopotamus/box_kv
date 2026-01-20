//! 命令缓冲
use boxkv_core::hooks::WriteCommand;

/// 命令缓冲
pub struct CommandBuffer {
    /// 指令列表
    commands: Vec<WriteCommand>,

    /// 拒绝原因（Reject 时使用）
    reject_reason: Option<String>,
}

impl CommandBuffer {
    pub fn new() -> Self {
        Self {
            commands: Vec::new(),
            reject_reason: None,
        }
    }

    /// 推送指令
    pub fn push(&mut self, cmd: WriteCommand) {
        self.commands.push(cmd);
    }

    /// 设置拒绝原因
    pub fn set_reason(&mut self, reason: String) {
        self.reject_reason = Some(reason);
    }

    /// 是否有变更
    pub fn has_changes(&self) -> bool {
        !self.commands.is_empty()
    }

    /// 获取拒绝原因
    pub fn reject_reason(&self) -> Option<&str> {
        self.reject_reason.as_deref()
    }

    /// 转移所有权，返回指令列表
    pub fn into_commands(self) -> Vec<WriteCommand> {
        self.commands
    }

    /// 克隆命令列表（不消耗所有权）
    pub fn clone_commands(&self) -> Vec<WriteCommand> {
        self.commands.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn test_command_buffer() {
        let mut buf = CommandBuffer::new();

        assert!(!buf.has_changes());

        buf.push(WriteCommand::SetTTL(60));
        buf.push(WriteCommand::SetValue(Bytes::from("new")));

        assert!(buf.has_changes());

        let commands = buf.into_commands();
        assert_eq!(commands.len(), 2);
    }

    #[test]
    fn test_reject_reason() {
        let mut buf = CommandBuffer::new();
        buf.set_reason("invalid key".to_string());

        assert_eq!(buf.reject_reason(), Some("invalid key"));
    }
}
