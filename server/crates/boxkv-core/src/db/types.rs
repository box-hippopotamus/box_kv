use crate::memtable::Memtable;
use crate::version::Version;
/// DB 内部类型定义
/// - SuperVersion：只读视图
/// - WriteStallCondition：写停止条件
use std::sync::Arc;

/// SuperVersion：数据库只读视图（Immutable）
/// - 包含：当前可写 Memtable、所有不可变 Memtable、当前 Version、当前最大序列号
/// - 读路径仅访问该视图；后台 Flush/Compaction 完成后通过原子替换整体更新视图
#[derive(Clone)]
pub struct SuperVersion {
    pub mem: Arc<Memtable>,
    pub imm: Arc<Vec<Arc<Memtable>>>,
    pub version: Arc<Version>,
    pub sequence: u64,
}

/// 写停止条件：根据 L0 文件数和不可变 Memtable 数量判断
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteStallCondition {
    /// 正常，无需停止
    Normal,
    /// 软停止：延迟写入（sleep）
    SoftStall,
    /// 硬停止：完全阻塞写入直到 Flush/Compaction 完成
    HardStall,
}

impl WriteStallCondition {
    /// 根据不可变 Memtable 数量和 L0 文件数量计算写停止条件
    /// - imm_count >= hard_limit: HardStall
    /// - imm_count >= soft_limit: SoftStall
    /// - l0_count >= hard_limit: HardStall
    /// - l0_count >= soft_limit: SoftStall
    pub fn compute(
        imm_count: usize,
        l0_count: usize,
        soft_limit: usize,
        hard_limit: usize,
    ) -> Self {
        if imm_count >= hard_limit || l0_count >= hard_limit {
            WriteStallCondition::HardStall
        } else if imm_count >= soft_limit || l0_count >= soft_limit {
            WriteStallCondition::SoftStall
        } else {
            WriteStallCondition::Normal
        }
    }
}
