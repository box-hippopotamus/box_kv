use serde::{Deserialize, Serialize};

/// Compaction 压缩配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactionConfig {
    /// 层级总数（含 L0）。例如 7 表示 L0..L6
    pub max_levels: u32,
    /// 当 L0 文件数达到该阈值时触发 L0→L1 压缩
    pub level0_trigger: usize,
    /// 各层总容量倍数（Lk ≈ L1 × multiplier^(k-1)）
    pub level_size_multiplier: u32,
    /// L1 目标总容量（MB）
    pub level1_size_mb: u64,
    /// 目标 SSTable 文件大小（MB）
    pub target_file_size_base_mb: u64,
    /// 后台压缩并发上限
    pub max_background_jobs: usize,
    /// 子压缩并行度（>=1；1 表示不切分）
    pub max_subcompactions: usize,
    /// 软限流阈值（待压缩字节数）
    pub soft_pending_compaction_bytes: u64,
    /// 硬限流阈值（待压缩字节数）
    pub hard_pending_compaction_bytes: u64,
    /// 单次压缩的最大输入文件数（0 表示不限制）
    pub max_compaction_input_files: usize,
    /// 单次压缩的最大输入字节数（0 表示不限制）
    pub max_compaction_input_bytes: u64,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            max_levels: 7,
            level0_trigger: 4,
            level_size_multiplier: 10,
            level1_size_mb: 256,
            target_file_size_base_mb: 64,
            max_background_jobs: 4,
            max_subcompactions: 1,
            soft_pending_compaction_bytes: 1024 * 1024 * 1024, // 1GB
            hard_pending_compaction_bytes: 8 * 1024 * 1024 * 1024, // 8GB
            max_compaction_input_files: 0,
            max_compaction_input_bytes: 0,
        }
    }
}

impl CompactionConfig {
    /// 计算某层的目标总容量（字节）
    /// - L0 返回 u64::MAX（不受容量约束）
    /// - L1 = level1_size_mb
    /// - Lk = L1 × level_size_multiplier^(k-1)
    pub fn target_level_bytes(&self, level: u32) -> u64 {
        if level == 0 {
            return u64::MAX;
        }
        let base = self.level1_size_mb.saturating_mul(1024 * 1024);
        let mut sz = base;
        for _ in 1..level {
            sz = sz.saturating_mul(self.level_size_multiplier as u64);
        }
        sz
    }

    /// 计算目标文件大小（字节）
    pub fn target_file_size_bytes(&self, _level: u32) -> u64 {
        self.target_file_size_base_mb.saturating_mul(1024 * 1024)
    }
}
