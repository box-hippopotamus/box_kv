use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// 压缩指标统计
///
/// 使用原子计数记录压缩过程中的各项指标，包括任务数、文件数、字节数等。
/// 所有计数器使用 Relaxed 内存序，适合统计场景。
#[derive(Default)]
pub struct CompactionMetrics {
    /// 已启动的压缩任务数量
    jobs_started: AtomicU64,
    /// 已完成的压缩任务数量
    jobs_finished: AtomicU64,
    /// 压缩读入的累计字节数
    bytes_in: AtomicU64,
    /// 压缩写出的累计字节数
    bytes_out: AtomicU64,
    /// 压缩读入的文件数（inputs_k + inputs_k+1）
    files_in: AtomicU64,
    /// 压缩写出的文件数
    files_out: AtomicU64,
    /// 被丢弃的删除标记（tombstone）条目数量
    dropped_tombstone: AtomicU64,
    /// 被丢弃的过期条目（TTL 到期）数量
    dropped_ttl: AtomicU64,
    /// 被丢弃的旧版本条目数量（保留首个可见版本）
    dropped_older_version: AtomicU64,
    /// 当前待压缩的累计字节数（用于背压控制）
    pending_bytes: AtomicU64,
}

impl CompactionMetrics {
    /// 创建新的指标实例（Arc 包装，便于跨线程共享）
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// 增加已启动任务计数
    pub fn inc_jobs_started(&self) {
        self.jobs_started.fetch_add(1, Ordering::Relaxed);
    }

    /// 增加已完成任务计数
    pub fn inc_jobs_finished(&self) {
        self.jobs_finished.fetch_add(1, Ordering::Relaxed);
    }

    /// 累加读入字节数
    pub fn add_bytes_in(&self, v: u64) {
        self.bytes_in.fetch_add(v, Ordering::Relaxed);
    }

    /// 累加写出字节数
    pub fn add_bytes_out(&self, v: u64) {
        self.bytes_out.fetch_add(v, Ordering::Relaxed);
    }

    /// 累加读入文件数
    pub fn add_files_in(&self, v: u64) {
        self.files_in.fetch_add(v, Ordering::Relaxed);
    }

    /// 累加写出文件数
    pub fn add_files_out(&self, v: u64) {
        self.files_out.fetch_add(v, Ordering::Relaxed);
    }

    /// 累加被丢弃的 tombstone 数量
    pub fn add_dropped_tombstone(&self, v: u64) {
        self.dropped_tombstone.fetch_add(v, Ordering::Relaxed);
    }

    /// 累加被丢弃的 TTL 过期条目数量
    pub fn add_dropped_ttl(&self, v: u64) {
        self.dropped_ttl.fetch_add(v, Ordering::Relaxed);
    }

    /// 累加被丢弃的旧版本条目数量
    pub fn add_dropped_older_version(&self, v: u64) {
        self.dropped_older_version.fetch_add(v, Ordering::Relaxed);
    }

    /// 更新当前待压缩字节数
    pub fn set_pending_bytes(&self, v: u64) {
        self.pending_bytes.store(v, Ordering::Relaxed);
    }

    /// 读取当前指标快照（不可变结构，便于展示和测试）
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            jobs_started: self.jobs_started.load(Ordering::Relaxed),
            jobs_finished: self.jobs_finished.load(Ordering::Relaxed),
            bytes_in: self.bytes_in.load(Ordering::Relaxed),
            bytes_out: self.bytes_out.load(Ordering::Relaxed),
            files_in: self.files_in.load(Ordering::Relaxed),
            files_out: self.files_out.load(Ordering::Relaxed),
            dropped_tombstone: self.dropped_tombstone.load(Ordering::Relaxed),
            dropped_ttl: self.dropped_ttl.load(Ordering::Relaxed),
            dropped_older_version: self.dropped_older_version.load(Ordering::Relaxed),
            pending_bytes: self.pending_bytes.load(Ordering::Relaxed),
        }
    }
}

/// 压缩指标快照的只读结构，便于序列化/打印/断言
#[derive(Clone, Copy, Debug, Default)]
pub struct MetricsSnapshot {
    /// 已启动的压缩任务数量
    pub jobs_started: u64,
    /// 已完成的压缩任务数量
    pub jobs_finished: u64,
    /// 压缩读入的累计字节数
    pub bytes_in: u64,
    /// 压缩写出的累计字节数
    pub bytes_out: u64,
    /// 读入的文件数
    pub files_in: u64,
    /// 写出的文件数
    pub files_out: u64,
    /// 丢弃的 tombstone 数量
    pub dropped_tombstone: u64,
    /// 丢弃的 TTL 过期条目数量
    pub dropped_ttl: u64,
    /// 丢弃的旧版本条目数量
    pub dropped_older_version: u64,
    /// 当前待压缩的累计字节数
    pub pending_bytes: u64,
}
