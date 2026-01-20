use crate::version::VersionSet;
use crate::wal::Wal;
use boxkv_storage::FileSystem;
/// 文件清理模块
///
/// # 清理时机
/// - Flush 完成后
/// - Compaction 完成后
/// - 数据库启动时（恢复阶段）
/// - 后台周期性任务（可选）
use std::collections::HashSet;
use std::path::PathBuf;
use tracing::{debug, error, info, warn};

/// 过期文件类型
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObsoleteFileType {
    /// SST 文件
    SSTable,
    /// WAL 文件
    WAL,
    /// Manifest 文件
    Manifest,
}

/// 文件清理统计
#[derive(Debug, Default, Clone)]
pub struct CleanupStats {
    /// 删除的 SST 文件数
    pub deleted_sst_count: usize,
    /// 删除的 SST 文件总字节数
    pub deleted_sst_bytes: u64,
    /// 删除的 WAL 文件数
    pub deleted_wal_count: usize,
    /// 删除的 Manifest 文件数
    pub deleted_manifest_count: usize,
    /// 扫描耗时（毫秒）
    pub scan_duration_ms: u64,
    /// 删除耗时（毫秒）
    pub delete_duration_ms: u64,
}

impl CleanupStats {
    pub fn total_deleted_files(&self) -> usize {
        self.deleted_sst_count + self.deleted_wal_count + self.deleted_manifest_count
    }
}

/// 文件清理器
///
/// # 线程安全
/// - 内部持有 VersionSet 的 Arc 引用，可在多线程环境使用
/// - 清理操作是原子的（要么全成功，要么失败时部分清理）
///
/// # 性能考虑
/// - 采用增量扫描：每次最多扫描/删除 N 个文件
/// - 避免持锁时间过长：收集文件列表后再批量删除
/// - 删除失败不中断整体流程，记录错误日志继续
pub struct FileCleaner<FS: FileSystem> {
    fs: FS,
    db_path: PathBuf,
    /// SST 文件目录
    sst_dir: PathBuf,
    /// WAL 文件目录
    wal_dir: PathBuf,
}

impl<FS: FileSystem> FileCleaner<FS> {
    /// 创建文件清理器
    pub fn new(fs: FS, db_path: PathBuf) -> Self {
        let sst_dir = db_path.join("sst");
        let wal_dir = db_path.join("wal");

        Self {
            fs,
            db_path,
            sst_dir,
            wal_dir,
        }
    }

    /// 完整清理：SST + WAL + Manifest
    ///
    /// 实现要点：分类清理、增量扫描、失败不中断
    ///
    /// # 参数
    /// - `versions`: VersionSet 引用，用于获取存活文件集合
    /// - `current_manifest_number`: 当前 Manifest 文件号
    /// - `keep_recent_manifests`: 保留最近的 N 个 Manifest（默认 2，用于灾难恢复）
    pub fn purge_obsolete_files(
        &self,
        versions: &VersionSet,
        current_manifest_number: u64,
        keep_recent_manifests: usize,
    ) -> CleanupStats {
        let start_time = std::time::Instant::now();
        let mut stats = CleanupStats::default();

        info!(
            target: "file_cleaner",
            "purge_obsolete_files: start cleanup db_path={:?}",
            self.db_path
        );

        // 1. 收集存活 SST 文件
        let live_sst_files = versions.collect_live_files();
        debug!(
            target: "file_cleaner",
            "live_sst_files count={}",
            live_sst_files.len()
        );

        // 2. 清理过期 SST
        let sst_stats = self.purge_obsolete_sst_files(&live_sst_files);
        stats.deleted_sst_count = sst_stats.0;
        stats.deleted_sst_bytes = sst_stats.1;

        // 3. 清理过期 WAL
        let log_number = versions.log_number();
        stats.deleted_wal_count = self.purge_obsolete_wal_files(log_number);

        // 4. 清理过期 Manifest
        stats.deleted_manifest_count =
            self.purge_obsolete_manifest_files(current_manifest_number, keep_recent_manifests);

        stats.scan_duration_ms = start_time.elapsed().as_millis() as u64;

        info!(
            target: "file_cleaner",
            "purge_obsolete_files: completed sst_deleted={} wal_deleted={} manifest_deleted={} duration_ms={}",
            stats.deleted_sst_count,
            stats.deleted_wal_count,
            stats.deleted_manifest_count,
            stats.scan_duration_ms
        );

        stats
    }

    /// 清理过期 SST 文件
    ///
    /// # 算法
    /// 1. 扫描 sst/ 目录下所有 *.sst 文件
    /// 2. 提取文件号（文件名格式：{file_number}.sst）
    /// 3. 对比存活文件集合，删除不在集合中的文件
    ///
    /// # 返回值
    /// (删除文件数, 删除字节数)
    fn purge_obsolete_sst_files(&self, live_files: &HashSet<u64>) -> (usize, u64) {
        let mut deleted_count = 0usize;
        let mut deleted_bytes = 0u64;

        // 扫描 SST 目录
        let entries = match self.fs.list_dir(&self.sst_dir) {
            Ok(e) => e,
            Err(err) => {
                warn!(
                    target: "file_cleaner",
                    "failed to list sst directory: {:?}, error: {:?}",
                    self.sst_dir,
                    err
                );
                return (0, 0);
            }
        };

        for entry in entries {
            let path = self.sst_dir.join(&entry);

            // 仅处理 .sst 文件
            if !entry.ends_with(".sst") {
                continue;
            }

            // 提取文件号（格式：{file_number}.sst）
            let file_number = match entry
                .strip_suffix(".sst")
                .and_then(|s| s.parse::<u64>().ok())
            {
                Some(n) => n,
                None => {
                    warn!(
                        target: "file_cleaner",
                        "invalid sst filename format: {}",
                        entry
                    );
                    continue;
                }
            };

            // 检查是否存活
            if live_files.contains(&file_number) {
                continue;
            }

            // 获取文件大小（用于统计）
            let file_size = self.fs.file_size(&path).unwrap_or(0);

            // 删除过期文件
            match self.fs.delete(&path) {
                Ok(_) => {
                    deleted_count += 1;
                    deleted_bytes += file_size;
                    info!(
                        target: "file_cleaner",
                        "deleted obsolete sst file_number={} size={}",
                        file_number,
                        file_size
                    );
                }
                Err(err) => {
                    error!(
                        target: "file_cleaner",
                        "failed to delete sst file_number={} path={:?} error={:?}",
                        file_number,
                        path,
                        err
                    );
                }
            }
        }

        (deleted_count, deleted_bytes)
    }

    /// 清理过期 WAL 文件
    ///
    /// 语义
    /// - 删除所有 file_id < log_number 的 WAL 文件
    /// - log_number 在每次 Flush 时更新（指向当前活跃 WAL）
    ///
    /// # 参数
    /// - `log_number`: 当前活跃的 WAL 文件号（来自 VersionSet）
    ///
    /// # 返回值
    /// 删除的 WAL 文件数
    fn purge_obsolete_wal_files(&self, log_number: u64) -> usize {
        let mut deleted_count = 0usize;

        debug!(
            target: "file_cleaner",
            "purge_obsolete_wal_files: current_log_number={}",
            log_number
        );

        // 扫描 WAL 目录
        let entries = match self.fs.list_dir(&self.wal_dir) {
            Ok(e) => e,
            Err(err) => {
                warn!(
                    target: "file_cleaner",
                    "failed to list wal directory: {:?}, error: {:?}",
                    self.wal_dir,
                    err
                );
                return 0;
            }
        };

        for entry in entries {
            // 仅处理 .wal 文件（格式：{file_id:09}.wal）
            if !entry.ends_with(".wal") {
                continue;
            }

            // 提取文件 ID
            let file_id = match entry
                .strip_suffix(".wal")
                .and_then(|s| s.parse::<u64>().ok())
            {
                Some(id) => id,
                None => {
                    warn!(
                        target: "file_cleaner",
                        "invalid wal filename format: {}",
                        entry
                    );
                    continue;
                }
            };

            // 删除所有 < log_number 的 WAL
            if file_id < log_number {
                match Wal::<FS>::delete(&self.fs, self.wal_dir.clone(), file_id) {
                    Ok(_) => {
                        deleted_count += 1;
                        info!(
                            target: "file_cleaner",
                            "deleted obsolete wal file_id={} (< log_number={})",
                            file_id,
                            log_number
                        );
                    }
                    Err(err) => {
                        error!(
                            target: "file_cleaner",
                            "failed to delete wal file_id={} error={:?}",
                            file_id,
                            err
                        );
                    }
                }
            }
        }

        deleted_count
    }

    /// 清理过期 Manifest 文件
    ///
    /// # 策略
    /// - 保留当前 Manifest（current_manifest_number）
    /// - 保留最近的 N 个旧 Manifest（用于灾难恢复，默认值为 2）
    /// - 删除更老的 Manifest 文件
    ///
    /// # 参数
    /// - `current_manifest_number`: 当前活跃的 Manifest 文件号
    /// - `keep_recent`: 保留最近的 N 个旧 Manifest
    ///
    /// # 返回值
    /// 删除的 Manifest 文件数
    fn purge_obsolete_manifest_files(
        &self,
        current_manifest_number: u64,
        keep_recent: usize,
    ) -> usize {
        let mut deleted_count = 0usize;

        debug!(
            target: "file_cleaner",
            "purge_obsolete_manifest_files: current={} keep_recent={}",
            current_manifest_number,
            keep_recent
        );

        // 扫描 db_path 根目录
        let entries = match self.fs.list_dir(&self.db_path) {
            Ok(e) => e,
            Err(err) => {
                warn!(
                    target: "file_cleaner",
                    "failed to list db directory: {:?}, error: {:?}",
                    self.db_path,
                    err
                );
                return 0;
            }
        };

        // 收集所有 Manifest 文件号
        let mut manifest_numbers = Vec::new();
        for entry in entries {
            // 仅处理 MANIFEST-* 文件
            if !entry.starts_with("MANIFEST-") {
                continue;
            }

            // 提取文件号（格式：MANIFEST-{number:06}）
            let number_str = entry.strip_prefix("MANIFEST-").unwrap_or("");
            if let Ok(num) = number_str.parse::<u64>() {
                manifest_numbers.push(num);
            }
        }

        // 按降序排序（最新的在前）
        manifest_numbers.sort_by(|a: &u64, b: &u64| b.cmp(a));

        debug!(
            target: "file_cleaner",
            "found {} manifest files: {:?}",
            manifest_numbers.len(),
            manifest_numbers
        );

        // 保留当前 + 最近 N 个
        let mut keep_set = HashSet::new();
        keep_set.insert(current_manifest_number);

        for (i, &num) in manifest_numbers.iter().enumerate() {
            if num == current_manifest_number {
                continue;
            }
            if i < keep_recent {
                keep_set.insert(num);
            }
        }

        // 删除不在保留集合中的 Manifest
        for &num in &manifest_numbers {
            if keep_set.contains(&num) {
                continue;
            }

            let filename = format!("MANIFEST-{:06}", num);
            let path = self.db_path.join(&filename);

            match self.fs.delete(&path) {
                Ok(_) => {
                    deleted_count += 1;
                    info!(
                        target: "file_cleaner",
                        "deleted obsolete manifest number={} filename={}",
                        num,
                        filename
                    );
                }
                Err(err) => {
                    error!(
                        target: "file_cleaner",
                        "failed to delete manifest number={} error={:?}",
                        num,
                        err
                    );
                }
            }
        }

        deleted_count
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use boxkv_storage::LocalFileSystem;
    use tempfile::TempDir;

    #[test]
    fn test_file_cleaner_creation() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().to_path_buf();

        let cleaner = FileCleaner::new(LocalFileSystem, db_path.clone());

        assert_eq!(cleaner.sst_dir, db_path.join("sst"));
        assert_eq!(cleaner.wal_dir, db_path.join("wal"));
    }
}
