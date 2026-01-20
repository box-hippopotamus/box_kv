//! 时间相关工具函数
//!
//! 提供不会 panic 的时间工具，异常场景（如系统时间早于 UNIX 纪元）将打印错误并返回安全兜底值。

use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// 获取当前时间的秒级时间戳（自 UNIX 纪元起）。
///
/// 返回：当前秒级时间戳；若系统时间早于 1970-01-01，则返回 0 并记录错误日志。
#[inline]
pub fn current_timestamp_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_else(|e| {
            tracing::error!(
                "System time is before UNIX epoch: {}. Using fallback timestamp 0.",
                e
            );
            0
        })
}

/// 获取当前时间的毫秒级时间戳（自 UNIX 纪元起）。
///
/// 返回：当前毫秒级时间戳；若系统时间早于 1970-01-01，则返回 0 并记录错误日志。
#[inline]
pub fn current_timestamp_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or_else(|e| {
            tracing::error!(
                "System time is before UNIX epoch: {}. Using fallback timestamp 0.",
                e
            );
            0
        })
}

/// 获取自 UNIX 纪元以来的持续时间。
///
/// 返回：`Duration`；若系统时间早于 1970-01-01，则返回 `Duration::from_secs(0)` 并记录错误日志。
#[inline]
pub fn duration_since_epoch() -> Duration {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|e| {
            tracing::error!(
                "System time is before UNIX epoch: {}. Using fallback duration 0.",
                e
            );
            Duration::from_secs(0)
        })
}

/// 根据 TTL 计算过期时间戳。
///
/// 参数：`ttl_secs` 为秒数。
///
/// 返回：`now + ttl_secs`；若当前时间异常，则仍做饱和相加，保证不会溢出。
#[inline]
pub fn expire_at_from_ttl(ttl_secs: u64) -> u64 {
    let now = current_timestamp_secs();
    now.saturating_add(ttl_secs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_current_timestamp_secs() {
        let ts = current_timestamp_secs();
        assert!(ts > 1577836800);
    }

    #[test]
    fn test_current_timestamp_millis() {
        let ts = current_timestamp_millis();
        assert!(ts > 1577836800000);
    }

    #[test]
    fn test_duration_since_epoch() {
        let duration = duration_since_epoch();
        assert!(duration.as_secs() > 50 * 365 * 24 * 3600);
    }

    #[test]
    fn test_expire_at_from_ttl() {
        let now = current_timestamp_secs();
        let ttl = 3600;
        let expire_at = expire_at_from_ttl(ttl);

        assert!(expire_at >= now + ttl);
        assert!(expire_at <= now + ttl + 1);
    }

    #[test]
    fn test_expire_at_from_ttl_no_overflow() {
        let expire_at = expire_at_from_ttl(u64::MAX);
        assert!(expire_at > 0);
    }
}
