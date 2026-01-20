use crate::cache::TableCache;
use crate::db::error::{DBError, Result};
use crate::db::types::SuperVersion;
use arc_swap::ArcSwap;
use boxkv_common::types::ValueType;
use bytes::Bytes;
/// 读取路径模块
/// - get：点查询实现
/// - 搜索顺序：Mem → Imms → L0 → L1+
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// 点查询实现
/// - 顺序：Mem → Imms（新→旧）→ L0（新→旧）→ L1+（二分查找单文件）
/// - 使用 TableCache 复用已打开的 SSTableReader
/// - 考虑 Tombstone 与 TTL 过滤
/// - 零拷贝：直接使用 Bytes，返回 Bytes
/// - 序列号过滤：只返回 seq <= read_seq 的版本（MVCC 语义）
pub fn get(
    key: Bytes,
    super_version: &Arc<ArcSwap<SuperVersion>>,
    table_cache: &Arc<TableCache>,
    closed: &AtomicBool,
    read_sequence: u64,
) -> Result<Option<Bytes>> {
    // 检查是否关闭（原子操作，无锁）
    if closed.load(Ordering::Acquire) {
        return Err(DBError::Closed);
    }

    let sv = super_version.load().as_ref().clone();
    let now_secs = boxkv_common::time::current_timestamp_secs();

    // 1. 在 Memtable 中查找（MVCC 序列号过滤）
    // 使用 get_at 方法直接获取 read_sequence 时刻可见的版本
    if let Some((value, _seq)) = sv.mem.get_at(key.clone(), read_sequence) {
        return Ok(extract_value(value, now_secs));
    }

    // 2. 在不可变 Memtable 中查找（从新到旧，MVCC 序列号过滤）
    for imm_mem in sv.imm.iter() {
        if let Some((value, _seq)) = imm_mem.get_at(key.clone(), read_sequence) {
            return Ok(extract_value(value, now_secs));
        }
    }

    // 3. 在 L0 中查找（从新到旧，按文件范围过滤 + 序列号过滤）
    if let Some(l0) = sv.version.level(0) {
        for f in l0.files().iter().rev() {
            // 跳过不在范围内的文件
            if f.smallest.user_key().as_ref() > key.as_ref()
                || key.as_ref() > f.largest.user_key().as_ref()
            {
                continue;
            }

            if let Ok(reader) = table_cache.get_reader(f.file_number) {
                if let Ok(Some(v)) = reader.get_at(key.clone(), read_sequence) {
                    return Ok(extract_value(v, now_secs));
                }
            }
        }
    }

    // 4. 在 L1+ 中查找（二分查找单文件 + 序列号过滤）
    for level in 1..sv.version.num_levels() {
        let meta = match sv.version.level(level) {
            Some(m) => m,
            None => continue,
        };

        // 使用 find_file 进行 O(log n) 二分查找，直接定位唯一文件
        let f = match meta.find_file(&key) {
            Some(f) => f,
            None => continue,
        };

        if let Ok(reader) = table_cache.get_reader(f.file_number) {
            if let Ok(Some(v)) = reader.get_at(key.clone(), read_sequence) {
                return Ok(extract_value(v, now_secs));
            }
        }
    }

    Ok(None)
}

/// 提取值并处理 TTL 和 Tombstone（零拷贝：直接返回 Bytes）
fn extract_value(value: ValueType, now_secs: u64) -> Option<Bytes> {
    match value {
        ValueType::Normal(data) => Some(data),
        ValueType::Expiring { data, expire_at } => {
            if now_secs < expire_at {
                Some(data)
            } else {
                None
            }
        }
        ValueType::Tombstone => None,
    }
}
