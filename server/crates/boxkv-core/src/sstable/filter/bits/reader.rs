use bytes::Bytes;

/// FilterBitsReader trait
pub trait FilterBitsReader: Send + Sync {
    /// 检查 key 是否可能存在
    fn may_match(&self, entry: Bytes) -> bool;

    /// 批量检查多个 keys
    fn may_match_batch(&self, entries: &[Bytes]) -> Vec<bool> {
        entries
            .iter()
            .map(|entry| self.may_match(entry.clone()))
            .collect()
    }
}
