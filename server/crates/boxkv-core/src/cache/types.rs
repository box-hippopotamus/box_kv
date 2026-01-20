use std::hash::{Hash, Hasher};

/// Block 缓存键
///
/// 由文件 ID 和块偏移量组成，唯一标识一个 Block
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockCacheKey {
    /// SSTable 文件 ID（通常是文件路径的 hash）
    pub file_id: u64,
    /// Block 在文件中的偏移量
    pub block_offset: u64,
}

impl BlockCacheKey {
    /// 创建新的 BlockCacheKey
    pub fn new(file_id: u64, block_offset: u64) -> Self {
        Self {
            file_id,
            block_offset,
        }
    }
}

impl Hash for BlockCacheKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.file_id.hash(state);
        self.block_offset.hash(state);
    }
}
