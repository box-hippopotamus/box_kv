use crate::sstable::block::RESTART_POINT_LEN;
use crate::sstable::block::types::{BlockCodec, DecodedKey};
use boxkv_common::varint;
use bytes::Bytes;
use thiserror::Error;

/// Block 读取错误
#[derive(Debug, Error)]
pub enum ReadError<E: std::error::Error> {
    /// Block 数据损坏
    #[error("Block data corrupted: {message}")]
    CorruptedBlock { message: String },

    /// 重启点数据无效
    #[error("Invalid restart points")]
    InvalidRestartPoints,

    /// 数据截断（长度不足）
    #[error("Data truncated: expected {expected} bytes, got {actual}")]
    TruncatedData { expected: usize, actual: usize },

    /// 偏移量越界
    #[error("Offset {offset} out of bounds (max: {max})")]
    OffsetOutOfBounds { offset: usize, max: usize },

    /// 编解码器错误
    #[error("Codec error: {0}")]
    CodecError(#[source] E),

    /// Varint 解码错误
    #[error("Varint error: {0}")]
    VarintError(String),

    /// Entry 格式错误
    #[error("Invalid entry: {message}")]
    InvalidEntry { message: String },
}

/// 通用 Block 读取器
pub struct BlockReader<C: BlockCodec> {
    /// 编解码器实例
    codec: C,

    /// 完整的 Block 数据
    data: Bytes,

    /// 解析后的重启点列表（偏移量）
    restart_points: Vec<u32>,

    /// 数据部分的结束位置（重启点数据的起始位置）
    data_end: usize,
}

impl<C: BlockCodec> BlockReader<C> {
    /// 从原始 Block 数据创建读取器
    ///
    /// # Block 数据格式
    /// ```text
    /// [Data Part: entries]
    /// [RestartPoint₁: u32][RestartPoint₂: u32]...[RestartPointₙ: u32]
    /// [RestartPointCount: u32]
    /// ```
    pub fn new(codec: C, block_data: Bytes) -> Result<Self, ReadError<C::Error>> {
        // 最小长度：至少包含 restart_count
        if block_data.len() < RESTART_POINT_LEN {
            return Err(ReadError::TruncatedData {
                expected: RESTART_POINT_LEN,
                actual: block_data.len(),
            });
        }

        // 解析重启点数据
        let (restart_points, data_end) = Self::parse_restart_points(&block_data)?;

        // 验证重启点的有效性
        Self::validate_restart_points(&restart_points, data_end)?;

        Ok(Self {
            codec,
            data: block_data,
            restart_points,
            data_end,
        })
    }

    /// 点查询：查找指定 key 对应的 value
    ///
    /// # 查询流程
    /// 1. 二分查找合适的重启点
    /// 2. 从重启点开始线性扫描
    /// 3. 找到匹配的 key，解码并返回 value
    ///
    /// # 参数
    /// - `target_key`: 目标 key
    ///
    /// # 返回
    /// - `Some(value)`: 找到对应的 value
    /// - `None`: 未找到
    pub fn get(&self, target_key: &C::Key) -> Result<Option<C::Value>, ReadError<C::Error>> {
        // 二分查找合适的重启点
        let restart_idx = self.binary_search_restart_point(target_key)?;

        // 从重启点开始线性扫描
        self.linear_search_from_restart(restart_idx, target_key)
    }

    /// 创建 Block 迭代器（从头开始）
    pub fn iter(&self) -> BlockIterator<'_, C> {
        BlockIterator::new(self)
    }

    /// 获取重启点数量
    pub fn restart_point_count(&self) -> usize {
        self.restart_points.len()
    }

    /// 获取数据部分大小（不包括重启点元数据）
    pub fn data_size(&self) -> usize {
        self.data_end
    }

    /// 获取总大小（包含重启点）
    pub fn total_size(&self) -> usize {
        self.data.len()
    }

    /// 获取 Block 的统计信息
    pub fn stats(&self) -> BlockStats {
        let restart_metadata_size =
            self.restart_points.len() * RESTART_POINT_LEN + RESTART_POINT_LEN;

        let estimated_entry_count = if self.restart_points.is_empty() {
            0
        } else {
            self.restart_points.len() * 16 // 假设 restart_interval = 16
        };

        let avg_entry_size = if estimated_entry_count > 0 {
            self.data_end / estimated_entry_count
        } else {
            0
        };

        let estimated_compression_ratio = if !self.data.is_empty() {
            self.data_end as f64 / self.data.len() as f64
        } else {
            1.0
        };

        BlockStats {
            estimated_entry_count,
            data_size: self.data_end,
            restart_point_count: self.restart_points.len(),
            restart_metadata_size,
            total_size: self.data.len(),
            avg_entry_size,
            estimated_compression_ratio,
        }
    }

    /// 检查 Block 数据的完整性
    ///
    /// 验证：
    /// - 重启点偏移量在有效范围内
    /// - 数据长度合法
    pub fn verify_integrity(&self) -> Result<(), ReadError<C::Error>> {
        Self::validate_restart_points(&self.restart_points, self.data_end)?;
        Ok(())
    }

    /// 获取使用的编解码器
    pub fn codec(&self) -> &C {
        &self.codec
    }

    /// 获取原始数据（调试用）
    pub fn raw_data(&self) -> &Bytes {
        &self.data
    }

    // ==================== 私有辅助方法 ====================

    /// 二分查找合适的重启点
    ///
    /// 返回 <= target_key 的最大重启点索引
    fn binary_search_restart_point(
        &self,
        target_key: &C::Key,
    ) -> Result<usize, ReadError<C::Error>> {
        if self.restart_points.is_empty() {
            return Ok(0);
        }

        let mut left = 0;
        let mut right = self.restart_points.len();
        let mut result = 0;

        while left < right {
            let mid = left + (right - left) / 2;
            let restart_offset = self.restart_points[mid] as usize;

            // 解码重启点的 key（重启点不使用前缀压缩）
            let decoded = self.decode_entry_at(restart_offset, None)?;

            if decoded.key <= *target_key {
                result = mid;
                left = mid + 1;
            } else {
                right = mid;
            }
        }

        Ok(result)
    }

    /// 从重启点开始线性搜索
    fn linear_search_from_restart(
        &self,
        restart_index: usize,
        target_key: &C::Key,
    ) -> Result<Option<C::Value>, ReadError<C::Error>> {
        let mut current_pos = self.restart_points[restart_index] as usize;
        let mut prev_key: Option<C::Key> = None;

        while current_pos < self.data_end {
            let decoded = self.decode_entry_at(current_pos, prev_key.as_ref())?;

            if decoded.key == *target_key {
                // 找到匹配，解码 value
                let value = self.decode_value_at(decoded.value_offset, decoded.value_len)?;
                return Ok(Some(value));
            } else if decoded.key > *target_key {
                // key 已经超过 target，不存在
                return Ok(None);
            } else {
                // 继续扫描
                prev_key = Some(decoded.key);
                current_pos += decoded.consumed_bytes;
            }
        }

        Ok(None)
    }

    /// 解析重启点数据
    ///
    /// # 返回
    /// - `Vec<u32>`: 重启点偏移列表
    /// - `usize`: 数据部分结束位置
    fn parse_restart_points(block_data: &[u8]) -> Result<(Vec<u32>, usize), ReadError<C::Error>> {
        let len = block_data.len();

        // 读取 restart_count
        let restart_count_pos = len - RESTART_POINT_LEN;
        let restart_count_bytes: [u8; RESTART_POINT_LEN] = block_data[restart_count_pos..]
            .try_into()
            .map_err(|_| ReadError::CorruptedBlock {
                message: "Invalid restart count".to_string(),
            })?;
        let restart_count = u32::from_be_bytes(restart_count_bytes);

        if restart_count == 0 {
            return Err(ReadError::InvalidRestartPoints);
        }

        // 计算重启点数据的位置
        let restart_points_size = restart_count as usize * RESTART_POINT_LEN;
        let restart_points_start = len
            .checked_sub(RESTART_POINT_LEN + restart_points_size)
            .ok_or_else(|| ReadError::CorruptedBlock {
                message: "Restart points overflow".to_string(),
            })?;

        // 解析所有重启点
        let mut restart_points = Vec::with_capacity(restart_count as usize);
        for i in 0..restart_count as usize {
            let offset = restart_points_start + i * RESTART_POINT_LEN;
            let point_bytes: [u8; RESTART_POINT_LEN] = block_data
                [offset..offset + RESTART_POINT_LEN]
                .try_into()
                .map_err(|_| ReadError::CorruptedBlock {
                    message: format!("Invalid restart point at index {}", i),
                })?;
            let point = u32::from_be_bytes(point_bytes);
            restart_points.push(point);
        }

        Ok((restart_points, restart_points_start))
    }

    /// 验证重启点的有效性
    fn validate_restart_points(
        restart_points: &[u32],
        data_end: usize,
    ) -> Result<(), ReadError<C::Error>> {
        for (i, &point) in restart_points.iter().enumerate() {
            if point as usize >= data_end {
                return Err(ReadError::OffsetOutOfBounds {
                    offset: point as usize,
                    max: data_end,
                });
            }

            // 重启点应该是递增的
            if i > 0 && point <= restart_points[i - 1] {
                return Err(ReadError::CorruptedBlock {
                    message: format!(
                        "Restart points not in order: {} <= {} at index {}",
                        point,
                        restart_points[i - 1],
                        i
                    ),
                });
            }
        }

        Ok(())
    }

    /// 在指定位置解码 entry（只解码 key）
    fn decode_entry_at(
        &self,
        offset: usize,
        prev_key: Option<&C::Key>,
    ) -> Result<DecodedKey<C::Key>, ReadError<C::Error>> {
        if offset >= self.data_end {
            return Err(ReadError::OffsetOutOfBounds {
                offset,
                max: self.data_end,
            });
        }

        let data = &self.data[offset..self.data_end];

        // Block 层统一解码 entry 结构
        self.decode_entry_structure(data, prev_key, offset)
    }

    /// Block 层统一解码 entry 结构
    ///
    /// 格式：[shared_len][unshared_len][value_len][unshared_key_data][value_data]
    fn decode_entry_structure(
        &self,
        data: &[u8],
        prev_key: Option<&C::Key>,
        base_offset: usize,
    ) -> Result<DecodedKey<C::Key>, ReadError<C::Error>> {
        let mut pos = 0;

        // 解码三个长度字段
        let (shared_len, len) = varint::decode::<u64>(&data[pos..])
            .map_err(|e| ReadError::VarintError(format!("shared_len: {}", e)))?;
        pos += len;

        let (unshared_len, len) = varint::decode::<u64>(&data[pos..])
            .map_err(|e| ReadError::VarintError(format!("unshared_len: {}", e)))?;
        pos += len;

        let (value_len, len) = varint::decode::<u64>(&data[pos..])
            .map_err(|e| ReadError::VarintError(format!("value_len: {}", e)))?;
        pos += len;

        let shared_len = shared_len as usize;
        let unshared_len = unshared_len as usize;
        let value_len = value_len as usize;

        // 检查数据是否足够
        let required = unshared_len + value_len;
        if pos + required > data.len() {
            return Err(ReadError::TruncatedData {
                expected: pos + required,
                actual: data.len(),
            });
        }

        // 重建完整的 key
        let key = if shared_len > 0 {
            // 需要前缀解压缩
            let prev = prev_key.ok_or_else(|| ReadError::InvalidEntry {
                message: "Missing previous key for prefix decompression".to_string(),
            })?;

            // 让 Codec 自己处理前缀重建
            let unshared_data = &data[pos..pos + unshared_len];
            self.codec
                .decode_key_with_prefix(prev, unshared_data, shared_len)
                .map_err(ReadError::CodecError)?
        } else {
            // 不需要前缀压缩，直接解码
            let (key, _) = self
                .codec
                .decode_key(&data[pos..pos + unshared_len])
                .map_err(ReadError::CodecError)?;
            key
        };

        pos += unshared_len;

        // value 的位置信息（绝对位置）
        let value_offset = base_offset + pos;
        let consumed_bytes = pos + value_len;

        Ok(DecodedKey::new(
            key,
            value_offset,
            value_len,
            consumed_bytes,
        ))
    }

    /// 从指定位置解码 value
    fn decode_value_at(&self, offset: usize, len: usize) -> Result<C::Value, ReadError<C::Error>> {
        if offset + len > self.data.len() {
            return Err(ReadError::TruncatedData {
                expected: offset + len,
                actual: self.data.len(),
            });
        }

        let value_data = &self.data[offset..];
        let (value, _) = self
            .codec
            .decode_value(value_data, len)
            .map_err(ReadError::CodecError)?;
        Ok(value)
    }
}

/// Block 统计信息
#[derive(Debug, Clone)]
pub struct BlockStats {
    /// 估算的 entry 数量
    pub estimated_entry_count: usize,

    /// 数据部分大小
    pub data_size: usize,

    /// 重启点数量
    pub restart_point_count: usize,

    /// 重启点元数据大小
    pub restart_metadata_size: usize,

    /// 总大小
    pub total_size: usize,

    /// 平均 entry 大小（估算）
    pub avg_entry_size: usize,

    /// 压缩率估算（0.0-1.0，越小越好）
    pub estimated_compression_ratio: f64,
}

/// 通用 Block 迭代器
///
/// 职责：
/// - 顺序遍历 Block 中的所有 entries
/// - 支持 seek 操作（前向定位）
/// - 维护当前位置状态
/// - 提供高效的 key/value 访问
///
/// # 生命周期
/// - `'a`: 对 BlockReader 的引用生命周期
pub struct BlockIterator<'a, C: BlockCodec> {
    /// 关联的读取器
    reader: &'a BlockReader<C>,

    /// 当前位置（在 data 中的偏移）
    current_pos: usize,

    /// 当前解码的 entry 信息
    current: Option<DecodedKey<C::Key>>,

    /// 前一个 key（用于解码）
    prev_key: Option<C::Key>,

    /// 迭代器是否有效
    valid: bool,
}

impl<'a, C: BlockCodec> BlockIterator<'a, C> {
    /// 创建新的迭代器（内部使用，从 BlockReader::iter 调用）
    fn new(reader: &'a BlockReader<C>) -> Self {
        Self {
            reader,
            current_pos: 0,
            current: None,
            prev_key: None,
            valid: false,
        }
    }

    /// 定位到第一个 entry
    pub fn seek_to_first(&mut self) -> Result<(), ReadError<C::Error>> {
        if !self.reader.restart_points.is_empty() {
            self.current_pos = self.reader.restart_points[0] as usize;
            self.prev_key = None;
            self.decode_current()?;
        } else {
            self.invalidate();
        }
        Ok(())
    }

    /// 定位到指定 key 或其后继
    ///
    /// # 行为
    /// - 如果找到精确匹配，定位到该 entry
    /// - 如果未找到，定位到第一个 > target 的 entry
    /// - 如果所有 key 都 < target，迭代器变为无效状态
    ///
    /// # 参数
    /// - `target`: 目标 key
    pub fn seek(&mut self, target: &C::Key) -> Result<(), ReadError<C::Error>> {
        // 二分查找合适的重启点
        let restart_idx = self.reader.binary_search_restart_point(target)?;

        // 从重启点开始
        self.current_pos = self.reader.restart_points[restart_idx] as usize;
        self.prev_key = None;
        self.decode_current()?;

        // 线性扫描到目标位置
        while self.valid {
            if let Some(current) = &self.current {
                if current.key < *target {
                    // 继续向前
                    self.next()?;
                } else {
                    // 找到了 (>= target)
                    return Ok(());
                }
            } else {
                break;
            }
        }

        Ok(())
    }

    /// 移动到下一个 entry
    ///
    /// # 错误
    /// - 如果迭代器已无效，操作无效果
    pub fn next(&mut self) -> Result<(), ReadError<C::Error>> {
        if !self.valid {
            return Ok(());
        }

        // 移动到下一个 entry
        if let Some(current) = &self.current {
            self.current_pos += current.consumed_bytes;
            self.prev_key = Some(current.key.clone());
        }

        self.decode_current()?;
        Ok(())
    }

    /// 获取当前 key（零拷贝访问）
    ///
    /// # 返回
    /// - `Some(&Key)`: 当前有效
    /// - `None`: 迭代器无效
    pub fn key(&self) -> Option<&C::Key> {
        self.current.as_ref().map(|entry| &entry.key)
    }

    /// 获取当前 value（按需解码）
    ///
    /// # 返回
    /// - `Some(Value)`: 当前有效
    /// - `None`: 迭代器无效
    pub fn value(&self) -> Result<Option<C::Value>, ReadError<C::Error>> {
        match &self.current {
            Some(decoded) => {
                let value = self
                    .reader
                    .decode_value_at(decoded.value_offset, decoded.value_len)?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    /// 检查迭代器是否有效
    pub fn valid(&self) -> bool {
        self.valid
    }

    /// 获取当前位置信息（调试用）
    pub fn position_info(&self) -> IteratorPosition {
        let estimated_progress = if self.reader.data_end > 0 {
            self.current_pos as f64 / self.reader.data_end as f64
        } else {
            0.0
        };

        let at_restart_point = self
            .reader
            .restart_points
            .contains(&(self.current_pos as u32));

        IteratorPosition {
            current_offset: self.current_pos,
            valid: self.valid,
            at_restart_point,
            estimated_progress,
        }
    }

    /// 判断当前是否在重启点上
    pub fn at_restart_point(&self) -> bool {
        self.reader
            .restart_points
            .contains(&(self.current_pos as u32))
    }

    // ==================== 私有辅助方法 ====================

    /// 解码当前位置的 entry
    fn decode_current(&mut self) -> Result<(), ReadError<C::Error>> {
        if self.current_pos >= self.reader.data_end {
            self.invalidate();
            return Ok(());
        }

        let decoded = self
            .reader
            .decode_entry_at(self.current_pos, self.prev_key.as_ref())?;

        self.current = Some(decoded);
        self.valid = true;

        Ok(())
    }

    /// 使迭代器失效
    fn invalidate(&mut self) {
        self.valid = false;
        self.current = None;
    }
}

/// 迭代器位置信息（调试用）
#[derive(Debug, Clone)]
pub struct IteratorPosition {
    /// 当前在 data 中的偏移
    pub current_offset: usize,

    /// 是否有效
    pub valid: bool,

    /// 是否在重启点上
    pub at_restart_point: bool,

    /// 估算的遍历进度（0.0 - 1.0）
    pub estimated_progress: f64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sstable::block::BlockBuilder;
    use crate::sstable::data_block::{DataBlockCodec, InternalKey, InternalValue};
    use boxkv_common::types::ValueType;

    fn test_key(user_key: &str, seq: u64) -> InternalKey {
        InternalKey::new(Bytes::copy_from_slice(user_key.as_bytes()), seq)
    }

    fn test_value(data: &[u8]) -> InternalValue {
        ValueType::Normal(Bytes::copy_from_slice(data))
    }

    fn build_test_block(
        entries: Vec<(InternalKey, InternalValue)>,
        restart_interval: usize,
    ) -> Bytes {
        let codec = DataBlockCodec::new();
        let mut builder = BlockBuilder::new(codec, restart_interval);

        for (key, value) in entries {
            builder.add(&key, &value).unwrap();
        }

        builder.finish().unwrap()
    }

    // ==================== Reader 基础功能测试 ====================

    #[test]
    fn test_reader_new() {
        let entries = vec![
            (test_key("key1", 100), test_value(b"value1")),
            (test_key("key2", 200), test_value(b"value2")),
        ];
        let block_data = build_test_block(entries, 16);

        let codec = DataBlockCodec::new();
        let reader = BlockReader::new(codec, block_data).unwrap();

        assert_eq!(reader.restart_point_count(), 1);
        assert!(reader.data_size() > 0);
        assert!(reader.total_size() > reader.data_size());
    }

    #[test]
    fn test_reader_get_existing_key() {
        let entries = vec![
            (test_key("key1", 100), test_value(b"value1")),
            (test_key("key2", 200), test_value(b"value2")),
            (test_key("key3", 300), test_value(b"value3")),
        ];
        let block_data = build_test_block(entries, 16);

        let codec = DataBlockCodec::new();
        let reader = BlockReader::new(codec, block_data).unwrap();

        let value = reader.get(&test_key("key2", 200)).unwrap().unwrap();
        assert_eq!(value, test_value(b"value2"));
    }

    #[test]
    fn test_reader_get_non_existing_key() {
        let entries = vec![
            (test_key("key1", 100), test_value(b"value1")),
            (test_key("key3", 300), test_value(b"value3")),
        ];
        let block_data = build_test_block(entries, 16);

        let codec = DataBlockCodec::new();
        let reader = BlockReader::new(codec, block_data).unwrap();

        let value = reader.get(&test_key("key2", 200)).unwrap();
        assert!(value.is_none());
    }

    #[test]
    fn test_reader_get_first_key() {
        let entries = vec![
            (test_key("aaa", 100), test_value(b"first")),
            (test_key("bbb", 200), test_value(b"second")),
        ];
        let block_data = build_test_block(entries, 16);

        let codec = DataBlockCodec::new();
        let reader = BlockReader::new(codec, block_data).unwrap();

        let value = reader.get(&test_key("aaa", 100)).unwrap().unwrap();
        assert_eq!(value, test_value(b"first"));
    }

    #[test]
    fn test_reader_get_last_key() {
        let entries = vec![
            (test_key("key1", 100), test_value(b"value1")),
            (test_key("key2", 200), test_value(b"value2")),
            (test_key("key3", 300), test_value(b"value3")),
        ];
        let block_data = build_test_block(entries, 16);

        let codec = DataBlockCodec::new();
        let reader = BlockReader::new(codec, block_data).unwrap();

        let value = reader.get(&test_key("key3", 300)).unwrap().unwrap();
        assert_eq!(value, test_value(b"value3"));
    }

    // ==================== Iterator 基础功能测试 ====================

    #[test]
    fn test_iterator_seek_to_first() {
        let entries = vec![
            (test_key("key1", 100), test_value(b"value1")),
            (test_key("key2", 200), test_value(b"value2")),
        ];
        let block_data = build_test_block(entries, 16);

        let codec = DataBlockCodec::new();
        let reader = BlockReader::new(codec, block_data).unwrap();
        let mut iter = reader.iter();

        iter.seek_to_first().unwrap();

        assert!(iter.valid());
        assert_eq!(iter.key().unwrap(), &test_key("key1", 100));
    }

    #[test]
    fn test_iterator_next() {
        let entries = vec![
            (test_key("key1", 100), test_value(b"value1")),
            (test_key("key2", 200), test_value(b"value2")),
            (test_key("key3", 300), test_value(b"value3")),
        ];
        let block_data = build_test_block(entries, 16);

        let codec = DataBlockCodec::new();
        let reader = BlockReader::new(codec, block_data).unwrap();
        let mut iter = reader.iter();

        iter.seek_to_first().unwrap();
        assert_eq!(iter.key().unwrap(), &test_key("key1", 100));

        iter.next().unwrap();
        assert_eq!(iter.key().unwrap(), &test_key("key2", 200));

        iter.next().unwrap();
        assert_eq!(iter.key().unwrap(), &test_key("key3", 300));

        iter.next().unwrap();
        assert!(!iter.valid());
    }

    #[test]
    fn test_iterator_value() {
        let entries = vec![(test_key("key1", 100), test_value(b"value1"))];
        let block_data = build_test_block(entries, 16);

        let codec = DataBlockCodec::new();
        let reader = BlockReader::new(codec, block_data).unwrap();
        let mut iter = reader.iter();

        iter.seek_to_first().unwrap();

        let value = iter.value().unwrap().unwrap();
        assert_eq!(value, test_value(b"value1"));
    }

    #[test]
    fn test_iterator_seek_exact_match() {
        let entries = vec![
            (test_key("key1", 100), test_value(b"value1")),
            (test_key("key2", 200), test_value(b"value2")),
            (test_key("key3", 300), test_value(b"value3")),
        ];
        let block_data = build_test_block(entries, 16);

        let codec = DataBlockCodec::new();
        let reader = BlockReader::new(codec, block_data).unwrap();
        let mut iter = reader.iter();

        iter.seek(&test_key("key2", 200)).unwrap();

        assert!(iter.valid());
        assert_eq!(iter.key().unwrap(), &test_key("key2", 200));
    }

    #[test]
    fn test_iterator_seek_to_successor() {
        let entries = vec![
            (test_key("key1", 100), test_value(b"value1")),
            (test_key("key3", 300), test_value(b"value3")),
        ];
        let block_data = build_test_block(entries, 16);

        let codec = DataBlockCodec::new();
        let reader = BlockReader::new(codec, block_data).unwrap();
        let mut iter = reader.iter();

        // seek 到不存在的 key2，应该定位到 key3
        iter.seek(&test_key("key2", 200)).unwrap();

        assert!(iter.valid());
        assert_eq!(iter.key().unwrap(), &test_key("key3", 300));
    }

    #[test]
    fn test_iterator_seek_beyond_last() {
        let entries = vec![
            (test_key("key1", 100), test_value(b"value1")),
            (test_key("key2", 200), test_value(b"value2")),
        ];
        let block_data = build_test_block(entries, 16);

        let codec = DataBlockCodec::new();
        let reader = BlockReader::new(codec, block_data).unwrap();
        let mut iter = reader.iter();

        // seek 到超过最后一个 key 的值
        iter.seek(&test_key("key9", 900)).unwrap();

        assert!(!iter.valid());
    }

    #[test]
    fn test_iterator_full_scan() {
        let entries = vec![
            (test_key("key1", 100), test_value(b"value1")),
            (test_key("key2", 200), test_value(b"value2")),
            (test_key("key3", 300), test_value(b"value3")),
            (test_key("key4", 400), test_value(b"value4")),
        ];
        let block_data = build_test_block(entries.clone(), 16);

        let codec = DataBlockCodec::new();
        let reader = BlockReader::new(codec, block_data).unwrap();
        let mut iter = reader.iter();

        iter.seek_to_first().unwrap();

        let mut scanned = Vec::new();
        while iter.valid() {
            let key = iter.key().unwrap().clone();
            let value = iter.value().unwrap().unwrap();
            scanned.push((key, value));
            iter.next().unwrap();
        }

        assert_eq!(scanned.len(), entries.len());
        for (i, (key, value)) in scanned.iter().enumerate() {
            assert_eq!(key, &entries[i].0);
            assert_eq!(value, &entries[i].1);
        }
    }

    // ==================== 重启点测试 ====================

    #[test]
    fn test_multiple_restart_points() {
        let mut entries = Vec::new();
        for i in 0..10 {
            entries.push((test_key(&format!("key{:02}", i), i), test_value(b"value")));
        }
        let block_data = build_test_block(entries, 3);

        let codec = DataBlockCodec::new();
        let reader = BlockReader::new(codec, block_data).unwrap();

        // restart_interval=3: 重启点在 0, 3, 6, 9
        assert_eq!(reader.restart_point_count(), 4);
    }

    #[test]
    fn test_seek_with_multiple_restart_points() {
        let mut entries = Vec::new();
        for i in 0..20 {
            entries.push((
                test_key(&format!("key{:03}", i), i),
                test_value(format!("value{}", i).as_bytes()),
            ));
        }
        let block_data = build_test_block(entries, 5);

        let codec = DataBlockCodec::new();
        let reader = BlockReader::new(codec, block_data).unwrap();
        let mut iter = reader.iter();

        // seek 到中间的 key
        iter.seek(&test_key("key010", 10)).unwrap();
        assert!(iter.valid());
        assert_eq!(iter.key().unwrap(), &test_key("key010", 10));
    }

    #[test]
    fn test_at_restart_point() {
        let entries = vec![
            (test_key("key1", 100), test_value(b"value1")),
            (test_key("key2", 200), test_value(b"value2")),
            (test_key("key3", 300), test_value(b"value3")),
            (test_key("key4", 400), test_value(b"value4")),
        ];
        let block_data = build_test_block(entries, 2);

        let codec = DataBlockCodec::new();
        let reader = BlockReader::new(codec, block_data).unwrap();
        let mut iter = reader.iter();

        iter.seek_to_first().unwrap();
        assert!(iter.at_restart_point()); // 第一个是重启点

        iter.next().unwrap();
        assert!(!iter.at_restart_point()); // 第二个不是

        iter.next().unwrap();
        assert!(iter.at_restart_point()); // 第三个是重启点 (interval=2)
    }

    // ==================== 统计信息测试 ====================

    #[test]
    fn test_reader_stats() {
        let mut entries = Vec::new();
        for i in 0..100 {
            entries.push((test_key(&format!("key{:03}", i), i), test_value(b"value")));
        }
        let block_data = build_test_block(entries, 16);

        let codec = DataBlockCodec::new();
        let reader = BlockReader::new(codec, block_data).unwrap();
        let stats = reader.stats();

        assert!(stats.data_size > 0);
        assert!(stats.total_size > stats.data_size);
        assert!(stats.restart_point_count > 0);
        assert!(stats.estimated_entry_count > 0);
        assert!(stats.avg_entry_size > 0);
        assert!(stats.estimated_compression_ratio > 0.0);
    }

    #[test]
    fn test_verify_integrity() {
        let entries = vec![(test_key("key1", 100), test_value(b"value1"))];
        let block_data = build_test_block(entries, 16);

        let codec = DataBlockCodec::new();
        let reader = BlockReader::new(codec, block_data).unwrap();

        assert!(reader.verify_integrity().is_ok());
    }

    // ==================== 错误情况测试 ====================

    #[test]
    fn test_reader_new_with_truncated_data() {
        let data = Bytes::from(vec![0x01, 0x02]); // 太短

        let codec = DataBlockCodec::new();
        let result = BlockReader::new(codec, data);

        assert!(matches!(result, Err(ReadError::TruncatedData { .. })));
    }

    #[test]
    fn test_reader_new_with_corrupted_restart_count() {
        let mut data = vec![0u8; 20];
        // 最后 4 字节设置为 0（无效的 restart_count）
        data[16..20].copy_from_slice(&0u32.to_be_bytes());

        let codec = DataBlockCodec::new();
        let result = BlockReader::new(codec, Bytes::from(data));

        assert!(matches!(result, Err(ReadError::InvalidRestartPoints)));
    }

    #[test]
    fn test_iterator_value_when_invalid() {
        let entries = vec![(test_key("key1", 100), test_value(b"value1"))];
        let block_data = build_test_block(entries, 16);

        let codec = DataBlockCodec::new();
        let reader = BlockReader::new(codec, block_data).unwrap();
        let iter = reader.iter(); // 未 seek，invalid

        assert!(!iter.valid());
        let value = iter.value().unwrap();
        assert!(value.is_none());
    }

    // ==================== 边界条件测试 ====================

    #[test]
    fn test_single_entry_block() {
        let entries = vec![(test_key("only_key", 100), test_value(b"only_value"))];
        let block_data = build_test_block(entries, 16);

        let codec = DataBlockCodec::new();
        let reader = BlockReader::new(codec, block_data).unwrap();

        assert_eq!(reader.restart_point_count(), 1);

        let value = reader.get(&test_key("only_key", 100)).unwrap().unwrap();
        assert_eq!(value, test_value(b"only_value"));
    }

    #[test]
    fn test_empty_user_key() {
        let entries = vec![(test_key("", 100), test_value(b"value"))];
        let block_data = build_test_block(entries, 16);

        let codec = DataBlockCodec::new();
        let reader = BlockReader::new(codec, block_data).unwrap();

        let value = reader.get(&test_key("", 100)).unwrap().unwrap();
        assert_eq!(value, test_value(b"value"));
    }

    #[test]
    fn test_tombstone_entries() {
        let entries = vec![
            (test_key("deleted1", 100), ValueType::Tombstone),
            (test_key("deleted2", 200), ValueType::Tombstone),
        ];
        let block_data = build_test_block(entries, 16);

        let codec = DataBlockCodec::new();
        let reader = BlockReader::new(codec, block_data).unwrap();

        let value = reader.get(&test_key("deleted1", 100)).unwrap().unwrap();
        assert_eq!(value, ValueType::Tombstone);
    }

    #[test]
    fn test_same_user_key_different_sequences() {
        let entries = vec![
            (test_key("key", 100), test_value(b"newest")),
            (test_key("key", 99), test_value(b"older")),
            (test_key("key", 98), test_value(b"oldest")),
        ];
        let block_data = build_test_block(entries, 16);

        let codec = DataBlockCodec::new();
        let reader = BlockReader::new(codec, block_data).unwrap();

        // 查询不同版本
        let v1 = reader.get(&test_key("key", 100)).unwrap().unwrap();
        assert_eq!(v1, test_value(b"newest"));

        let v2 = reader.get(&test_key("key", 99)).unwrap().unwrap();
        assert_eq!(v2, test_value(b"older"));
    }

    #[test]
    fn test_large_block() {
        let mut entries = Vec::new();
        for i in 0..1000 {
            entries.push((
                test_key(&format!("key{:05}", i), i),
                test_value(format!("value{}", i).as_bytes()),
            ));
        }
        let block_data = build_test_block(entries, 16);

        let codec = DataBlockCodec::new();
        let reader = BlockReader::new(codec, block_data).unwrap();

        // 测试随机访问
        let value = reader.get(&test_key("key00500", 500)).unwrap().unwrap();
        assert_eq!(value, test_value(b"value500"));
    }

    // ==================== Iterator 位置信息测试 ====================

    #[test]
    fn test_iterator_position_info() {
        let entries = vec![
            (test_key("key1", 100), test_value(b"value1")),
            (test_key("key2", 200), test_value(b"value2")),
        ];
        let block_data = build_test_block(entries, 16);

        let codec = DataBlockCodec::new();
        let reader = BlockReader::new(codec, block_data).unwrap();
        let mut iter = reader.iter();

        iter.seek_to_first().unwrap();

        let pos = iter.position_info();
        assert!(pos.valid);
        assert_eq!(pos.current_offset, 0);
        assert!(pos.estimated_progress >= 0.0 && pos.estimated_progress <= 1.0);
    }

    // ==================== Getter 测试 ====================

    #[test]
    fn test_reader_getters() {
        let entries = vec![(test_key("key1", 100), test_value(b"value1"))];
        let block_data = build_test_block(entries, 16);
        let data_len = block_data.len();

        let codec = DataBlockCodec::new();
        let reader = BlockReader::new(codec, block_data).unwrap();

        assert_eq!(reader.total_size(), data_len);
        assert!(reader.data_size() < reader.total_size());
        assert!(reader.restart_point_count() > 0);

        // raw_data 应该可以访问
        assert_eq!(reader.raw_data().len(), data_len);

        // codec 应该可以访问
        let _ = reader.codec();
    }
}
