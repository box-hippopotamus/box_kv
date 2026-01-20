use crate::sstable::block::types::BlockCodec;
use crate::sstable::block::{
    RESTART_POINT_LEN, max_key_len, max_value_len,
};
use boxkv_common::varint;
use bytes::{BufMut, Bytes, BytesMut};
use thiserror::Error;

/// Block 构建错误
#[derive(Debug, Error)]
pub enum BuildError<E: std::error::Error> {
    /// Block 已经完成构建
    #[error("Block already finished")]
    AlreadyFinished,

    /// Block 为空（finish 时）
    #[error("Cannot finish empty block")]
    EmptyBlock,

    /// Key 顺序错误（新 key <= 旧 key）
    #[error("Key order violation: {message}")]
    KeyOrderViolation { message: String },

    /// Block 大小超过限制
    #[error("Block size exceeded: {current} > {max}")]
    BlockSizeExceeded { current: usize, max: usize },

    /// 编解码器错误
    #[error("Codec error: {0}")]
    CodecError(#[source] E),

    /// Varint 编解码错误
    #[error("Varint error: {0}")]
    VarintError(String),
}

/// 通用 Block 构建器
///
/// # 物理格式
/// ```text
/// [Entry₁][Entry₂]...[Entryₙ]
/// [RestartPoint₁: u32][RestartPoint₂: u32]...[RestartPointₘ: u32]
/// [RestartPointCount: u32]
/// ```
///
/// # Entry 格式
/// ```text
/// [shared_key_len: varint][unshared_key_len: varint][value_len: varint]
/// [unshared_key_data][value_data]
/// ```
pub struct BlockBuilder<C: BlockCodec> {
    /// 编解码器实例
    codec: C,

    /// 重启点间隔
    restart_interval: usize,

    /// 数据缓冲区
    buf: BytesMut,

    /// 重启点列表（存储偏移量）
    restart_points: Vec<u32>,

    /// 当前 entry 数量
    entry_count: usize,

    /// 上一个添加的 key（用于前缀压缩）
    last_key: Option<C::Key>,

    /// 自上一个重启点以来的 entry 数量
    entries_since_restart: usize,

    /// 是否已完成构建
    finished: bool,
}

impl<C: BlockCodec> BlockBuilder<C> {
    /// 创建新的构建器
    pub fn new(codec: C, restart_interval: usize) -> Self {
        Self {
            codec,
            restart_interval,
            buf: BytesMut::new(),
            restart_points: Vec::new(),
            entry_count: 0,
            last_key: None,
            entries_since_restart: 0,
            finished: false,
        }
    }

    /// 添加一个 KV 对
    ///
    /// # 参数
    /// - `key`: 要添加的 key（必须 > last_key）
    /// - `value`: 要添加的 value
    ///
    /// # 错误
    /// - `BuildError::AlreadyFinished`: 如果已调用 finish()
    /// - `BuildError::KeyOrderViolation`: 如果 key 顺序错误
    /// - `BuildError::CodecError`: 如果编码失败
    pub fn add(&mut self, key: &C::Key, value: &C::Value) -> Result<(), BuildError<C::Error>> {
        if self.finished {
            return Err(BuildError::AlreadyFinished);
        }

        // 验证 key 顺序
        self.validate_key_order(key)?;

        // 当前 entry 在 buf 中的起始位置
        let entry_offset = self.buf.len() as u32;

        // 判断是否需要添加重启点
        let is_restart = self.should_add_restart_point();

        if is_restart {
            // 重启点：记录 offset
            self.restart_points.push(entry_offset);
            self.entries_since_restart = 0;
        }

        // Block 层统一编码 entry 结构
        self.encode_entry(key, value, is_restart)?;

        // 更新状态
        self.entry_count += 1;
        self.entries_since_restart += 1;
        self.last_key = Some(key.clone());

        Ok(())
    }

    /// 完成 Block 构建，返回编码后的数据
    ///
    /// # Block 最终格式
    /// ```text
    /// [Data Part: entries]
    /// [RestartPoint₁: u32][RestartPoint₂: u32]...[RestartPointₙ: u32]
    /// [RestartPointCount: u32]
    /// ```
    ///
    /// # 错误
    /// - `BuildError::EmptyBlock`: 如果 block 为空
    /// - `BuildError::AlreadyFinished`: 如果已调用过 finish()
    pub fn finish(&mut self) -> Result<Bytes, BuildError<C::Error>> {
        if self.finished {
            return Err(BuildError::AlreadyFinished);
        }

        if self.is_empty() {
            return Err(BuildError::EmptyBlock);
        }

        // 写入重启点数据
        self.write_restart_points();

        self.finished = true;
        let buf = std::mem::take(&mut self.buf);
        Ok(buf.freeze())
    }

    /// 重置构建器状态（复用构建器）
    pub fn reset(&mut self) {
        self.buf.clear();
        self.restart_points.clear();
        self.entry_count = 0;
        self.last_key = None;
        self.entries_since_restart = 0;
        self.finished = false;
    }

    /// 检查构建器是否为空
    pub fn is_empty(&self) -> bool {
        self.entry_count == 0
    }

    /// 检查是否已完成构建
    pub fn is_finished(&self) -> bool {
        self.finished
    }

    /// 获取当前 entry 数量
    pub fn entry_count(&self) -> usize {
        self.entry_count
    }

    /// 获取重启点数量
    pub fn restart_point_count(&self) -> usize {
        self.restart_points.len()
    }

    /// 获取当前估算的大小（包括未来的重启点数据）
    ///
    /// 用于判断是否应该结束当前 block
    pub fn estimated_size(&self) -> usize {
        self.buf.len()
            + self.restart_points.len() * RESTART_POINT_LEN  // 已有重启点
            + RESTART_POINT_LEN // restart count
    }

    /// 估算添加指定 KV 后的大小
    pub fn estimated_size_after_add(&self, key: &C::Key, value: &C::Value) -> usize {
        let current = self.estimated_size();
        let entry_size = self.estimate_entry_size(key, value);

        // 如果会创建新的重启点，额外计算重启点开销
        let restart_overhead = if self.should_add_restart_point() {
            RESTART_POINT_LEN
        } else {
            0
        };

        current + entry_size + restart_overhead
    }

    /// 获取重启点间隔
    pub fn restart_interval(&self) -> usize {
        self.restart_interval
    }

    /// 获取当前使用的编解码器
    pub fn codec(&self) -> &C {
        &self.codec
    }

    /// 获取当前缓冲区大小（不包括重启点）
    pub fn current_data_size(&self) -> usize {
        self.buf.len()
    }

    // ==================== 私有辅助方法 ====================

    /// 检查是否需要添加重启点
    fn should_add_restart_point(&self) -> bool {
        self.entry_count == 0 || self.entries_since_restart >= self.restart_interval
    }

    /// 写入重启点数据到缓冲区
    fn write_restart_points(&mut self) {
        // 写入所有重启点偏移
        for &point in &self.restart_points {
            self.buf.put_u32(point);
        }

        // 写入重启点数量
        self.buf.put_u32(self.restart_points.len() as u32);
    }

    /// 验证 key 的顺序
    fn validate_key_order(&self, key: &C::Key) -> Result<(), BuildError<C::Error>> {
        if let Some(last) = &self.last_key
            && last >= key
        {
            return Err(BuildError::KeyOrderViolation {
                message: if last == key {
                    "Duplicate key".to_string()
                } else {
                    "Key out of order".to_string()
                },
            });
        }
        Ok(())
    }

    /// Block 层统一编码 entry 结构
    ///
    /// 格式：[shared_len][unshared_len][value_len][unshared_key_data][value_data]
    fn encode_entry(
        &mut self,
        key: &C::Key,
        value: &C::Value,
        is_restart: bool,
    ) -> Result<(), BuildError<C::Error>> {
        // 计算共享前缀长度
        let shared_len = if !is_restart {
            if let Some(last) = &self.last_key {
                self.codec.shared_prefix_len(last, key)
            } else {
                0
            }
        } else {
            0
        };

        // 获取编码后的长度
        let key_len = self.codec.encoded_key_len(key);
        let value_len = self.codec.encoded_value_len(value);

        // 验证长度
        if key_len > max_key_len() {
            return Err(BuildError::BlockSizeExceeded {
                current: key_len,
                max: max_key_len(),
            });
        }

        if value_len > max_value_len() {
            return Err(BuildError::BlockSizeExceeded {
                current: value_len,
                max: max_value_len(),
            });
        }

        let unshared_len = key_len - shared_len;

        // Block 层统一写入三个长度字段
        varint::encode(shared_len as u64, &mut self.buf);
        varint::encode(unshared_len as u64, &mut self.buf);
        varint::encode(value_len as u64, &mut self.buf);

        // 让 Codec 直接写入未共享的 key 部分（零拷贝）
        self.codec
            .encode_key(key, &mut self.buf, shared_len)
            .map_err(BuildError::CodecError)?;

        // 让 Codec 直接写入 value（零拷贝）
        self.codec
            .encode_value(value, &mut self.buf)
            .map_err(BuildError::CodecError)?;

        Ok(())
    }

    /// 估算 entry 编码后的大小
    fn estimate_entry_size(&self, key: &C::Key, value: &C::Value) -> usize {
        let shared_len = if !self.should_add_restart_point()
            && let Some(last) = &self.last_key
        {
            self.codec.shared_prefix_len(last, key)
        } else {
            0
        };

        let key_len = self.codec.encoded_key_len(key);
        let unshared_len = key_len - shared_len;
        let value_len = self.codec.encoded_value_len(value);

        // varint 长度 + 数据长度
        varint::encoded_len(shared_len as u64)
            + varint::encoded_len(unshared_len as u64)
            + varint::encoded_len(value_len as u64)
            + unshared_len
            + value_len
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sstable::data_block::{DataBlockCodec, InternalKey, InternalValue};
    use boxkv_common::types::ValueType;

    fn test_key(user_key: &str, seq: u64) -> InternalKey {
        InternalKey::new(Bytes::copy_from_slice(user_key.as_bytes()), seq)
    }

    fn test_value(data: &[u8]) -> InternalValue {
        ValueType::Normal(Bytes::copy_from_slice(data))
    }

    // ==================== Builder 基础功能测试 ====================

    #[test]
    fn test_builder_new() {
        let codec = DataBlockCodec::new();
        let builder: BlockBuilder<DataBlockCodec> = BlockBuilder::new(codec, 16);

        assert!(builder.is_empty());
        assert_eq!(builder.entry_count(), 0);
        assert_eq!(builder.restart_point_count(), 0);
        assert_eq!(builder.restart_interval(), 16);
    }

    #[test]
    fn test_builder_add_single_entry() {
        let codec = DataBlockCodec::new();
        let mut builder = BlockBuilder::new(codec, 16);

        let key = test_key("key1", 100);
        let value = test_value(b"value1");

        builder.add(&key, &value).unwrap();

        assert!(!builder.is_empty());
        assert_eq!(builder.entry_count(), 1);
        assert_eq!(builder.restart_point_count(), 1); // 第一个 entry 是重启点
    }

    #[test]
    fn test_builder_add_multiple_entries() {
        let codec = DataBlockCodec::new();
        let mut builder = BlockBuilder::new(codec, 4);

        for i in 0..10 {
            let key = test_key(&format!("key{:03}", i), i);
            let value = test_value(format!("value{}", i).as_bytes());
            builder.add(&key, &value).unwrap();
        }

        assert_eq!(builder.entry_count(), 10);
        // restart_interval=4: entries 0,4,8 是重启点
        assert_eq!(builder.restart_point_count(), 3);
    }

    #[test]
    fn test_builder_finish() {
        let codec = DataBlockCodec::new();
        let mut builder = BlockBuilder::new(codec, 16);

        builder
            .add(&test_key("key1", 100), &test_value(b"value1"))
            .unwrap();
        builder
            .add(&test_key("key2", 200), &test_value(b"value2"))
            .unwrap();

        let block_data = builder.finish().unwrap();

        assert!(!block_data.is_empty());
        assert!(builder.is_finished());
    }

    #[test]
    fn test_builder_reset() {
        let codec = DataBlockCodec::new();
        let mut builder = BlockBuilder::new(codec, 16);

        builder
            .add(&test_key("key1", 100), &test_value(b"value1"))
            .unwrap();
        builder.finish().unwrap();

        builder.reset();

        assert!(builder.is_empty());
        assert!(!builder.is_finished());
        assert_eq!(builder.entry_count(), 0);
        assert_eq!(builder.restart_point_count(), 0);
    }

    // ==================== 错误情况测试 ====================

    #[test]
    fn test_builder_finish_empty_block() {
        let codec = DataBlockCodec::new();
        let mut builder: BlockBuilder<DataBlockCodec> = BlockBuilder::new(codec, 16);

        let result = builder.finish();
        assert!(matches!(result, Err(BuildError::EmptyBlock)));
    }

    #[test]
    fn test_builder_add_after_finish() {
        let codec = DataBlockCodec::new();
        let mut builder = BlockBuilder::new(codec, 16);

        builder
            .add(&test_key("key1", 100), &test_value(b"value1"))
            .unwrap();
        builder.finish().unwrap();

        let result = builder.add(&test_key("key2", 200), &test_value(b"value2"));
        assert!(matches!(result, Err(BuildError::AlreadyFinished)));
    }

    #[test]
    fn test_builder_finish_twice() {
        let codec = DataBlockCodec::new();
        let mut builder = BlockBuilder::new(codec, 16);

        builder
            .add(&test_key("key1", 100), &test_value(b"value1"))
            .unwrap();
        builder.finish().unwrap();

        let result = builder.finish();
        assert!(matches!(result, Err(BuildError::AlreadyFinished)));
    }

    #[test]
    fn test_builder_key_order_violation_descending() {
        let codec = DataBlockCodec::new();
        let mut builder = BlockBuilder::new(codec, 16);

        builder
            .add(&test_key("key2", 100), &test_value(b"value2"))
            .unwrap();

        let result = builder.add(&test_key("key1", 200), &test_value(b"value1"));
        assert!(matches!(result, Err(BuildError::KeyOrderViolation { .. })));
    }

    #[test]
    fn test_builder_duplicate_key() {
        let codec = DataBlockCodec::new();
        let mut builder = BlockBuilder::new(codec, 16);

        let key = test_key("duplicate", 100);
        builder.add(&key, &test_value(b"value1")).unwrap();

        let result = builder.add(&key, &test_value(b"value2"));
        assert!(matches!(result, Err(BuildError::KeyOrderViolation { .. })));
    }

    // ==================== 重启点逻辑测试 ====================

    #[test]
    fn test_restart_interval_1() {
        let codec = DataBlockCodec::new();
        let mut builder = BlockBuilder::new(codec, 1);

        for i in 0..5 {
            let key = test_key(&format!("key{}", i), i);
            builder.add(&key, &test_value(b"value")).unwrap();
        }

        // restart_interval=1，每个 entry 都是重启点
        assert_eq!(builder.restart_point_count(), 5);
    }

    #[test]
    fn test_restart_interval_large() {
        let codec = DataBlockCodec::new();
        let mut builder = BlockBuilder::new(codec, 100);

        for i in 0..10 {
            let key = test_key(&format!("key{:02}", i), i);
            builder.add(&key, &test_value(b"value")).unwrap();
        }

        // restart_interval=100，只有第一个是重启点
        assert_eq!(builder.restart_point_count(), 1);
    }

    #[test]
    fn test_restart_points_at_correct_positions() {
        let codec = DataBlockCodec::new();
        let mut builder = BlockBuilder::new(codec, 3);

        for i in 0..7 {
            let key = test_key(&format!("key{}", i), i);
            builder.add(&key, &test_value(b"val")).unwrap();
        }

        // restart_interval=3: 重启点在 entry 0, 3, 6
        assert_eq!(builder.restart_point_count(), 3);
    }

    // ==================== 估算大小测试 ====================

    #[test]
    fn test_estimated_size_empty() {
        let codec = DataBlockCodec::new();
        let builder: BlockBuilder<DataBlockCodec> = BlockBuilder::new(codec, 16);

        // 空 builder: 只有 restart_count (4 bytes)
        assert_eq!(builder.estimated_size(), 4);
    }

    #[test]
    fn test_estimated_size_increases() {
        let codec = DataBlockCodec::new();
        let mut builder = BlockBuilder::new(codec, 16);

        let size_before = builder.estimated_size();
        builder
            .add(&test_key("key", 100), &test_value(b"value"))
            .unwrap();
        let size_after = builder.estimated_size();

        assert!(size_after > size_before);
    }

    #[test]
    fn test_estimated_size_after_add() {
        let codec = DataBlockCodec::new();
        let builder = BlockBuilder::new(codec, 16);

        let key = test_key("testkey", 100);
        let value = test_value(b"testvalue");

        let estimated = builder.estimated_size_after_add(&key, &value);
        assert!(estimated > builder.estimated_size());
    }

    #[test]
    fn test_current_data_size() {
        let codec = DataBlockCodec::new();
        let mut builder = BlockBuilder::new(codec, 16);

        builder
            .add(&test_key("key1", 100), &test_value(b"value1"))
            .unwrap();

        let data_size = builder.current_data_size();
        assert!(data_size > 0);

        // current_data_size 不应该包括重启点
        assert!(data_size < builder.estimated_size());
    }

    // ==================== 前缀压缩效果测试 ====================

    #[test]
    fn test_prefix_compression_reduces_size() {
        let codec = DataBlockCodec::new();
        let mut builder_no_compression = BlockBuilder::new(codec.clone(), 1); // 每个都是重启点
        let mut builder_with_compression = BlockBuilder::new(codec, 100); // 只有第一个是重启点

        // 添加相似的 keys
        for i in 0..10 {
            let key = test_key(&format!("common_prefix_{:02}", i), i);
            let value = test_value(b"value");

            builder_no_compression.add(&key, &value).unwrap();
            builder_with_compression.add(&key, &value).unwrap();
        }

        let size_no_compression = builder_no_compression.current_data_size();
        let size_with_compression = builder_with_compression.current_data_size();

        // 有前缀压缩应该更小
        assert!(size_with_compression < size_no_compression);
    }

    // ==================== 边界条件测试 ====================

    #[test]
    fn test_empty_key() {
        let codec = DataBlockCodec::new();
        let mut builder = BlockBuilder::new(codec, 16);

        let key = test_key("", 100);
        let value = test_value(b"value");

        builder.add(&key, &value).unwrap();
        let block_data = builder.finish().unwrap();

        assert!(!block_data.is_empty());
    }

    #[test]
    fn test_empty_value() {
        let codec = DataBlockCodec::new();
        let mut builder = BlockBuilder::new(codec, 16);

        let key = test_key("key", 100);
        let value = test_value(b"");

        builder.add(&key, &value).unwrap();
        let block_data = builder.finish().unwrap();

        assert!(!block_data.is_empty());
    }

    #[test]
    fn test_tombstone_value() {
        let codec = DataBlockCodec::new();
        let mut builder = BlockBuilder::new(codec, 16);

        let key = test_key("deleted_key", 100);
        let value = ValueType::Tombstone;

        builder.add(&key, &value).unwrap();
        let block_data = builder.finish().unwrap();

        assert!(!block_data.is_empty());
    }

    #[test]
    fn test_large_entry_count() {
        let codec = DataBlockCodec::new();
        let mut builder = BlockBuilder::new(codec, 16);

        for i in 0..1000 {
            let key = test_key(&format!("key{:05}", i), i);
            let value = test_value(format!("value{}", i).as_bytes());
            builder.add(&key, &value).unwrap();
        }

        assert_eq!(builder.entry_count(), 1000);
        let block_data = builder.finish().unwrap();
        assert!(!block_data.is_empty());
    }

    // ==================== 序列号排序测试 ====================

    #[test]
    fn test_same_user_key_different_sequences() {
        let codec = DataBlockCodec::new();
        let mut builder = BlockBuilder::new(codec, 16);

        // 相同 user_key，不同 sequence（降序）
        builder
            .add(&test_key("key", 100), &test_value(b"v1"))
            .unwrap();
        builder
            .add(&test_key("key", 99), &test_value(b"v2"))
            .unwrap();
        builder
            .add(&test_key("key", 98), &test_value(b"v3"))
            .unwrap();

        assert_eq!(builder.entry_count(), 3);
        let block_data = builder.finish().unwrap();
        assert!(!block_data.is_empty());
    }

    #[test]
    fn test_same_user_key_wrong_sequence_order() {
        let codec = DataBlockCodec::new();
        let mut builder = BlockBuilder::new(codec, 16);

        builder
            .add(&test_key("key", 100), &test_value(b"v1"))
            .unwrap();

        // sequence 应该降序，这里是升序，会失败
        let result = builder.add(&test_key("key", 101), &test_value(b"v2"));
        assert!(matches!(result, Err(BuildError::KeyOrderViolation { .. })));
    }

    // ==================== 复用测试 ====================

    #[test]
    fn test_builder_reuse_after_reset() {
        let codec = DataBlockCodec::new();
        let mut builder = BlockBuilder::new(codec, 16);

        // 第一轮
        builder
            .add(&test_key("key1", 100), &test_value(b"value1"))
            .unwrap();
        let _block1 = builder.finish().unwrap();

        // 重置后第二轮
        builder.reset();
        builder
            .add(&test_key("key2", 200), &test_value(b"value2"))
            .unwrap();
        let block2 = builder.finish().unwrap();

        assert!(!block2.is_empty());
    }

    // ==================== 获取器测试 ====================

    #[test]
    fn test_builder_getters() {
        let codec = DataBlockCodec::new();
        let builder = BlockBuilder::new(codec, 16);

        assert_eq!(builder.restart_interval(), 16);
        assert!(builder.is_empty());
        assert!(!builder.is_finished());

        // codec 应该可以访问
        let _ = builder.codec();
    }
}
