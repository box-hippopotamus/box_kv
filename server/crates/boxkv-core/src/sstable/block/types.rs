use bytes::BufMut;
use std::error::Error;

/// Block 编解码协议
pub trait BlockCodec {
    /// Key类型
    type Key: Clone + Ord;

    /// Value类型
    type Value: Clone;

    /// 编解码错误类型
    type Error: Error + Send + Sync + 'static;

    /// 将 Key 编码为字节序列     
    fn encode_key(
        &self,
        key: &Self::Key,
        buf: &mut impl BufMut,
        shared_prefix_len: usize,
    ) -> Result<(), Self::Error>;

    /// 从字节序列解码 Key
    fn decode_key(&self, data: &[u8]) -> Result<(Self::Key, usize), Self::Error>;

    /// 从前缀压缩的数据重建 Key
    fn decode_key_with_prefix(
        &self,
        prev_key: &Self::Key,
        unshared_data: &[u8],
        shared_len: usize,
    ) -> Result<Self::Key, Self::Error>;

    /// 将 Value 编码为字节序列
    fn encode_value(&self, value: &Self::Value, buf: &mut impl BufMut) -> Result<(), Self::Error>;

    /// 从字节序列解码 Value
    fn decode_value(
        &self,
        data: &[u8],
        value_len: usize,
    ) -> Result<(Self::Value, usize), Self::Error>;

    /// 计算编码后的 Key 大小
    fn encoded_key_len(&self, key: &Self::Key) -> usize;

    /// 计算编码后的 Value 大小
    fn encoded_value_len(&self, value: &Self::Value) -> usize;

    /// 计算两个 Key 编码后的共享前缀长度
    fn shared_prefix_len(&self, a: &Self::Key, b: &Self::Key) -> usize;
}

/// 解码后的 Entry（延迟解码 value）
#[derive(Debug, Clone)]
pub struct DecodedKey<K> {
    /// 解码后的 key
    pub key: K,

    /// value 在原始数据中的偏移（绝对位置）
    pub value_offset: usize,

    /// value 的长度
    pub value_len: usize,

    /// 本次解码消耗的字节数（整个entry的大小）
    pub consumed_bytes: usize,
}

impl<K> DecodedKey<K> {
    /// 创建新的 key 解码结果
    pub fn new(key: K, value_offset: usize, value_len: usize, consumed_bytes: usize) -> Self {
        Self {
            key,
            value_offset,
            value_len,
            consumed_bytes,
        }
    }
}
