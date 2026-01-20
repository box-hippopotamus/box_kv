use bytes::BufMut;

/// 编码接口：将对象序列化到连续缓冲区
pub trait Encode {
    type CodecError;
    /// 将自身写入到可追加的缓冲区
    fn encode_to(&self, buf: &mut impl BufMut) -> Result<(), Self::CodecError>;
    /// 返回编码后的字节长度，用于预分配或边界检查
    fn encoded_len(&self) -> usize;
}

/// 解码接口：从字节切片还原对象
pub trait Decode: Sized {
    type CodecError;
    /// 从缓冲区起始位置解码，返回(对象, 消耗字节数)
    fn decode_from(buf: &[u8]) -> Result<(Self, usize), Self::CodecError>;
}

/// 携带解码上下文的解码接口：适用于外部已知类型标签等场景
pub trait DecodeWithContext: Sized {
    type Context;
    type CodecError;
    /// 使用外部提供的上下文进行解码，返回(对象, 消耗字节数)
    fn decode_with(buf: &[u8], ctx: Self::Context) -> Result<(Self, usize), Self::CodecError>;
}
