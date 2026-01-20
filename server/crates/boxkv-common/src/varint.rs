//! 可变长整数（LEB128）编解码工具
//!
//! 说明：提供无符号整数的变长编码/解码与长度计算，适用于紧凑持久化格式。
use bytes::BufMut;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum VarintError {
    #[error("unexpected end of buffer while decoding varint")]
    UnexpectedEof,

    #[error("decoded value does not fit in target integer type")]
    Overflow,
}

pub trait VarInt: Copy {
    /// 精确：该值 LEB128 编码后占用的字节数
    fn encoded_len(self) -> usize;

    /// 写 varint
    fn encode_varint(self, buf: &mut impl BufMut);

    /// 读 varint
    fn decode_varint(buf: &[u8]) -> Result<(Self, usize), VarintError>
    where
        Self: Sized;
}

macro_rules! impl_varint_for_uint {
    ($t:ty) => {
        impl VarInt for $t {
            #[inline]
            fn encoded_len(self) -> usize {
                if self == 0 {
                    1
                } else {
                    // 有效位数 = 总位宽 - 前导 0
                    let bits = <$t>::BITS - self.leading_zeros();
                    (bits as usize).div_ceil(7)
                }
            }

            #[inline]
            fn encode_varint(self, buf: &mut impl BufMut) {
                let mut v: $t = self;
                if v == 0 {
                    buf.put_u8(0);
                    return;
                }

                while v > 0x7f {
                    let byte = ((v & 0x7f) as u8) | 0x80;
                    buf.put_u8(byte);
                    v >>= 7;
                }
                buf.put_u8(v as u8);
            }

            #[inline]
            fn decode_varint(buf: &[u8]) -> Result<(Self, usize), VarintError> {
                let mut v: $t = 0;
                let mut shift: u32 = 0;
                let mut i: usize = 0;

                for &byte in buf {
                    // 取低 7 位
                    let low7 = (byte & 0x7f) as $t;

                    // 检查这 7 位移上去后是否会溢出：
                    // 条件：low7 != 0 且 shift >= T::BITS
                    // 用 >= 是因为如果 shift == T::BITS，low7 << shift 这一位已经落到类型范围之外。
                    if low7 != 0 && shift >= <$t>::BITS {
                        return Err(VarintError::Overflow);
                    }

                    v |= low7.wrapping_shl(shift);
                    i += 1;

                    if byte & 0x80 == 0 {
                        // 结束字节，直接返回
                        return Ok((v, i));
                    }

                    shift += 7;
                }

                Err(VarintError::UnexpectedEof)
            }
        }
    };
}

impl_varint_for_uint!(u8);
impl_varint_for_uint!(u16);
impl_varint_for_uint!(u32);
impl_varint_for_uint!(u64);
impl_varint_for_uint!(u128);
impl_varint_for_uint!(usize);

#[inline]
pub fn encode<T: VarInt>(value: T, buf: &mut impl BufMut) {
    value.encode_varint(buf)
}

#[inline]
pub fn decode<T: VarInt>(buf: &[u8]) -> Result<(T, usize), VarintError> {
    T::decode_varint(buf)
}

#[inline]
pub fn encoded_len<T: VarInt>(value: T) -> usize {
    value.encoded_len()
}
