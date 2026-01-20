use crate::sstable::block::types::BlockCodec;
use crate::sstable::data_block::InternalKey;
use crate::sstable::filter::block::key_partitioned::index::types::{
    FilterPartitionIndexKey, FilterPartitionIndexValue,
};
use crate::sstable::format::BlockHandle;
use boxkv_common::codec::{Decode, Encode};
use bytes::{BufMut, Bytes};
use thiserror::Error;
const SEQUENCE_LEN: usize = 8;

/// Filter Partition Index Codec
#[derive(Clone, Debug)]
pub struct FilterPartitionIndexCodec;

impl FilterPartitionIndexCodec {
    /// 创建新的编解码器
    pub fn new() -> Self {
        Self
    }
}

impl BlockCodec for FilterPartitionIndexCodec {
    type Key = FilterPartitionIndexKey;
    type Value = FilterPartitionIndexValue;
    type Error = FilterPartitionIndexError;

    fn encode_key(
        &self,
        key: &Self::Key,
        buf: &mut impl BufMut,
        shared_prefix_len: usize,
    ) -> Result<(), Self::Error> {
        let user_key = key.user_key();
        let total_len = key.encoded_len();
        let user_key_len = user_key.len();
        if shared_prefix_len >= total_len {
            return Ok(());
        }
        if shared_prefix_len < user_key_len {
            buf.put_slice(&user_key[shared_prefix_len..]);
            buf.put_u64(key.sequence());
        } else {
            let sequence_skip = shared_prefix_len - user_key_len;
            let sequence_bytes = key.sequence().to_be_bytes();
            buf.put_slice(&sequence_bytes[sequence_skip..]);
        }
        Ok(())
    }

    fn decode_key(&self, data: &[u8]) -> Result<(Self::Key, usize), Self::Error> {
        if data.len() < SEQUENCE_LEN {
            return Err(FilterPartitionIndexError::TruncatedData {
                expected: SEQUENCE_LEN,
                actual: data.len(),
            });
        }

        let user_key_len = data.len() - SEQUENCE_LEN;
        let user_key = Bytes::copy_from_slice(&data[..user_key_len]);
        let sequence_bytes: [u8; SEQUENCE_LEN] = data[user_key_len..user_key_len + SEQUENCE_LEN]
            .try_into()
            .map_err(|_| FilterPartitionIndexError::InvalidSequence)?;
        let sequence = u64::from_be_bytes(sequence_bytes);

        Ok((InternalKey::new(user_key, sequence), data.len()))
    }

    fn decode_key_with_prefix(
        &self,
        prev_key: &Self::Key,
        unshared_data: &[u8],
        shared_len: usize,
    ) -> Result<Self::Key, Self::Error> {
        let prev_user_key = prev_key.user_key();
        let prev_total_len = prev_user_key.len() + SEQUENCE_LEN;

        if shared_len > prev_total_len {
            return Err(FilterPartitionIndexError::InvalidSharedPrefix {
                shared_len,
                prev_key_len: prev_total_len,
            });
        }

        let current_total_len = shared_len + unshared_data.len();
        if current_total_len < SEQUENCE_LEN {
            return Err(FilterPartitionIndexError::TruncatedData {
                expected: SEQUENCE_LEN,
                actual: current_total_len,
            });
        }

        let current_user_key_len = current_total_len - SEQUENCE_LEN;
        if unshared_data.len() >= SEQUENCE_LEN {
            let unshared_user_key_len = current_user_key_len - shared_len;
            let mut user_key_buf = bytes::BytesMut::with_capacity(current_user_key_len);

            prev_key.copy_range_to(0..shared_len, &mut user_key_buf);
            user_key_buf.put_slice(&unshared_data[..unshared_user_key_len]);

            let user_key = user_key_buf.freeze();
            let sequence_bytes: [u8; SEQUENCE_LEN] = unshared_data
                [unshared_user_key_len..unshared_user_key_len + SEQUENCE_LEN]
                .try_into()
                .map_err(|_| FilterPartitionIndexError::InvalidSequence)?;
            let sequence = u64::from_be_bytes(sequence_bytes);

            Ok(InternalKey::new(user_key, sequence))
        } else {
            let mut user_key_buf = bytes::BytesMut::with_capacity(current_user_key_len);

            if current_user_key_len > prev_key.encoded_len() {
                return Err(FilterPartitionIndexError::InvalidSharedPrefix {
                    shared_len,
                    prev_key_len: prev_total_len,
                });
            }

            prev_key.copy_range_to(0..current_user_key_len, &mut user_key_buf);

            let user_key = user_key_buf.freeze();
            let mut seq_buf = bytes::BytesMut::with_capacity(SEQUENCE_LEN);

            prev_key.copy_range_to(current_user_key_len..shared_len, &mut seq_buf);
            seq_buf.put_slice(unshared_data);

            let sequence_bytes: [u8; SEQUENCE_LEN] = seq_buf
                .freeze()
                .as_ref()
                .try_into()
                .map_err(|_| FilterPartitionIndexError::InvalidSequence)?;

            let sequence = u64::from_be_bytes(sequence_bytes);

            Ok(InternalKey::new(user_key, sequence))
        }
    }

    fn encode_value(&self, value: &Self::Value, buf: &mut impl BufMut) -> Result<(), Self::Error> {
        value
            .encode_to(buf)
            .map_err(|e| FilterPartitionIndexError::BlockHandleError(e.to_string()))
    }

    fn decode_value(
        &self,
        data: &[u8],
        _value_len: usize,
    ) -> Result<(Self::Value, usize), Self::Error> {
        let (handle, consumed) = BlockHandle::decode_from(data)
            .map_err(|e| FilterPartitionIndexError::BlockHandleError(e.to_string()))?;
        Ok((handle, consumed))
    }

    fn encoded_key_len(&self, key: &Self::Key) -> usize {
        key.encoded_len()
    }

    fn encoded_value_len(&self, value: &Self::Value) -> usize {
        value.encoded_len()
    }

    fn shared_prefix_len(&self, a: &Self::Key, b: &Self::Key) -> usize {
        let max_len = a.encoded_len().min(b.encoded_len());
        for i in 0..max_len {
            if a.get(i) != b.get(i) {
                return i;
            }
        }
        max_len
    }
}

/// Filter Partition Index 错误
#[derive(Debug, Error)]
pub enum FilterPartitionIndexError {
    #[error("Truncated data: expected {expected} bytes, got {actual}")]
    TruncatedData { expected: usize, actual: usize },

    #[error("Invalid shared prefix: {shared_len} > {prev_key_len}")]
    InvalidSharedPrefix {
        shared_len: usize,
        prev_key_len: usize,
    },

    #[error("Invalid sequence")]
    InvalidSequence,

    #[error("BlockHandle decode error: {0}")]
    BlockHandleError(String),

    #[error("Encode error: {0}")]
    EncodeError(String),

    #[error("Decode error: {0}")]
    DecodeError(String),
}
