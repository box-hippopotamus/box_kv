use std::path::PathBuf;

use super::{WAL_HEADER_SIZE, WAL_KEY_LEN_SIZE};
use boxkv_common::codec::Encode;
use boxkv_common::types::{Entry, ValueType};
use boxkv_storage::{FileSystem, StorageError, WritableFile};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum WriteError {
    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Storage(#[from] StorageError),

    #[error("Encode error: {0}")]
    Encode(String),
}

/// WAL 文件写入器
pub struct WalWriter<FS: FileSystem> {
    file: Box<dyn WritableFile>,
    _fs: std::marker::PhantomData<FS>,
}

impl<FS: FileSystem> WalWriter<FS> {
    /// 为指定路径创建 WalWriter
    ///
    /// 文件不存在则创建，存在则截断
    pub fn new(fs: &FS, path: PathBuf) -> Result<Self, WriteError> {
        let file = fs.open_write(&path).map_err(WriteError::Storage)?;

        Ok(Self {
            file,
            _fs: std::marker::PhantomData,
        })
    }

    /// 序列化并追加一条 Entry 到 WAL 缓冲区
    ///
    /// # 格式
    /// 写入顺序：
    /// 1. Header: CRC | PayloadLen | ValueTag | Seq
    /// 2. Payload: KeyLen | Key | Value Section
    pub fn append(&mut self, entry: &Entry) -> Result<(), WriteError> {
        let value_type = entry.value.tag();
        let key_len = entry.key.len() as u64;
        let value_len = entry.value.encoded_len() as u64;
        let sequence = entry.sequence;

        // 计算 payload 长度：Key Length + Value Length + Key Data + Value Data
        let payload_len = WAL_KEY_LEN_SIZE as u64 + key_len + value_len;

        // 1. 计算 CRC 校验和
        // CRC 覆盖：Payload Length, Type, Sequence Number, Key Length, Value Length, Key, Value
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&payload_len.to_be_bytes());
        hasher.update(&[value_type]);
        hasher.update(&sequence.to_be_bytes());
        hasher.update(&key_len.to_be_bytes());
        hasher.update(&entry.key);

        // value body（具体布局见 ValueType::encode_value_body）
        match &entry.value {
            ValueType::Normal(data) => {
                hasher.update(data);
            }
            ValueType::Tombstone => {}
            ValueType::Expiring { data, expire_at } => {
                hasher.update(&expire_at.to_be_bytes());
                hasher.update(data);
            }
        }

        let crc = hasher.finalize();

        // 2. 写入 Header
        // [CRC: 4 bytes]
        let crc_bytes = crc.to_be_bytes();
        self.file.write(&crc_bytes)?;

        // [Payload Length: 8 bytes]
        let payload_len_bytes = payload_len.to_be_bytes();
        self.file.write(&payload_len_bytes)?;

        // [Type: 1 byte]
        self.file.write(&[value_type])?;

        // [Sequence: 8 bytes]
        let seq_bytes = sequence.to_be_bytes();
        self.file.write(&seq_bytes)?;

        // [Key Length: 8 bytes]
        let key_len_bytes = key_len.to_be_bytes();
        self.file.write(&key_len_bytes)?;

        self.file.write(&entry.key)?;

        // 3. 写入 value body
        let mut buf = Vec::new();
        entry
            .value
            .encode_to(&mut buf)
            .map_err(|e| WriteError::Encode(format!("{:?}", e)))?;
        self.file.write(&buf)?;

        Ok(())
    }

    /// 批量序列化并追加多条 Entry
    pub fn append_batch(&mut self, entries: &[Entry]) -> Result<(), WriteError> {
        if entries.is_empty() {
            return Ok(());
        }

        // 预分配缓冲区以减少重新分配
        // 每条记录有 21 字节 header + key/value payload
        let mut buf: Vec<u8> = Vec::with_capacity(entries.len() * (WAL_HEADER_SIZE + 32));

        for entry in entries {
            let value_type = entry.value.tag();
            let key_len = entry.key.len() as u64;
            let value_len = entry.value.encoded_len() as u64;
            let sequence = entry.sequence;

            // payload = key_len (8B) + key + value_body
            let payload_len = WAL_KEY_LEN_SIZE as u64 + key_len + value_len;

            // 计算 CRC（与 append 相同的字段）
            let mut hasher = crc32fast::Hasher::new();
            hasher.update(&payload_len.to_be_bytes());
            hasher.update(&[value_type]);
            hasher.update(&sequence.to_be_bytes());
            hasher.update(&key_len.to_be_bytes());
            hasher.update(&entry.key);

            // value body
            let mut value_buf = Vec::with_capacity(value_len as usize);
            entry
                .value
                .encode_to(&mut value_buf)
                .map_err(|e| WriteError::Encode(format!("{:?}", e)))?;
            hasher.update(&value_buf);

            let crc = hasher.finalize();

            // Header
            buf.extend_from_slice(&crc.to_be_bytes());
            buf.extend_from_slice(&payload_len.to_be_bytes());
            buf.push(value_type);
            buf.extend_from_slice(&sequence.to_be_bytes());
            buf.extend_from_slice(&key_len.to_be_bytes());

            // Key
            buf.extend_from_slice(&entry.key);

            // Value body
            buf.extend_from_slice(&value_buf);
        }

        // 单次写入所有记录
        self.file.write(&buf)?;

        Ok(())
    }

    /// 刷新所有缓冲写入到磁盘（fsync）
    pub fn sync(&mut self) -> Result<(), WriteError> {
        self.file.sync()?;
        Ok(())
    }
}
