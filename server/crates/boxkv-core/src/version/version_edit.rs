use crate::{sstable::InternalKey, version::FileMeta};
use boxkv_common::{
    codec::{Decode, Encode},
    varint,
};
use bytes::BufMut;

const TAG_COMPARATOR: u8 = 1;
const TAG_LOG_NUMBER: u8 = 2;
const TAG_NEXT_FILE_NUMBER: u8 = 3;
const TAG_LAST_SEQUENCE: u8 = 4;
const TAG_COMPACT_CURSOR: u8 = 5;
const TAG_DELETED_FILE: u8 = 6;
const TAG_NEW_FILE: u8 = 7;

#[derive(Debug, Clone, Default)]
pub struct VersionEdit {
    /// 比较器名称
    comparator_name: Option<String>,
    /// 当前活跃的 WAL 日志文件编号
    log_number: Option<u64>,
    /// 待删除的文件集合
    deleted_files: Vec<(u32, u64)>,
    /// 待新增的文件集合
    new_files: Vec<(u32, FileMeta)>,
    /// 新的全局最大序列号
    new_last_sequence: Option<u64>,
    /// 新的文件编号分配器游标
    new_next_file_number: Option<u64>,
    /// 各层级的压缩游标（Compaction 指针）
    compact_cursors: Vec<(u32, InternalKey)>,
}

impl VersionEdit {
    pub fn set_comparator_name<S: Into<String>>(&mut self, name: S) {
        self.comparator_name = Some(name.into());
    }

    pub fn comparator_name(&self) -> Option<&str> {
        self.comparator_name.as_deref()
    }

    pub fn set_log_number(&mut self, log_number: u64) {
        self.log_number = Some(log_number);
    }

    pub fn log_number(&self) -> Option<u64> {
        self.log_number
    }

    pub fn delete_file(&mut self, level: u32, file_number: u64) {
        self.deleted_files.push((level, file_number));
    }

    pub fn add_file(&mut self, level: u32, file_meta: FileMeta) {
        self.new_files.push((level, file_meta));
    }

    pub fn set_last_sequence(&mut self, sequence: u64) {
        self.new_last_sequence = Some(sequence);
    }

    pub fn set_next_file_number(&mut self, file_number: u64) {
        self.new_next_file_number = Some(file_number);
    }

    pub fn add_compact_cursor(&mut self, level: u32, key: InternalKey) {
        self.compact_cursors.push((level, key));
    }

    pub fn deleted_files(&self) -> &[(u32, u64)] {
        &self.deleted_files
    }

    pub fn new_files(&self) -> &[(u32, FileMeta)] {
        &self.new_files
    }

    pub fn new_last_sequence(&self) -> Option<u64> {
        self.new_last_sequence
    }

    pub fn new_next_file_number(&self) -> Option<u64> {
        self.new_next_file_number
    }

    pub fn compact_cursors(&self) -> &[(u32, InternalKey)] {
        &self.compact_cursors
    }
}

impl Encode for VersionEdit {
    type CodecError = String;

    fn encode_to(&self, buf: &mut impl BufMut) -> Result<(), Self::CodecError> {
        if let Some(name) = &self.comparator_name {
            buf.put_u8(TAG_COMPARATOR);
            let bytes = name.as_bytes();
            varint::encode(bytes.len() as u64, buf);
            buf.put_slice(bytes);
        }

        if let Some(log) = self.log_number {
            buf.put_u8(TAG_LOG_NUMBER);
            varint::encode(log, buf);
        }

        if let Some(seq) = self.new_last_sequence {
            buf.put_u8(TAG_LAST_SEQUENCE);
            varint::encode(seq, buf);
        }

        if let Some(next) = self.new_next_file_number {
            buf.put_u8(TAG_NEXT_FILE_NUMBER);
            varint::encode(next, buf);
        }

        for (level, key) in &self.compact_cursors {
            buf.put_u8(TAG_COMPACT_CURSOR);
            varint::encode(*level, buf);
            encode_internal_key(key, buf);
        }

        for (level, file_number) in &self.deleted_files {
            buf.put_u8(TAG_DELETED_FILE);
            varint::encode(*level, buf);
            varint::encode(*file_number, buf);
        }

        for (level, meta) in &self.new_files {
            buf.put_u8(TAG_NEW_FILE);
            varint::encode(*level, buf);
            varint::encode(meta.file_number, buf);
            varint::encode(meta.size_bytes, buf);
            encode_internal_key(&meta.smallest, buf);
            encode_internal_key(&meta.largest, buf);
            varint::encode(meta.num_entries, buf);
            varint::encode(meta.num_deletions, buf);
            varint::encode(meta.creation_time_unix, buf);
        }

        Ok(())
    }

    fn encoded_len(&self) -> usize {
        let mut len = 0usize;

        if let Some(name) = &self.comparator_name {
            len += 1;
            let bytes = name.as_bytes();
            len += varint::encoded_len(bytes.len() as u64);
            len += bytes.len();
        }

        if let Some(log) = self.log_number {
            len += 1;
            len += varint::encoded_len(log);
        }

        if let Some(seq) = self.new_last_sequence {
            len += 1;
            len += varint::encoded_len(seq);
        }

        if let Some(next) = self.new_next_file_number {
            len += 1;
            len += varint::encoded_len(next);
        }

        for (level, key) in &self.compact_cursors {
            len += 1;
            len += varint::encoded_len(*level);
            len += encoded_internal_key_len(key);
        }

        for (level, file_number) in &self.deleted_files {
            len += 1;
            len += varint::encoded_len(*level);
            len += varint::encoded_len(*file_number);
        }

        for (level, meta) in &self.new_files {
            len += 1;
            len += varint::encoded_len(*level);
            len += varint::encoded_len(meta.file_number);
            len += varint::encoded_len(meta.size_bytes);
            len += encoded_internal_key_len(&meta.smallest);
            len += encoded_internal_key_len(&meta.largest);
            len += varint::encoded_len(meta.num_entries);
            len += varint::encoded_len(meta.num_deletions);
            len += varint::encoded_len(meta.creation_time_unix);
        }

        len
    }
}

impl Decode for VersionEdit {
    type CodecError = String;

    fn decode_from(buf: &[u8]) -> Result<(Self, usize), Self::CodecError> {
        let mut pos = 0usize;
        let mut edit = VersionEdit::default();

        while pos < buf.len() {
            let tag = buf[pos];
            pos += 1;

            match tag {
                TAG_COMPARATOR => {
                    let (len, read) =
                        varint::decode::<u64>(&buf[pos..]).map_err(|e| e.to_string())?;
                    pos += read;
                    let len_usize = len as usize;
                    if buf.len() < pos + len_usize {
                        return Err("unexpected eof while decoding comparator name".to_string());
                    }
                    let s = std::str::from_utf8(&buf[pos..pos + len_usize])
                        .map_err(|e| e.to_string())?;
                    edit.set_comparator_name(s.to_string());
                    pos += len_usize;
                }
                TAG_LOG_NUMBER => {
                    let (value, read) =
                        varint::decode::<u64>(&buf[pos..]).map_err(|e| e.to_string())?;
                    pos += read;
                    edit.set_log_number(value);
                }
                TAG_LAST_SEQUENCE => {
                    let (value, read) =
                        varint::decode::<u64>(&buf[pos..]).map_err(|e| e.to_string())?;
                    pos += read;
                    edit.set_last_sequence(value);
                }
                TAG_NEXT_FILE_NUMBER => {
                    let (value, read) =
                        varint::decode::<u64>(&buf[pos..]).map_err(|e| e.to_string())?;
                    pos += read;
                    edit.set_next_file_number(value);
                }
                TAG_COMPACT_CURSOR => {
                    let (level, r1) =
                        varint::decode::<u32>(&buf[pos..]).map_err(|e| e.to_string())?;
                    pos += r1;
                    let (key, r2) = decode_internal_key(&buf[pos..])?;
                    pos += r2;
                    edit.add_compact_cursor(level, key);
                }
                TAG_DELETED_FILE => {
                    let (level, r1) =
                        varint::decode::<u32>(&buf[pos..]).map_err(|e| e.to_string())?;
                    pos += r1;
                    let (file_number, r2) =
                        varint::decode::<u64>(&buf[pos..]).map_err(|e| e.to_string())?;
                    pos += r2;
                    edit.delete_file(level, file_number);
                }
                TAG_NEW_FILE => {
                    let (level, r1) =
                        varint::decode::<u32>(&buf[pos..]).map_err(|e| e.to_string())?;
                    pos += r1;
                    let (file_number, r2) =
                        varint::decode::<u64>(&buf[pos..]).map_err(|e| e.to_string())?;
                    pos += r2;
                    let (size_bytes, r3) =
                        varint::decode::<u64>(&buf[pos..]).map_err(|e| e.to_string())?;
                    pos += r3;
                    let (smallest, r4) = decode_internal_key(&buf[pos..])?;
                    pos += r4;
                    let (largest, r5) = decode_internal_key(&buf[pos..])?;
                    pos += r5;
                    let (num_entries, r6) =
                        varint::decode::<u64>(&buf[pos..]).map_err(|e| e.to_string())?;
                    pos += r6;
                    let (num_deletions, r7) =
                        varint::decode::<u64>(&buf[pos..]).map_err(|e| e.to_string())?;
                    pos += r7;
                    let (creation_time_unix, r8) =
                        varint::decode::<u64>(&buf[pos..]).map_err(|e| e.to_string())?;
                    pos += r8;

                    let meta = FileMeta {
                        file_number,
                        level,
                        size_bytes,
                        smallest,
                        largest,
                        num_entries,
                        num_deletions,
                        creation_time_unix,
                    };

                    edit.add_file(level, meta);
                }
                _ => return Err("invalid tag".to_string()),
            }
        }

        Ok((edit, pos))
    }
}

fn encode_internal_key(key: &InternalKey, buf: &mut impl BufMut) {
    let len = key.user_key().len() as u32;
    varint::encode(len, buf);
    buf.put_slice(key.user_key());
    varint::encode(key.sequence(), buf);
}

fn encoded_internal_key_len(key: &InternalKey) -> usize {
    let len = key.user_key().len() as u32;
    varint::encoded_len(len) + key.user_key().len() + varint::encoded_len(key.sequence())
}

fn decode_internal_key(buf: &[u8]) -> Result<(InternalKey, usize), String> {
    let (len, r1) = varint::decode::<u32>(buf).map_err(|e| e.to_string())?;
    let len_usize = len as usize;
    if buf.len() < r1 + len_usize {
        return Err("unexpected eof while decoding internal key".to_string());
    }
    let start = r1;
    let end = r1 + len_usize;
    let user_key = bytes::Bytes::copy_from_slice(&buf[start..end]);
    let (seq, r2) = varint::decode::<u64>(&buf[end..]).map_err(|e| e.to_string())?;
    let total = r1 + len_usize + r2;
    Ok((InternalKey::new(user_key, seq), total))
}
