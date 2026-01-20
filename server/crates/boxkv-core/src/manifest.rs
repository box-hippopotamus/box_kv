use std::path::{Path, PathBuf};

use bytes::{BufMut, Bytes, BytesMut};
use thiserror::Error;

use boxkv_storage::{FileSystem, StorageError, WritableFile};

use crate::version::{VersionEdit, VersionSet};
use boxkv_common::config::GlobalConfig;

use std::str;

const BLOCK_SIZE: usize = 32 * 1024;
const HEADER_SIZE: usize = 7;

const RECORD_TYPE_FULL: u8 = 1;
const RECORD_TYPE_FIRST: u8 = 2;
const RECORD_TYPE_MIDDLE: u8 = 3;
const RECORD_TYPE_LAST: u8 = 4;

const DEFAULT_MAX_MANIFEST_FILE_SIZE: u64 = 128 * 1024 * 1024;

fn manifest_file_name(number: u64) -> String {
    format!("MANIFEST-{number:06}")
}

#[inline]
fn manifest_block_size() -> usize {
    if let Some(cfg) = GlobalConfig::try_get() {
        cfg.storage.manifest_block_size_bytes
    } else {
        BLOCK_SIZE
    }
}

pub fn parse_manifest_file_number(name: &str) -> Result<u64, ManifestError> {
    let trimmed = name.trim();
    let prefix = "MANIFEST-";
    if !trimmed.starts_with(prefix) {
        return Err(ManifestError::InvalidManifestFilename(name.to_string()));
    }
    let rest = &trimmed[prefix.len()..];
    rest.parse::<u64>()
        .map_err(|_| ManifestError::InvalidManifestFilename(name.to_string()))
}

#[derive(Debug, Error)]
pub enum ManifestError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("invalid record type {0}")]
    InvalidRecordType(u8),
    #[error("record crc mismatch")]
    CrcMismatch,
    #[error("corrupted fragmented record sequence")]
    CorruptedFragmentedRecord,
    #[error("unexpected end of record")]
    UnexpectedEof,
    #[error("codec error: {0}")]
    Codec(String),
    #[error("version error: {0}")]
    Version(String),
    #[error("invalid manifest filename {0}")]
    InvalidManifestFilename(String),
}

use std::sync::Arc;

pub struct Manifest<FS: FileSystem> {
    writer: ManifestWriter<FS>,
    manifest_file_number: u64,
    db_path: PathBuf,
    fs: Arc<FS>,
    max_file_size: u64,
}

impl<FS: FileSystem> Manifest<FS> {
    pub fn open(
        fs: Arc<FS>,
        db_path: PathBuf,
        manifest_file_number: u64,
        max_file_size: Option<u64>,
    ) -> Result<Self, ManifestError> {
        let filename = manifest_file_name(manifest_file_number);
        let path = db_path.join(filename);
        let writer = ManifestWriter::new(fs.as_ref(), &path)?;
        Ok(Self {
            writer,
            manifest_file_number,
            db_path,
            fs,
            max_file_size: max_file_size.unwrap_or(DEFAULT_MAX_MANIFEST_FILE_SIZE),
        })
    }

    /// 添加 VersionEdit 记录，并根据文件大小自动执行轮换
    pub fn add_record(
        &mut self,
        vs: &VersionSet,
        mut edit: VersionEdit,
    ) -> Result<(), ManifestError> {
        // 检查是否需要轮换
        if self.writer.file_size()? >= self.max_file_size {
            self.rotate(vs, &mut edit)?;
        } else {
            self.writer.add_version_edit(&edit)?;
            self.writer.sync()?;
        }
        Ok(())
    }

    fn rotate(&mut self, vs: &VersionSet, edit: &mut VersionEdit) -> Result<(), ManifestError> {
        // 1. 分配新的 Manifest 文件号
        let new_file_number = vs.allocate_file_number();
        let new_filename = manifest_file_name(new_file_number);
        let new_path = self.db_path.join(&new_filename);

        // 2. 创建新的 Writer
        let mut new_writer = ManifestWriter::new(self.fs.as_ref(), &new_path)?;

        // 3. 构建快照 (Snapshot)
        let mut snapshot = vs.current().build_snapshot();

        snapshot.set_log_number(vs.log_number());
        snapshot.set_last_sequence(vs.last_sequence());
        snapshot.set_next_file_number(vs.next_file_number());

        // 4. 写入快照
        new_writer.add_version_edit(&snapshot)?;

        // 5. 写入当前的新增 Edit
        new_writer.add_version_edit(edit)?;

        // 6. 同步磁盘
        new_writer.sync()?;

        // 7. 原子更新 CURRENT
        update_current(self.fs.as_ref(), &self.db_path, &new_filename)?;

        // 8. 切换内存状态
        self.writer = new_writer;
        self.manifest_file_number = new_file_number;

        Ok(())
    }

    pub fn manifest_file_number(&self) -> u64 {
        self.manifest_file_number
    }
}

pub struct ManifestWriter<FS: FileSystem> {
    file: Box<dyn WritableFile>,
    block_offset: usize,
    _fs: std::marker::PhantomData<FS>,
}

impl<FS: FileSystem> ManifestWriter<FS> {
    pub fn new(fs: &FS, path: &Path) -> Result<Self, ManifestError> {
        let file = fs.open_write(path)?;
        Ok(Self {
            file,
            block_offset: 0,
            _fs: std::marker::PhantomData,
        })
    }

    pub fn add_version_edit(&mut self, edit: &VersionEdit) -> Result<(), ManifestError> {
        let mut buf = BytesMut::new();
        use boxkv_common::codec::Encode;
        edit.encode_to(&mut buf).map_err(ManifestError::Codec)?;
        self.add_record(buf.as_ref())
    }

    pub fn add_record(&mut self, mut data: &[u8]) -> Result<(), ManifestError> {
        let mut first = true;

        while !data.is_empty() {
            let bs = manifest_block_size();
            let block_remaining = bs - self.block_offset;

            if block_remaining < HEADER_SIZE {
                if block_remaining > 0 {
                    let padding = vec![0u8; block_remaining];
                    self.file.write(&padding)?;
                }
                self.block_offset = 0;
            }

            let avail = bs - self.block_offset - HEADER_SIZE;
            let fragment_len = data.len().min(avail);

            let record_type = if first && fragment_len == data.len() {
                RECORD_TYPE_FULL
            } else if first {
                RECORD_TYPE_FIRST
            } else if fragment_len == data.len() {
                RECORD_TYPE_LAST
            } else {
                RECORD_TYPE_MIDDLE
            };

            let fragment = &data[..fragment_len];
            write_record(&mut self.file, record_type, fragment)?;

            self.block_offset += HEADER_SIZE + fragment_len;
            data = &data[fragment_len..];
            first = false;
        }

        Ok(())
    }

    pub fn sync(&mut self) -> Result<(), ManifestError> {
        self.file.flush()?;
        self.file.sync()?;
        Ok(())
    }

    pub fn file_size(&self) -> Result<u64, ManifestError> {
        let size = self.file.get_file_size()?;
        Ok(size)
    }
}

pub struct ManifestReader<FS: FileSystem> {
    _fs: std::marker::PhantomData<FS>,
    data: Bytes,
    offset: usize,
}

impl<FS: FileSystem> ManifestReader<FS> {
    pub fn open(fs: &FS, path: &Path) -> Result<Self, ManifestError> {
        if !fs.exists(path) {
            return Ok(Self {
                _fs: std::marker::PhantomData,
                data: Bytes::new(),
                offset: 0,
            });
        }

        let file = fs.open_read(path)?;
        let data = file.read_all()?;

        Ok(Self {
            _fs: std::marker::PhantomData,
            data,
            offset: 0,
        })
    }

    pub fn next_record(&mut self) -> Result<Option<Vec<u8>>, ManifestError> {
        let mut result = Vec::new();
        let mut in_fragmented = false;

        loop {
            let header_start = self.offset;

            if header_start + HEADER_SIZE > self.data.len() {
                if in_fragmented {
                    return Err(ManifestError::CorruptedFragmentedRecord);
                }
                return Ok(None);
            }

            let bs = manifest_block_size();
            let block_offset = header_start % bs;
            let block_remaining = bs - block_offset;

            if block_remaining < HEADER_SIZE {
                self.offset += block_remaining;
                continue;
            }

            let header = &self.data[header_start..header_start + HEADER_SIZE];
            let stored_crc = u32::from_be_bytes(header[0..4].try_into().unwrap());
            let length = u16::from_be_bytes(header[4..6].try_into().unwrap()) as usize;
            let record_type = header[6];

            self.offset += HEADER_SIZE;

            if length == 0 && record_type == 0 {
                let skip = bs - block_offset - HEADER_SIZE;
                self.offset += skip;
                continue;
            }

            if self.offset + length > self.data.len() {
                return Err(ManifestError::UnexpectedEof);
            }

            if self.offset % bs < HEADER_SIZE && length > 0 {
                return Err(ManifestError::UnexpectedEof);
            }

            let fragment = &self.data[self.offset..self.offset + length];
            self.offset += length;

            let actual_crc = crc32c_with_type(record_type, fragment);
            let unmasked = unmask_crc32c(stored_crc);
            if unmasked != actual_crc {
                return Err(ManifestError::CrcMismatch);
            }

            match record_type {
                RECORD_TYPE_FULL => {
                    if in_fragmented {
                        return Err(ManifestError::CorruptedFragmentedRecord);
                    }
                    return Ok(Some(fragment.to_vec()));
                }
                RECORD_TYPE_FIRST => {
                    if in_fragmented {
                        return Err(ManifestError::CorruptedFragmentedRecord);
                    }
                    result.clear();
                    result.extend_from_slice(fragment);
                    in_fragmented = true;
                }
                RECORD_TYPE_MIDDLE => {
                    if !in_fragmented {
                        continue;
                    }
                    result.extend_from_slice(fragment);
                }
                RECORD_TYPE_LAST => {
                    if !in_fragmented {
                        return Err(ManifestError::CorruptedFragmentedRecord);
                    }
                    result.extend_from_slice(fragment);
                    return Ok(Some(result.clone()));
                }
                other => return Err(ManifestError::InvalidRecordType(other)),
            }
        }
    }

    pub fn next_version_edit(&mut self) -> Result<Option<VersionEdit>, ManifestError> {
        use boxkv_common::codec::Decode;
        match self.next_record()? {
            None => Ok(None),
            Some(data) => {
                let (edit, _) = VersionEdit::decode_from(&data).map_err(ManifestError::Codec)?;
                Ok(Some(edit))
            }
        }
    }
}

pub fn load_version_set_from_manifest<FS: FileSystem>(
    fs: &FS,
    path: &Path,
    num_levels: u32,
    next_file_number: u64,
    last_sequence: u64,
) -> Result<VersionSet, ManifestError> {
    let vs = VersionSet::new(num_levels, next_file_number, last_sequence)
        .map_err(|e| ManifestError::Version(e.to_string()))?;

    let mut reader = ManifestReader::<FS>::open(fs, path)?;

    while let Some(edit) = reader.next_version_edit()? {
        let _ = vs
            .apply_edit(&edit)
            .map_err(|e| ManifestError::Version(e.to_string()))?;
    }

    Ok(vs)
}

/// 读取 CURRENT 文件以获取当前活跃的 Manifest 文件名
pub fn read_current<FS: FileSystem>(fs: &FS, dir: &Path) -> Result<Option<String>, ManifestError> {
    let current_path = dir.join("CURRENT");
    if !fs.exists(&current_path) {
        return Ok(None);
    }
    let file = fs.open_read(&current_path)?;
    let data = file.read_all()?;
    if data.is_empty() {
        return Ok(None);
    }
    let s = str::from_utf8(&data).map_err(|e| ManifestError::Codec(e.to_string()))?;
    let line = s.lines().next().unwrap_or("").trim();
    if line.is_empty() {
        Ok(None)
    } else {
        Ok(Some(line.to_string()))
    }
}

/// 原子更新 CURRENT 指针，指向新的 Manifest 文件名
pub fn update_current<FS: FileSystem>(
    fs: &FS,
    dir: &Path,
    manifest_filename: &str,
) -> Result<(), ManifestError> {
    fs.create_dir(dir).map_err(ManifestError::Storage)?;
    let tmp_path = dir.join("CURRENT.tmp");
    let mut tmp = fs.open_write(&tmp_path)?;
    tmp.write(manifest_filename.as_bytes())?;
    tmp.write(b"\n")?;
    tmp.flush()?;
    tmp.sync()?;
    // 使用 storage 层的 rename 完成原子替换
    let final_path = dir.join("CURRENT");
    fs.rename(&tmp_path, &final_path)?;
    Ok(())
}

/// 自动检测并加载 VersionSet：
/// - 若 CURRENT 存在，则读取并加载对应 Manifest
/// - 若 CURRENT 不存在或为空，则返回空的 VersionSet（用于首次创建）
pub fn load_version_set_autodetect<FS: FileSystem>(
    fs: &FS,
    dir: &Path,
    num_levels: u32,
    next_file_number: u64,
    last_sequence: u64,
) -> Result<VersionSet, ManifestError> {
    match read_current::<FS>(fs, dir)? {
        Some(name) => {
            let path = dir.join(name);
            load_version_set_from_manifest::<FS>(
                fs,
                &path,
                num_levels,
                next_file_number,
                last_sequence,
            )
        }
        None => VersionSet::new(num_levels, next_file_number, last_sequence)
            .map_err(|e| ManifestError::Version(e.to_string())),
    }
}

pub fn load_current_manifest_path<FS: FileSystem>(
    fs: &FS,
    dir: &Path,
) -> Result<Option<PathBuf>, ManifestError> {
    match read_current::<FS>(fs, dir)? {
        Some(name) => Ok(Some(dir.join(name))),
        None => Ok(None),
    }
}

pub fn write_current_manifest<FS: FileSystem>(
    fs: &FS,
    dir: &Path,
    manifest_filename: &str,
) -> Result<(), ManifestError> {
    update_current::<FS>(fs, dir, manifest_filename)
}

fn write_record(
    file: &mut Box<dyn WritableFile>,
    record_type: u8,
    data: &[u8],
) -> Result<(), ManifestError> {
    let length = data.len() as u16;
    let crc = crc32c_with_type(record_type, data);
    let masked_crc = mask_crc32c(crc);

    let mut header = [0u8; HEADER_SIZE];
    header[0..4].copy_from_slice(&masked_crc.to_be_bytes());
    header[4..6].copy_from_slice(&length.to_be_bytes());
    header[6] = record_type;

    file.write(&header)?;
    file.write(data)?;

    Ok(())
}

fn crc32c_with_type(record_type: u8, data: &[u8]) -> u32 {
    let mut buf = BytesMut::with_capacity(1 + data.len());
    buf.put_u8(record_type);
    buf.put_slice(data);
    crc32c::crc32c(&buf)
}

fn mask_crc32c(crc: u32) -> u32 {
    let rotated = crc.rotate_right(15);
    rotated.wrapping_add(0xa282ead8)
}

fn unmask_crc32c(masked: u32) -> u32 {
    let rot = masked.wrapping_sub(0xa282ead8);
    rot.rotate_left(15)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sstable::InternalKey;
    use crate::version::FileMeta;
    use boxkv_storage::LocalFileSystem;
    use bytes::Bytes;
    use tempfile::tempdir;

    fn ik(user_key: &str, seq: u64) -> InternalKey {
        InternalKey::new(Bytes::from(user_key.as_bytes().to_vec()), seq)
    }

    #[test]
    fn record_roundtrip_single_block() {
        let dir = tempdir().unwrap();
        let fs = LocalFileSystem;
        let path = dir.path().join("manifest.log");

        let mut writer = ManifestWriter::<LocalFileSystem>::new(&fs, &path).unwrap();
        let payload = b"hello-manifest";
        writer.add_record(payload).unwrap();
        writer.sync().unwrap();

        let mut reader = ManifestReader::<LocalFileSystem>::open(&fs, &path).unwrap();
        let rec = reader.next_record().unwrap().unwrap();
        assert_eq!(rec.as_slice(), payload);
        assert!(reader.next_record().unwrap().is_none());
    }

    #[test]
    fn record_roundtrip_fragmented() {
        let dir = tempdir().unwrap();
        let fs = LocalFileSystem;
        let path = dir.path().join("manifest_frag.log");

        let mut writer = ManifestWriter::<LocalFileSystem>::new(&fs, &path).unwrap();
        let payload = vec![b'x'; BLOCK_SIZE * 2];
        writer.add_record(&payload).unwrap();
        writer.sync().unwrap();

        let mut reader = ManifestReader::<LocalFileSystem>::open(&fs, &path).unwrap();
        let rec = reader.next_record().unwrap().unwrap();
        assert_eq!(rec.len(), payload.len());
        assert_eq!(rec, payload);
        assert!(reader.next_record().unwrap().is_none());
    }

    #[test]
    fn version_edit_roundtrip() {
        let dir = tempdir().unwrap();
        let fs = LocalFileSystem;
        let path = dir.path().join("manifest_edit.log");

        let mut edit = VersionEdit::default();
        edit.set_last_sequence(123);
        edit.set_next_file_number(1000);

        let f1 = FileMeta::new(1, 1, 10, ik("a", 5), ik("m", 4), 100, 1);
        let f2 = FileMeta::new(2, 2, 20, ik("n", 5), ik("z", 4), 200, 2);
        edit.add_file(1, f1.clone());
        edit.add_file(2, f2.clone());
        edit.delete_file(1, 1);

        let mut writer = ManifestWriter::<LocalFileSystem>::new(&fs, &path).unwrap();
        writer.add_version_edit(&edit).unwrap();
        writer.sync().unwrap();

        let mut reader = ManifestReader::<LocalFileSystem>::open(&fs, &path).unwrap();
        let decoded = reader.next_version_edit().unwrap().unwrap();

        assert_eq!(decoded.new_last_sequence(), Some(123));
        assert_eq!(decoded.new_next_file_number(), Some(1000));

        let deleted = decoded.deleted_files();
        assert_eq!(deleted.len(), 1);
        assert_eq!(deleted[0].0, 1);
        assert_eq!(deleted[0].1, 1);

        let new_files = decoded.new_files();
        assert_eq!(new_files.len(), 2);
        assert_eq!(new_files[0].0, 1);
        assert_eq!(new_files[0].1.file_number, f1.file_number);
        assert_eq!(new_files[1].0, 2);
        assert_eq!(new_files[1].1.file_number, f2.file_number);
    }

    #[test]
    fn load_empty_manifest_creates_empty_versionset() {
        let dir = tempdir().unwrap();
        let fs = LocalFileSystem;
        let path = dir.path().join("empty_manifest.log");

        let vs = load_version_set_from_manifest::<LocalFileSystem>(&fs, &path, 3, 100, 0).unwrap();
        let current = vs.current();
        assert_eq!(current.num_levels(), 3);
        assert_eq!(current.total_size_bytes(), 0);
    }

    #[test]
    fn current_roundtrip_update_and_read() {
        let dir = tempdir().unwrap();
        let fs = LocalFileSystem;
        let root = dir.path();

        update_current::<LocalFileSystem>(&fs, root, "MANIFEST-000123").unwrap();
        let cur = read_current::<LocalFileSystem>(&fs, root).unwrap();
        assert_eq!(cur, Some("MANIFEST-000123".to_string()));

        update_current::<LocalFileSystem>(&fs, root, "MANIFEST-000456").unwrap();
        let cur2 = read_current::<LocalFileSystem>(&fs, root).unwrap();
        assert_eq!(cur2, Some("MANIFEST-000456".to_string()));
    }

    #[test]
    fn load_version_set_autodetect_empty_dir() {
        let dir = tempdir().unwrap();
        let fs = LocalFileSystem;
        let root = dir.path();

        let vs = load_version_set_autodetect::<LocalFileSystem>(&fs, root, 3, 100, 0).unwrap();
        let current = vs.current();
        assert_eq!(current.num_levels(), 3);
        assert_eq!(current.total_size_bytes(), 0);
    }

    #[test]
    fn test_manifest_rotation() {
        let dir = tempdir().unwrap();
        let fs = Arc::new(LocalFileSystem);
        let path = dir.path().to_path_buf();

        let vs = VersionSet::new(3, 100, 0).unwrap();

        let mut manifest = Manifest::open(fs.clone(), path.clone(), 1, Some(200)).unwrap(); // Small size limit

        let mut edit1 = VersionEdit::default();
        edit1.set_last_sequence(10);
        manifest.add_record(&vs, edit1).unwrap();

        let initial_file = manifest.manifest_file_number();
        assert_eq!(initial_file, 1);

        let mut large_edit = VersionEdit::default();
        for i in 0..20 {
            let f = FileMeta::new(i, 0, 1000, ik("a", 10), ik("z", 10), 10, 0);
            large_edit.add_file(0, f);
        }
        manifest.add_record(&vs, large_edit).unwrap();

        let mut edit3 = VersionEdit::default();
        edit3.set_last_sequence(30);
        manifest.add_record(&vs, edit3).unwrap();

        let new_file = manifest.manifest_file_number();
        assert_ne!(new_file, initial_file);

        let current_name = read_current(&*fs, &path).unwrap().unwrap();
        assert_eq!(current_name, manifest_file_name(new_file));

        let mut reader =
            ManifestReader::<LocalFileSystem>::open(&*fs, &path.join(current_name)).unwrap();

        let snapshot = reader.next_version_edit().unwrap().unwrap();
        assert_eq!(snapshot.new_next_file_number(), Some(101));

        let rec2 = reader.next_version_edit().unwrap().unwrap();
        assert_eq!(rec2.new_last_sequence(), Some(30));
    }
}
