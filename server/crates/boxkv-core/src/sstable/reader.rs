use crate::cache::{BlockCache, BlockCacheKey};
use crate::sstable::{
    Result, SSTableContext, SSTableError, compression,
    data_block::{DataBlockCodec, DataBlockReader, InternalKey},
    filter::block::FilterBlockReader,
    footer::{FOOTER_SIZE, Footer},
    format::{BlockHandle, BlockTrailer},
    index_block::{IndexBlockCodec, IndexBlockReader, IndexKey},
    meta_index::{MetaIndexCodec, MetaIndexReader},
};
use boxkv_common::config::GlobalConfig;
use boxkv_common::{codec::Decode, types::ValueType};
use boxkv_storage::{FileSystem, LocalFileSystem, RandomAccessFileReader};
use bytes::{Bytes, BytesMut};
use std::path::Path;
use std::sync::Arc;

/// SSTable 读取器
pub struct SSTableReader {
    /// 文件读取器
    file: Arc<RandomAccessFileReader>,

    /// 文件唯一 ID
    file_id: u64,

    /// IndexBlock 读取器（
    index_block: IndexBlockReader,

    /// FilterBlock 读取器
    filter_block: Option<Box<dyn FilterBlockReader>>,

    /// Block Cache
    block_cache: Option<Arc<BlockCache>>,
}

impl SSTableReader {
    /// 打开 SSTable 文件
    pub fn open(path: &Path, ctx: &SSTableContext) -> Result<Self> {
        // 使用 LocalFileSystem 打开文件
        let fs = LocalFileSystem;
        let readable_file = fs.open_read(path).map_err(SSTableError::Storage)?;

        let file_size = fs.file_size(path).map_err(SSTableError::Storage)?;

        // 验证文件大小
        if file_size < FOOTER_SIZE as u64 {
            return Err(SSTableError::Corrupted(format!(
                "File too small: {} < {}",
                file_size, FOOTER_SIZE
            )));
        }

        // 生成文件 ID（使用路径的 hash）
        let file_id = {
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};
            let mut hasher = DefaultHasher::new();
            path.hash(&mut hasher);
            hasher.finish()
        };

        let reader = Arc::new(RandomAccessFileReader::new(readable_file));

        // 1. 读取 Footer
        let footer_offset = file_size - FOOTER_SIZE as u64;
        let footer_buf = reader
            .read(footer_offset, FOOTER_SIZE)
            .map_err(SSTableError::Storage)?;

        let footer = Footer::decode(&footer_buf)?;

        // 验证 magic number
        if !footer.validate_magic() {
            return Err(SSTableError::Corrupted("Invalid magic number".into()));
        }

        // 2. 读取并解析 MetaIndex Block
        let meta_index_data = Self::read_block_internal(&reader, &footer.metaindex_block_handle)?;
        let meta_index = MetaIndexReader::new(MetaIndexCodec, meta_index_data).map_err(|e| {
            SSTableError::Corrupted(format!("Failed to parse meta index block: {:?}", e))
        })?;

        // 3. 使用工厂从 MetaIndex 打开 FilterBlock
        let block_loader = |handle: &BlockHandle| Self::read_block_internal(&reader, handle);
        let partition_loader = {
            let reader_clone = Arc::clone(&reader);
            let cache_clone = ctx.block_cache.clone();
            move |handle: &BlockHandle| {
                Self::read_block_with_cache(
                    reader_clone.as_ref(),
                    handle,
                    file_id,
                    cache_clone.clone(),
                )
                .map_err(|e| crate::sstable::filter::FilterError::DecodeError(e.to_string()))
            }
        };

        let cfg = &GlobalConfig::get().sstable;
        let filter_block = crate::sstable::filter::factory::open_from_metaindex(
            &meta_index,
            cfg,
            ctx.filter_policy.as_ref().map(|p| p.as_ref()),
            block_loader,
            partition_loader,
        )?;

        // 4. 读取并解析 IndexBlock
        let index_data = Self::read_block_internal(&reader, &footer.index_block_handle)?;
        let index_block = IndexBlockReader::new(IndexBlockCodec, index_data).map_err(|e| {
            SSTableError::Corrupted(format!("Failed to parse index block: {:?}", e))
        })?;

        // 5. 获取 Block Cache
        let block_cache = ctx.block_cache.clone();

        Ok(Self {
            file: reader,
            file_id,
            index_block,
            filter_block,
            block_cache,
        })
    }

    /// 点查询
    pub fn get(&self, key: Bytes) -> Result<Option<ValueType>> {
        self.get_at(key, u64::MAX)
    }

    pub fn get_at(&self, key: Bytes, sequence: u64) -> Result<Option<ValueType>> {
        if let Some(ref filter) = self.filter_block {
            let dummy_handle = BlockHandle::new(0, 0);
            let filter_key = InternalKey::new(key.clone(), sequence);
            if !filter.key_may_match(filter_key, &dummy_handle) {
                return Ok(None);
            }
        }

        let search_key = IndexKey {
            user_key: key.clone(),
        };

        let block_handle = match self.index_block.get(&search_key) {
            Ok(Some(index_value)) => index_value,
            Ok(None) => return Ok(None),
            Err(e) => {
                return Err(SSTableError::Corrupted(format!(
                    "Failed to search index block: {:?}",
                    e
                )));
            }
        };

        let data_block_data = self.read_block(&block_handle)?;
        let data_block = DataBlockReader::new(DataBlockCodec, data_block_data)
            .map_err(|e| SSTableError::Corrupted(format!("Failed to parse data block: {:?}", e)))?;

        let search_internal_key = InternalKey {
            user_key: key.clone(),
            sequence,
        };

        let mut data_iter = data_block.iter();

        data_iter.seek(&search_internal_key).map_err(|e| {
            SSTableError::Corrupted(format!("Failed to seek in data block: {:?}", e))
        })?;

        if !data_iter.valid() {
            return Ok(None);
        }

        let found_key = match data_iter.key() {
            Some(k) => k,
            None => return Ok(None),
        };

        if found_key.user_key != key {
            return Ok(None);
        }

        let v = data_iter
            .value()
            .map_err(|e| SSTableError::Corrupted(format!("Failed to decode value: {:?}", e)))?;

        Ok(v)
    }

    /// 读取 Block（带解压、校验和 Cache）
    ///
    /// # Block 格式
    /// ```text
    /// [compressed_data][compression_type: 1 byte][crc32: 4 bytes]
    /// ```
    fn read_block(&self, handle: &BlockHandle) -> Result<Bytes> {
        // 1. 尝试从 Cache 读取
        if let Some(ref cache) = self.block_cache {
            let cache_key = BlockCacheKey::new(self.file_id, handle.offset);

            if let Some(cached_block) = cache.get(&cache_key) {
                return Ok(cached_block);
            }

            let block_data = Self::read_block_internal(&self.file, handle)?;

            cache.insert(cache_key, block_data.clone());

            return Ok(block_data);
        }

        // 2. 没有 Cache，直接从磁盘读取
        Self::read_block_internal(&self.file, handle)
    }

    /// 读取 Block
    fn read_block_internal(reader: &RandomAccessFileReader, handle: &BlockHandle) -> Result<Bytes> {
        // 1. 读取 [compressed_data][trailer]
        let total_size = handle.size as usize;
        let buf = reader
            .read(handle.offset, total_size)
            .map_err(SSTableError::Storage)?;

        if buf.len() < BlockTrailer::SIZE {
            return Err(SSTableError::Corrupted(format!(
                "Block too small: {} < {}",
                buf.len(),
                BlockTrailer::SIZE
            )));
        }

        // 2. 分离 trailer
        // buf 格式: [compressed_data][compression_type: 1 byte][crc32: 4 bytes]
        let data_size = total_size - BlockTrailer::SIZE;
        let compressed_data = &buf[..data_size];
        let trailer_data = &buf[data_size..];
        let (trailer, _) = BlockTrailer::decode_from(trailer_data)?;

        // 3. 校验 CRC32
        let crc_data = &buf[..data_size + 1];
        let actual_crc = compression::compute_crc32c(crc_data);
        if actual_crc != trailer.crc32 {
            return Err(SSTableError::Corrupted(format!(
                "CRC mismatch: expected {}, got {}",
                trailer.crc32, actual_crc
            )));
        }

        // 4. 解压
        let compression_type = trailer.compression_type;
        let mut uncompressed_buf = BytesMut::new();
        compression::decompress(compressed_data, compression_type, &mut uncompressed_buf)
            .map_err(|e| SSTableError::Corrupted(format!("Decompression failed: {:?}", e)))?;

        Ok(uncompressed_buf.freeze())
    }

    fn read_block_with_cache(
        reader: &RandomAccessFileReader,
        handle: &BlockHandle,
        file_id: u64,
        block_cache: Option<Arc<BlockCache>>,
    ) -> Result<Bytes> {
        if let Some(cache) = block_cache {
            let cache_key = BlockCacheKey::new(file_id, handle.offset);
            if let Some(cached_block) = cache.get(&cache_key) {
                return Ok(cached_block);
            }
            let block_data = Self::read_block_internal(reader, handle)?;
            cache.insert(cache_key, block_data.clone());
            return Ok(block_data);
        }
        Self::read_block_internal(reader, handle)
    }

    pub(crate) fn prefetch_block(&self, handle: &BlockHandle) -> Result<()> {
        self.file
            .prefetch(handle.offset, handle.size as usize)
            .map_err(SSTableError::Storage)
    }

    /// 获取 IndexBlock 读取器
    pub(crate) fn index_block(&self) -> &IndexBlockReader {
        &self.index_block
    }

    /// 读取 DataBlock
    pub(crate) fn read_data_block(&self, handle: &BlockHandle) -> Result<Bytes> {
        self.read_block(handle)
    }
}
