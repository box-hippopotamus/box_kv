use crate::sstable::{
    Result, SSTableContext, SSTableError,
    compression::{self, CompressionType},
    data_block::{DataBlockBuilder, DataBlockCodec, InternalKey},
    filter::{
        FilterBlockBuilder,
        block::{FullFilterBlockBuilder, PartitionedFilterBlockBuilder},
        policy::FilterBuildingContext,
    },
    footer::Footer,
    format::{BlockHandle, BlockTrailer},
    index_block::{IndexBlockBuilder, IndexBlockCodec, IndexKey},
    meta_index::{MetaIndexBuilder, MetaIndexCodec, MetaIndexKey},
};
use boxkv_common::config::{FilterBlockType, GlobalConfig};

/// 将 GlobalConfig 的 CompressionType 转换为 SSTable 的 CompressionType
fn convert_compression_type(cfg_type: boxkv_common::config::CompressionType) -> CompressionType {
    match cfg_type {
        boxkv_common::config::CompressionType::None => CompressionType::None,
        boxkv_common::config::CompressionType::Snappy => CompressionType::Snappy,
        boxkv_common::config::CompressionType::Lz4 => CompressionType::Lz4,
        boxkv_common::config::CompressionType::Zstd => CompressionType::Zstd,
    }
}
use boxkv_common::{codec::Encode, types::Entry};
use boxkv_storage::{FileSystem, LocalFileSystem, WritableFileWriter};
use bytes::{Bytes, BytesMut};
use std::path::Path;

/// SSTable 构建器
pub struct SSTableBuilder {
    file: WritableFileWriter,

    // 当前正在构建的 DataBlock
    data_block_builder: DataBlockBuilder,

    // Filter 构建器
    filter_builder: Option<Box<dyn FilterBlockBuilder>>,

    // Filter Policy（用于获取 compatibility_name）
    filter_policy: Option<std::sync::Arc<dyn crate::sstable::FilterPolicy>>,

    // Index 构建器
    index_block_builder: IndexBlockBuilder,

    // 状态跟踪
    entry_count: usize,

    // 元数据
    min_key: Option<Bytes>,
    max_key: Option<InternalKey>, // MVCC: 保存完整的 InternalKey 用于顺序检查

    // 状态标记
    finished: bool,
}

/// SSTable 元数据
#[derive(Debug, Clone)]
pub struct SSTableMetadata {
    pub file_size: u64,
    pub entry_count: usize,
    pub min_key: Bytes,
    pub max_key: Bytes,
}

impl SSTableBuilder {
    /// 创建新的 SSTable 构建器
    pub fn create(path: &Path, ctx: &SSTableContext, level: i32) -> Result<Self> {
        // 从全局配置读取 SSTable 配置
        let cfg = &GlobalConfig::get().sstable;

        // 验证配置
        cfg.validate()
            .map_err(|e| SSTableError::InvalidFormat(e.to_string()))?;

        // 使用 LocalFileSystem 打开文件
        let fs = LocalFileSystem;
        let writable_file = fs
            .open_write(path)
            .map_err(|e| SSTableError::Internal(format!("Failed to open file: {:?}", e)))?;

        // 创建 WritableFileWriter，使用合适的缓冲区大小
        let buffer_size = Some(cfg.block_size.max(64 * 1024)); // 至少 64KB
        let file = WritableFileWriter::new(writable_file, buffer_size);

        // 根据全局配置和注入的 filter_policy 创建 Filter
        let filter_builder: Option<Box<dyn FilterBlockBuilder>> = if cfg.filter_enabled {
            if let Some(ref policy) = ctx.filter_policy {
                let context = FilterBuildingContext::new(level);
                match cfg.filter_block_type {
                    FilterBlockType::Full => {
                        let bits_builder = policy.get_bits_builder(&context);
                        Some(Box::new(FullFilterBlockBuilder::new(bits_builder)))
                    }
                    FilterBlockType::Partitioned => {
                        let bits_builder = policy.get_bits_builder(&context);
                        let partition_size = ((cfg.metadata_block_size * 100) / 100) as u32;
                        let partition_size = partition_size.max(1);
                        Some(Box::new(PartitionedFilterBlockBuilder::new(
                            bits_builder,
                            partition_size,
                            cfg,
                        )))
                    }
                }
            } else {
                None
            }
        } else {
            None
        };

        Ok(Self {
            file,
            data_block_builder: DataBlockBuilder::new(DataBlockCodec, cfg.restart_interval),
            filter_builder,
            filter_policy: ctx.filter_policy.clone(),
            index_block_builder: IndexBlockBuilder::new(IndexBlockCodec, cfg.restart_interval),
            entry_count: 0,
            min_key: None,
            max_key: None,
            finished: false,
        })
    }

    /// 添加 Entry（核心方法）
    pub fn add(&mut self, entry: &Entry) -> Result<()> {
        if self.finished {
            return Err(SSTableError::Internal("Builder already finished".into()));
        }

        // 1. 检查 InternalKey 顺序（user_key 升序，同 user_key 时 sequence 降序）
        let current_internal_key = InternalKey::new(entry.key.clone(), entry.sequence);
        if let Some(ref last_key) = self.max_key {
            // 使用 InternalKey 的 Ord 比较（user_key 升序，sequence 降序）
            if &current_internal_key <= last_key {
                return Err(SSTableError::InvalidFormat(format!(
                    "Keys must be added in InternalKey order: last={:?} current={:?}",
                    last_key, current_internal_key
                )));
            }
        }

        // 2. 更新 min key（max_key 将在 flush 后、添加当前 entry 后更新）
        if self.min_key.is_none() {
            self.min_key = Some(entry.key.clone());
        }

        // 3. 检查当前 DataBlock 是否需要 flush
        let cfg = &GlobalConfig::get().sstable;
        let current_size = self.data_block_builder.estimated_size();
        if current_size >= cfg.block_size {
            self.flush_data_block()?;
        }

        // 4. 添加到 DataBlock
        let internal_key = InternalKey::new(entry.key.clone(), entry.sequence);
        let internal_value = entry.value.clone();

        self.data_block_builder
            .add(&internal_key, &internal_value)
            .map_err(|e| SSTableError::Internal(format!("Failed to add to data block: {:?}", e)))?;

        // 5. 添加到 Filter
        if let Some(ref mut filter) = self.filter_builder {
            let internal_key = InternalKey::new(entry.key.clone(), entry.sequence);
            filter.add(internal_key);
        }

        // 6. 更新状态
        self.max_key = Some(current_internal_key);
        self.entry_count += 1;

        Ok(())
    }

    /// 完成构建，返回元数据
    pub fn finish(mut self) -> Result<SSTableMetadata> {
        if self.finished {
            return Err(SSTableError::Internal("Already finished".into()));
        }

        // 1. Flush 最后一个 DataBlock
        self.flush_data_block()?;

        // 2. 写入 FilterBlock（如果有）
        let filter_handle = if let Some(mut filter) = self.filter_builder.take() {
            let mut last_partition_handle = BlockHandle::new(0, 0);

            // 循环处理 filter partitions
            loop {
                let filter_result = filter.finish(last_partition_handle).map_err(|e| {
                    SSTableError::Internal(format!("Failed to finish filter: {:?}", e))
                })?;

                match filter_result {
                    crate::sstable::filter::block::FinishResult::Incomplete(partition_data) => {
                        // PartitionedFilterBlockBuilder: 写入当前 partition，更新 handle 用于下一次调用
                        last_partition_handle =
                            self.write_raw_block(&partition_data, CompressionType::None)?;
                    }
                    crate::sstable::filter::block::FinishResult::Complete(final_data) => {
                        // FullFilterBlockBuilder: 直接返回完整的 filter 数据
                        // PartitionedFilterBlockBuilder: 返回 filter partition index
                        // 写入最终数据并返回 handle
                        let handle = self.write_raw_block(&final_data, CompressionType::None)?;
                        break handle;
                    }
                }
            }
        } else {
            BlockHandle::new(0, 0)
        };

        // 3. 构建 MetaIndex Block
        let mut meta_index_builder = MetaIndexBuilder::new(MetaIndexCodec, 1);

        // 3.1 添加 Filter Block 到 MetaIndex（如果有）
        let cfg = &GlobalConfig::get().sstable;
        if filter_handle.size > 0 {
            if let Some(ref policy) = self.filter_policy {
                let filter_key = crate::sstable::filter::factory::meta_key_for(
                    &policy.compatibility_name(),
                    cfg.filter_block_type,
                )?;

                let meta_index_key = MetaIndexKey::new(filter_key);
                meta_index_builder
                    .add(&meta_index_key, &filter_handle)
                    .map_err(|e| {
                        SSTableError::Internal(format!(
                            "Failed to add filter to meta index: {:?}",
                            e
                        ))
                    })?;
            }
        }

        // 3.2 完成 MetaIndex Block 构建
        let metaindex_handle = if meta_index_builder.is_empty() {
            // 空 MetaIndex：写入一个空的 block
            let empty_meta_index = Bytes::from_static(&[0, 0, 0, 0, 1, 0, 0, 0]);
            self.write_raw_block(&empty_meta_index, CompressionType::None)?
        } else {
            let meta_index_data = meta_index_builder.finish().map_err(|e| {
                SSTableError::Internal(format!("Failed to finish meta index block: {:?}", e))
            })?;

            // 3.3 写入 MetaIndex Block
            self.write_raw_block(&meta_index_data, CompressionType::None)?
        };

        // 4. 写入 IndexBlock
        let cfg = &GlobalConfig::get().sstable;
        let index_data = self.index_block_builder.finish().map_err(|e| {
            SSTableError::Internal(format!("Failed to finish index block: {:?}", e))
        })?;
        // Index Block 根据配置决定是否压缩
        let index_compression = if cfg.enable_index_compression {
            convert_compression_type(cfg.compression)
        } else {
            CompressionType::None
        };
        let index_handle = self.write_raw_block(&index_data, index_compression)?;

        // 5. 写入 Footer
        let footer = Footer::new(metaindex_handle, index_handle);
        let mut footer_buf = Vec::new();
        footer.encode(&mut footer_buf)?;
        self.file
            .append(&footer_buf)
            .map_err(|e| SSTableError::Internal(format!("Failed to write footer: {:?}", e)))?;

        // 6. 同步到磁盘
        self.file
            .sync()
            .map_err(|e| SSTableError::Internal(format!("Failed to sync file: {:?}", e)))?;

        // 获取最终文件大小
        let file_size = self.file.get_file_size();

        // 关闭文件
        self.file
            .close()
            .map_err(|e| SSTableError::Internal(format!("Failed to close file: {:?}", e)))?;

        self.finished = true;

        Ok(SSTableMetadata {
            file_size,
            entry_count: self.entry_count,
            min_key: self
                .min_key
                .ok_or_else(|| SSTableError::Internal("No entries added".into()))?,
            max_key: self
                .max_key
                .as_ref()
                .map(|ik| ik.user_key().clone())
                .ok_or_else(|| SSTableError::Internal("No entries added".into()))?,
        })
    }

    /// 刷新当前 Data Block（写入磁盘）
    fn flush_data_block(&mut self) -> Result<()> {
        if self.data_block_builder.is_empty() {
            return Ok(());
        }

        // 1. Finish DataBlock，获取未压缩数据
        let uncompressed_data = self
            .data_block_builder
            .finish()
            .map_err(|e| SSTableError::Internal(format!("Failed to finish data block: {:?}", e)))?;

        // 2. 压缩（如果启用）
        let cfg = &GlobalConfig::get().sstable;
        let comp_type = convert_compression_type(cfg.compression);
        let mut compressed_buf = BytesMut::new();
        compression::compress(uncompressed_data.clone(), comp_type, &mut compressed_buf)?;
        let compressed_data = compressed_buf.freeze();

        // 3. 计算 CRC32
        let crc = compression::compute_crc32c(&compressed_data);

        // 4. 构建 BlockTrailer
        let trailer = BlockTrailer::new(comp_type, crc);
        let mut trailer_buf = Vec::new();
        trailer.encode_to(&mut trailer_buf)?;

        // 5. 写入磁盘：[compressed_data][trailer]
        self.file.flush().map_err(|e| {
            SSTableError::Internal(format!("Failed to flush before writing block: {:?}", e))
        })?;

        let block_offset = self.file.get_file_size();

        self.file.append(&compressed_data).map_err(|e| {
            SSTableError::Internal(format!("Failed to write compressed data: {:?}", e))
        })?;
        self.file
            .append(&trailer_buf)
            .map_err(|e| SSTableError::Internal(format!("Failed to write trailer: {:?}", e)))?;

        let block_size = compressed_data.len() + trailer_buf.len();

        // 6. 记录 BlockHandle
        let handle = BlockHandle::new(block_offset, block_size as u64);

        // 7. 添加到 IndexBlock (使用 last_key 的 user_key 作为 separator)
        if let Some(ref last_key) = self.max_key {
            let index_key = IndexKey::new(last_key.user_key().clone());
            let index_value = handle;
            self.index_block_builder
                .add(&index_key, &index_value)
                .map_err(|e| {
                    SSTableError::Internal(format!("Failed to add to index block: {:?}", e))
                })?;
        }

        // 8. 重置 DataBlock Builder
        let cfg = &GlobalConfig::get().sstable;
        self.data_block_builder = DataBlockBuilder::new(DataBlockCodec, cfg.restart_interval);

        Ok(())
    }

    /// 写入原始 Block（用于 Filter/Index）
    fn write_raw_block(&mut self, data: &Bytes, comp_type: CompressionType) -> Result<BlockHandle> {
        let mut compressed_buf = BytesMut::new();
        compression::compress(data.clone(), comp_type, &mut compressed_buf)?;
        let compressed = compressed_buf.freeze();

        let crc = compression::compute_crc32c(&compressed);

        let trailer = BlockTrailer::new(comp_type, crc);
        let mut trailer_buf = Vec::new();
        trailer.encode_to(&mut trailer_buf)?;

        // 先 flush 确保之前的写入已完成，获取准确的 offset
        self.file.flush().map_err(|e| {
            SSTableError::Internal(format!("Failed to flush before writing raw block: {:?}", e))
        })?;

        let offset = self.file.get_file_size();

        self.file.append(&compressed).map_err(|e| {
            SSTableError::Internal(format!("Failed to write compressed data: {:?}", e))
        })?;
        self.file
            .append(&trailer_buf)
            .map_err(|e| SSTableError::Internal(format!("Failed to write trailer: {:?}", e)))?;

        let size = compressed.len() + trailer_buf.len();

        Ok(BlockHandle::new(offset, size as u64))
    }
}
