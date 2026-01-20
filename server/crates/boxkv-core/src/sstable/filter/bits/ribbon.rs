use crate::sstable::filter::FilterError;
use crate::sstable::filter::bits::builder::{FilterBitsResult, FilterMetadataParams};
use crate::sstable::filter::bits::{FilterBitsBuilder, FilterBitsReader};
use crate::sstable::filter::hash;
use bytes::Bytes;

/// RibbonFilterBitsBuilder
pub struct RibbonFilterBitsBuilder {
    /// 每个 key 使用的比特数
    bits_per_key: usize,

    /// 已添加的 keys
    keys: Vec<Bytes>,
}

impl RibbonFilterBitsBuilder {
    /// 创建新的 RibbonFilterBitsBuilder
    pub fn new(bits_per_key: usize) -> Self {
        Self {
            bits_per_key,
            keys: Vec::new(),
        }
    }

    /// 计算 Ribbon Filter 的 slot 数量
    fn calculate_num_slots(num_entries: usize, bits_per_key: usize) -> usize {
        if num_entries == 0 {
            return 0;
        }

        // 计算总比特数
        let total_bits = num_entries * bits_per_key;

        // Ribbon Filter 需要更多的 slots（约 1.3x）来保证可解性
        // 每个 slot 存储一个字节（8 bits）
        let slots = (total_bits as f64 * 1.3 / 8.0).ceil() as usize;

        // 确保至少有一个 slot，且是 kCoeffBits 的倍数
        // kCoeffBits = 16 (band_width)
        const K_COEFF_BITS: usize = 16;
        let min_slots = K_COEFF_BITS.max(1);
        let slots = slots.max(min_slots);

        // 向上取整到 128 的倍数
        const BLOCKSIZE: usize = 128;
        slots.div_ceil(BLOCKSIZE) * BLOCKSIZE
    }

    /// 计算带状矩阵的带宽
    fn calculate_band_width(_num_slots: usize) -> usize {
        16
    }

    /// 计算 trailing zero bits（用于找到第一个非零位）
    fn count_trailing_zero_bits(v: u64) -> usize {
        if v == 0 {
            return 64;
        }
        v.trailing_zeros() as usize
    }

    /// 计算 bit parity（XOR 所有位）
    fn bit_parity(v: u64) -> u8 {
        v.count_ones() as u8 & 1
    }

    /// 应用 seed 变换到 hash 值
    fn apply_seed_to_hash(hash: u64, seed: u8) -> u64 {
        if seed == 0 {
            hash
        } else {
            // 使用多个位移来增加混合效果
            let seed_u64 = seed as u64;
            hash ^ (seed_u64 << 56)
                ^ (seed_u64 << 48)
                ^ (seed_u64 << 40)
                ^ (seed_u64 << 32)
                ^ (seed_u64 << 24)
                ^ (seed_u64 << 16)
                ^ (seed_u64 << 8)
                ^ seed_u64
        }
    }

    /// 为 key 生成 start 位置和 coefficient row
    fn hash_to_ribbon_data(
        hash: u64,
        num_slots: usize,
        band_width: usize,
        seed: u8,
    ) -> (usize, u64, u8) {
        // 应用 seed 变换
        let seeded_hash = Self::apply_seed_to_hash(hash, seed);
        // 计算 start 位置（num_starts = num_slots - band_width + 1）
        let num_starts = num_slots.saturating_sub(band_width).saturating_add(1);
        let start = (seeded_hash % (num_starts as u64)) as usize;

        // 生成 coefficient row（r-bit 序列）
        // 使用哈希值生成一个 band_width 位的系数行
        // 确保第一个系数是 1（kFirstCoeffAlwaysOne = true）
        let mut coeff_row = seeded_hash;
        // 确保至少有一个非零位
        if coeff_row == 0 {
            coeff_row = 1;
        }
        // 确保第一个位是 1
        coeff_row |= 1;
        // 只保留 band_width 位
        let mask = if band_width >= 64 {
            u64::MAX
        } else {
            (1u64 << band_width) - 1
        };
        coeff_row &= mask;

        // 生成 result row（8 位，用于 filter）
        // 使用哈希值的高位生成 result
        let result_row =
            ((seeded_hash >> 32) ^ (seeded_hash >> 16) ^ (seeded_hash >> 8) ^ seeded_hash) as u8;

        (start, coeff_row, result_row)
    }

    /// On-the-fly 高斯消元法添加一个 entry
    fn banding_add(
        coeff_rows: &mut [u64],
        result_rows: &mut [u8],
        start: usize,
        mut coeff_row: u64,
        mut result_row: u8,
    ) -> bool {
        // kFirstCoeffAlwaysOne = true，所以不需要处理 trailing zeros
        // 直接从 start 开始
        let mut i = start;

        loop {
            // 确保第一个系数是 1
            assert_eq!(coeff_row & 1, 1);

            // 加载当前位置的数据
            let cr_at_i = coeff_rows[i];
            let rr_at_i = result_rows[i];

            if cr_at_i == 0 {
                // 空位置，直接存储
                coeff_rows[i] = coeff_row;
                result_rows[i] = result_row;
                return true;
            }

            // 确保已存储的系数行的第一个系数也是 1
            assert_eq!(cr_at_i & 1, 1);

            // 高斯行消元：XOR 操作
            coeff_row ^= cr_at_i;
            result_row ^= rr_at_i;

            if coeff_row == 0 {
                // 系数行变成 0，检查结果行
                // 如果 result_row == 0，说明是重复或冗余，成功
                // 否则失败
                break;
            }

            // 找到下一个非零系数位
            let tz = Self::count_trailing_zero_bits(coeff_row);
            i += tz;
            coeff_row >>= tz;

            // 确保不会越界
            if i >= coeff_rows.len() {
                return false;
            }
        }

        // 失败，除非 result_row == 0（重复或冗余）
        result_row == 0
    }

    /// 使用 on-the-fly 高斯消元法求解带状线性系统
    fn solve_band_system(
        num_slots: usize,
        band_width: usize,
        hashes: &[u64],
        seed: u8,
    ) -> Result<Vec<u8>, FilterError> {
        // 初始化 banding storage
        let mut coeff_rows = vec![0u64; num_slots];
        let mut result_rows = vec![0u8; num_slots];

        // 为每个 key 添加 entry（应用 seed 变换）
        for &hash in hashes {
            let (start, coeff_row, result_row) =
                Self::hash_to_ribbon_data(hash, num_slots, band_width, seed);

            if !Self::banding_add(
                &mut coeff_rows,
                &mut result_rows,
                start,
                coeff_row,
                result_row,
            ) {
                return Err(FilterError::EncodeError(format!(
                    "Ribbon filter banding failed with seed {}. This may indicate insufficient slots (num_slots={}, band_width={}) or hash collisions.",
                    seed, num_slots, band_width
                )));
            }
        }

        // 进行回代（back-substitution）生成解决方案
        const K_RESULT_BITS: usize = 8; // result_row 是 8 位（u8）
        let mut state = [0u64; K_RESULT_BITS]; // 每列一个 state

        let mut solution = vec![0u8; num_slots];

        // 从后往前处理每个 slot
        for i in (0..num_slots).rev() {
            let cr = coeff_rows[i];
            let rr = result_rows[i];

            // 计算 solution row
            let mut sr = 0u8;

            for j in 0..K_RESULT_BITS {
                // 计算下一个 solution bit 在 row i, column j
                let mut tmp = state[j] << 1;

                // bit = BitParity(tmp & cr) ^ ((rr >> j) & 1)
                let bit = (Self::bit_parity(tmp & cr) ^ ((rr >> j) & 1)) != 0;

                // 更新 tmp
                if bit {
                    tmp |= 1;
                }

                // 更新 state
                state[j] = tmp;

                // 添加到 solution row
                if bit {
                    sr |= 1 << j;
                }
            }

            solution[i] = sr;
        }

        // 验证解决方案
        for (idx, &hash) in hashes.iter().enumerate() {
            let (start, coeff_row, expected_result) =
                Self::hash_to_ribbon_data(hash, num_slots, band_width, seed);

            // 计算查询结果
            let mut result = 0u8;
            let mut cr_remaining = coeff_row;
            let mut offset = 0;
            while cr_remaining != 0 && (start + offset) < num_slots {
                if cr_remaining & 1 != 0 {
                    result ^= solution[start + offset];
                }
                cr_remaining >>= 1;
                offset += 1;
            }

            if result != expected_result {
                return Err(FilterError::EncodeError(format!(
                    "Ribbon filter solution verification failed for key at index {}: expected {}, got {}. This may indicate insufficient slots (num_slots={}, band_width={}) or hash collisions.",
                    idx, expected_result, result, num_slots, band_width
                )));
            }
        }

        Ok(solution)
    }
}

impl FilterBitsBuilder for RibbonFilterBitsBuilder {
    fn add_key(&mut self, key: Bytes) {
        // 保存 key
        if self.keys.is_empty() || self.keys.last() != Some(&key) {
            self.keys.push(key);
        }
    }

    fn finish(&mut self) -> Result<FilterBitsResult, FilterError> {
        let num_entries = self.keys.len();

        if num_entries == 0 {
            // 空过滤器
            return Ok(FilterBitsResult {
                filter_bits: Bytes::new(),
                metadata_params: FilterMetadataParams::Ribbon {
                    seed: 0,
                    num_blocks: 0,
                },
            });
        }

        // 计算 slot 数量
        let num_slots = Self::calculate_num_slots(num_entries, self.bits_per_key);
        let band_width = Self::calculate_band_width(num_slots);

        // 为每个 key 生成哈希值
        let keys = std::mem::take(&mut self.keys);
        let mut hashes = Vec::with_capacity(num_entries);

        for key in &keys {
            let hash = hash::hash(key);
            hashes.push(hash);
        }

        // 计算 entropy
        let entropy = if !hashes.is_empty() {
            // 使用所有 hash 的 XOR 作为 entropy
            hashes.iter().fold(0u64, |acc, &h| acc ^ h)
        } else {
            0
        };
        let starting_seed = (entropy & 0xFF) as u8;

        let mut slots = None;
        let mut final_seed = starting_seed;
        const MAX_SEED_TRIES: usize = 256;

        // 首先尝试使用原始 num_slots
        for _ in 0..MAX_SEED_TRIES {
            match Self::solve_band_system(num_slots, band_width, &hashes, final_seed) {
                Ok(s) => {
                    slots = Some(s);
                    break;
                }
                Err(_) => {
                    // 尝试下一个 seed（循环，0-255）
                    final_seed = final_seed.wrapping_add(1);
                }
            }
        }

        // 如果所有 seed 都失败，尝试增加 slot 数量重试（最多重试 3 次）
        if slots.is_none() {
            let mut current_slots = num_slots;
            let mut retry_count = 0;
            const MAX_RETRIES: usize = 3;

            loop {
                // 重新尝试所有 seed
                let mut current_seed = starting_seed;
                for _ in 0..MAX_SEED_TRIES {
                    match Self::solve_band_system(current_slots, band_width, &hashes, current_seed)
                    {
                        Ok(s) => {
                            slots = Some(s);
                            final_seed = current_seed;
                            break;
                        }
                        Err(_) => {
                            current_seed = current_seed.wrapping_add(1);
                        }
                    }
                }

                if slots.is_some() {
                    break;
                }

                if retry_count >= MAX_RETRIES {
                    return Err(FilterError::EncodeError(format!(
                        "Ribbon filter construction failed after trying {} seeds and {} slot retries. This may indicate insufficient slots (num_slots={}) or too many hash collisions.",
                        MAX_SEED_TRIES, MAX_RETRIES, current_slots
                    )));
                }

                // 如果求解失败，增加 50% 的 slots 重试
                // 确保是 128 的倍数（Standard128Ribbon 要求）
                current_slots = (current_slots as f64 * 1.5).ceil() as usize;
                const BLOCKSIZE: usize = 128;
                current_slots = current_slots.div_ceil(BLOCKSIZE) * BLOCKSIZE;
                retry_count += 1;
            }
        }

        // 解包 slots（此时一定不为 None）
        let slots = slots.expect("slots should be Some at this point");

        // 验证 slots 长度必须是 128 的倍数
        const BLOCKSIZE: usize = 128;
        if slots.len() % BLOCKSIZE != 0 {
            return Err(FilterError::EncodeError(format!(
                "Ribbon filter slots length {} is not a multiple of 128",
                slots.len()
            )));
        }

        // 计算 num_blocks = num_slots / 128 (每个 block 128 slots)
        let num_blocks = (slots.len() / BLOCKSIZE) as u32;

        // 确保 num_blocks >= 1（如果 slots.len() == 0，则 num_blocks = 0，这是允许的）
        if num_blocks == 0 && !slots.is_empty() {
            return Err(FilterError::EncodeError(format!(
                "Ribbon filter slots length {} is not a multiple of 128",
                slots.len()
            )));
        }

        // 返回纯 slots 数据（不包含 num_slots 和 band_width）
        Ok(FilterBitsResult {
            filter_bits: Bytes::from(slots),
            metadata_params: FilterMetadataParams::Ribbon {
                seed: final_seed,
                num_blocks,
            },
        })
    }

    fn estimate_entries_added(&self) -> usize {
        self.keys.len()
    }

    fn calculate_space(&self, num_entries: usize) -> usize {
        if num_entries == 0 {
            return 0; // 空过滤器，0 字节
        }

        // 只返回 slots 数据的大小（不包含 num_slots 和 band_width，它们在 metadata 中）
        Self::calculate_num_slots(num_entries, self.bits_per_key)
    }

    fn approximate_num_entries(&self, bytes: usize) -> usize {
        use crate::sstable::filter::FilterMetadata;

        // 去除 metadata 大小（5 字节）
        if bytes < FilterMetadata::SIZE {
            return 0;
        }
        let bytes_no_meta = bytes - FilterMetadata::SIZE;

        if bytes_no_meta == 0 || self.bits_per_key == 0 {
            return 0;
        }

        // 从 high 开始向下查找，直到找到满足条件的 n
        let total_bits = bytes_no_meta * 8;
        let effective_bits_per_key = (self.bits_per_key as f64 * 1.3) as usize;
        let high = if effective_bits_per_key > 0 {
            (total_bits / effective_bits_per_key).saturating_add(1)
        } else {
            0
        };
        let low = 1;

        // 从 high 向下查找，直到找到满足条件的 n
        let mut n = high.min(usize::MAX / 2);
        while n >= low {
            if self.calculate_space(n) <= bytes_no_meta {
                return n;
            }
            n = n.saturating_sub(1);
            if n == 0 {
                break;
            }
        }

        0
    }

    fn estimated_fp_rate(&self, num_entries: usize, bytes: usize) -> f64 {
        if num_entries == 0 || bytes == 0 {
            return 0.0;
        }

        if bytes < 5 {
            return 1.0;
        }

        let slots = bytes - 5;
        let bits = (slots * 8) as f64;

        let n = num_entries as f64;
        let m = bits;

        if m <= 0.0 {
            return 1.0;
        }

        let bits_per_key = m / n;
        let fp_rate = (1.0 - (-bits_per_key * 0.693).exp()).powf(1.2);

        fp_rate.min(1.0)
    }
}

/// RibbonFilterBitsReader
pub struct RibbonFilterBitsReader {
    /// Slot 数据
    slots: Bytes,

    /// Slot 数量
    num_slots: usize,

    /// 带宽
    band_width: usize,

    /// 哈希种子（用于哈希计算，确保与构建时一致）
    seed: u8,
}

impl RibbonFilterBitsReader {
    /// 从 filter bits 创建 RibbonFilterBitsReader
    /// # 格式
    /// ```text
    /// [slots: n bytes]
    /// ```
    pub fn new(data: &[u8], num_blocks: u32, seed: u8) -> Result<Self, FilterError> {
        if num_blocks == 0 {
            if data.is_empty() {
                // 空过滤器
                return Ok(Self {
                    slots: Bytes::new(),
                    num_slots: 0,
                    band_width: 16,
                    seed,
                });
            } else {
                return Err(FilterError::InvalidData(
                    "num_blocks is 0 but data is not empty".to_string(),
                ));
            }
        }

        // 计算 num_slots = num_blocks * 128
        let num_slots = (num_blocks as usize) * 128;

        // 验证数据长度（必须是 128 的倍数）
        if data.len() != num_slots {
            return Err(FilterError::InvalidData(format!(
                "Filter data length mismatch: expected {} (num_blocks={} * 128), got {}",
                num_slots,
                num_blocks,
                data.len()
            )));
        }

        // band_width 固定为 16
        const BAND_WIDTH: usize = 16;

        // 读取 slots 数据
        let slots = Bytes::copy_from_slice(data);

        Ok(Self {
            slots,
            num_slots,
            band_width: BAND_WIDTH,
            seed,
        })
    }

    /// 为 key 生成 start 位置和 coefficient row
    fn hash_to_ribbon_data(&self, hash: u64) -> (usize, u64, u8) {
        // 应用 seed 变换
        let seeded_hash = RibbonFilterBitsBuilder::apply_seed_to_hash(hash, self.seed);
        // 计算 start 位置（num_starts = num_slots - band_width + 1）
        let num_starts = self
            .num_slots
            .saturating_sub(self.band_width)
            .saturating_add(1);
        let start = (seeded_hash % (num_starts as u64)) as usize;

        // 生成 coefficient row（r-bit 序列）
        let mut coeff_row = seeded_hash;
        if coeff_row == 0 {
            coeff_row = 1;
        }
        coeff_row |= 1; // 确保第一个位是 1
        let mask = if self.band_width >= 64 {
            u64::MAX
        } else {
            (1u64 << self.band_width) - 1
        };
        coeff_row &= mask;

        // 生成 result row（8 位，用于 filter）
        let result_row =
            ((seeded_hash >> 32) ^ (seeded_hash >> 16) ^ (seeded_hash >> 8) ^ seeded_hash) as u8;

        (start, coeff_row, result_row)
    }
}

impl FilterBitsReader for RibbonFilterBitsReader {
    fn may_match(&self, entry: Bytes) -> bool {
        if self.slots.is_empty() {
            return true; // 空过滤器，保守返回 true
        }

        // 计算 entry 的哈希值
        let h = hash::hash(&entry);

        // 计算 start、coeff_row 和 expected_result
        let (start, coeff_row, expected_result) = self.hash_to_ribbon_data(h);

        // 计算所有受影响 slots 的 XOR 结果
        let mut result = 0u8;
        let mut cr_remaining = coeff_row;
        let mut offset = 0;

        while cr_remaining != 0 && (start + offset) < self.slots.len() {
            if cr_remaining & 1 != 0 {
                result ^= self.slots[start + offset];
            }
            cr_remaining >>= 1;
            offset += 1;
        }

        // 如果 XOR 结果等于期望值，则 key 可能存在
        result == expected_result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ribbon_filter_bits_builder_new() {
        let builder = RibbonFilterBitsBuilder::new(10);
        assert_eq!(builder.bits_per_key, 10);
        assert_eq!(builder.keys.len(), 0);
    }

    #[test]
    fn test_ribbon_filter_bits_builder_add_key() {
        let mut builder = RibbonFilterBitsBuilder::new(10);
        builder.add_key(Bytes::from("hello"));
        builder.add_key(Bytes::from("world"));

        assert_eq!(builder.estimate_entries_added(), 2);
    }

    #[test]
    fn test_ribbon_filter_bits_builder_finish() {
        let mut builder = RibbonFilterBitsBuilder::new(10);
        builder.add_key(Bytes::from("hello"));
        builder.add_key(Bytes::from("world"));

        let result = builder.finish();
        assert!(result.is_ok());

        let result = result.unwrap();
        // filter_bits 应该包含 slots 数据
        assert!(!result.filter_bits.is_empty());
        // metadata_params 应该包含 seed 和 num_blocks
        match result.metadata_params {
            crate::sstable::filter::bits::builder::FilterMetadataParams::Ribbon {
                seed: _,
                num_blocks,
            } => {
                assert!(num_blocks > 0);
            }
            _ => panic!("Expected Ribbon metadata params"),
        }
    }

    #[test]
    fn test_ribbon_filter_bits_builder_empty() {
        let mut builder = RibbonFilterBitsBuilder::new(10);
        let result = builder.finish();
        assert!(result.is_ok());

        let result = result.unwrap();
        // 空过滤器：filter_bits 为空
        assert!(result.filter_bits.is_empty());
        // metadata_params 应该包含 seed=0, num_blocks=0
        match result.metadata_params {
            crate::sstable::filter::bits::builder::FilterMetadataParams::Ribbon {
                seed,
                num_blocks,
            } => {
                assert_eq!(seed, 0);
                assert_eq!(num_blocks, 0);
            }
            _ => panic!("Expected Ribbon metadata params"),
        }
    }

    #[test]
    fn test_ribbon_filter_bits_reader_new() {
        // 创建一个简单的 filter
        let mut builder = RibbonFilterBitsBuilder::new(10);
        builder.add_key(Bytes::from("test"));
        let result = builder.finish().unwrap();

        // 从 metadata_params 获取 seed 和 num_blocks
        let (seed, num_blocks) = match result.metadata_params {
            crate::sstable::filter::bits::builder::FilterMetadataParams::Ribbon {
                seed,
                num_blocks,
            } => (seed, num_blocks),
            _ => panic!("Expected Ribbon metadata params"),
        };

        // 解码
        let reader = RibbonFilterBitsReader::new(&result.filter_bits, num_blocks, seed);
        assert!(reader.is_ok());
    }

    #[test]
    fn test_ribbon_filter_bits_reader_may_match() {
        // 创建 filter
        let mut builder = RibbonFilterBitsBuilder::new(10);
        builder.add_key(Bytes::from("hello"));
        builder.add_key(Bytes::from("world"));
        let result = builder.finish().unwrap();

        // 从 metadata_params 获取 seed 和 num_blocks
        let (seed, num_blocks) = match result.metadata_params {
            crate::sstable::filter::bits::builder::FilterMetadataParams::Ribbon {
                seed,
                num_blocks,
            } => (seed, num_blocks),
            _ => panic!("Expected Ribbon metadata params"),
        };

        // 创建 reader
        let reader = RibbonFilterBitsReader::new(&result.filter_bits, num_blocks, seed).unwrap();

        // 查询存在的 keys
        assert!(reader.may_match(Bytes::from("hello")));
        assert!(reader.may_match(Bytes::from("world")));
    }

    #[test]
    fn test_ribbon_filter_roundtrip() {
        // 创建 filter
        let mut builder = RibbonFilterBitsBuilder::new(10);
        builder.add_key(Bytes::from("key1"));
        builder.add_key(Bytes::from("key2"));
        builder.add_key(Bytes::from("key3"));
        let result = builder.finish().unwrap();

        // 从 metadata_params 获取 seed 和 num_blocks
        let (seed, num_blocks) = match result.metadata_params {
            crate::sstable::filter::bits::builder::FilterMetadataParams::Ribbon {
                seed,
                num_blocks,
            } => (seed, num_blocks),
            _ => panic!("Expected Ribbon metadata params"),
        };

        // 解码
        let reader = RibbonFilterBitsReader::new(&result.filter_bits, num_blocks, seed).unwrap();

        // 验证所有添加的 keys 都能找到
        assert!(reader.may_match(Bytes::from("key1")));
        assert!(reader.may_match(Bytes::from("key2")));
        assert!(reader.may_match(Bytes::from("key3")));
    }

    #[test]
    fn test_calculate_space() {
        let builder = RibbonFilterBitsBuilder::new(10);

        // 100 个 entries
        let space = builder.calculate_space(100);
        assert!(space > 0); // slots 数据

        // 空 entries
        let space_empty = builder.calculate_space(0);
        assert_eq!(space_empty, 0); // 空过滤器，0 字节
    }
}
