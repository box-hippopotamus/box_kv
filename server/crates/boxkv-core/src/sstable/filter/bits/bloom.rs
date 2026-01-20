use crate::sstable::filter::FilterError;
use crate::sstable::filter::bits::builder::{FilterBitsResult, FilterMetadataParams};
use crate::sstable::filter::bits::{FilterBitsBuilder, FilterBitsReader};
use crate::sstable::filter::hash;
use bytes::Bytes;

/// BloomFilterBitsBuilder
pub struct BloomFilterBitsBuilder {
    /// 每个 key 使用的比特数
    bits_per_key: usize,

    /// 已添加的 keys 的哈希值
    hash_entries: Vec<u64>,
}

impl BloomFilterBitsBuilder {
    /// 创建新的 BloomFilterBitsBuilder
    pub fn new(bits_per_key: usize) -> Self {
        Self {
            bits_per_key,
            hash_entries: Vec::new(),
        }
    }

    /// 计算最优的哈希函数数量
    pub fn calculate_k(bits_per_key: usize) -> u32 {
        // k = bits_per_key * ln(2)
        // ln(2) ≈ 0.69
        let k = (bits_per_key as f64 * 0.69) as u32;
        k.clamp(1, 30)
    }
}

impl FilterBitsBuilder for BloomFilterBitsBuilder {
    fn add_key(&mut self, key: Bytes) {
        let h = hash::hash(&key);

        if self.hash_entries.is_empty() || self.hash_entries.last() != Some(&h) {
            self.hash_entries.push(h);
        }
    }

    fn finish(&mut self) -> Result<FilterBitsResult, FilterError> {
        let num_entries = self.hash_entries.len();

        let k = Self::calculate_k(self.bits_per_key);
        if k > 30 {
            return Err(FilterError::EncodeError(format!(
                "k value {} too large (max 30)",
                k
            )));
        }
        let num_probes = k as u8;

        if num_entries == 0 {
            return Ok(FilterBitsResult {
                filter_bits: Bytes::new(),
                metadata_params: FilterMetadataParams::Bloom { num_probes },
            });
        }

        let total_bits = num_entries * self.bits_per_key;
        let bytes_needed = total_bits.div_ceil(8);
        let bits_len = bytes_needed * 8;

        // 现在才分配位数组
        let mut bits = vec![0u8; bytes_needed.max(1)];

        // 处理所有哈希值
        let hash_entries = std::mem::take(&mut self.hash_entries);
        for h in &hash_entries {
            // 使用 double_hash 生成两个哈希值
            let (h1, h2) = hash::double_hash(*h, bits_len);

            // 生成 k 个哈希位置
            for i in 0..k {
                // hash_i = (h1 + i * h2) % bits_len
                let bit_pos = ((h1 + (i as u64) * h2) % (bits_len as u64)) as usize;
                let byte_index = bit_pos / 8;
                let bit_index = bit_pos % 8;
                if byte_index < bits.len() {
                    bits[byte_index] |= 1 << bit_index;
                }
            }
        }

        // 返回纯 bits_data（不包含 k 值）
        Ok(FilterBitsResult {
            filter_bits: Bytes::from(bits),
            metadata_params: FilterMetadataParams::Bloom { num_probes },
        })
    }

    fn estimate_entries_added(&self) -> usize {
        self.hash_entries.len()
    }

    fn calculate_space(&self, num_entries: usize) -> usize {
        if num_entries == 0 {
            return 0; // 空过滤器，0 字节
        }

        let total_bits = num_entries * self.bits_per_key;
        total_bits.div_ceil(8) // 不包含 k 值，k 值在 metadata 中
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

        // 使用二分查找
        // 从 high = (total_bits / bits_per_key) + 1 开始向下查找
        let total_bits = bytes_no_meta * 8;
        let high = (total_bits / self.bits_per_key).saturating_add(1);
        let low = 1;

        // 从 high 向下查找，直到找到满足条件的 n
        let mut n = high.min(usize::MAX / 2); // 避免溢出
        while n >= low {
            if self.calculate_space(n) <= bytes_no_meta {
                return n;
            }
            n = n.saturating_sub(1);
            if n == 0 {
                break;
            }
        }

        // 如果找不到，返回 0
        0
    }

    fn estimated_fp_rate(&self, num_entries: usize, bytes: usize) -> f64 {
        if num_entries == 0 || bytes == 0 {
            return 0.0;
        }

        // 实际 bits = (bytes - 1) * 8（减去 k 值占用的 1 字节）
        let bits = ((bytes - 1) * 8) as f64;

        // 计算 k 值
        let k = Self::calculate_k(self.bits_per_key) as f64;

        // 误判率公式：f = (1 - e^(-k*n/m))^k
        // 其中 k 是哈希函数数量，n 是 entries 数量，m 是 bits 数量
        let n = num_entries as f64;
        let m = bits;

        // 避免数值问题
        if m <= 0.0 || k <= 0.0 {
            return 1.0;
        }

        let exponent = -k * n / m;
        let base = 1.0 - exponent.exp();
        base.powf(k)
    }
}

/// BloomFilterBitsReader
///
/// Bloom Filter 的 FilterBitsReader 实现
pub struct BloomFilterBitsReader {
    /// 位数组
    bits: Bytes,

    /// 哈希函数数量
    k: u32,

    /// 位数组大小（bits）
    bits_len: usize,
}

impl BloomFilterBitsReader {
    /// 从 filter bits 创建 BloomFilterBitsReader
    ///
    /// # 参数
    /// - `data`: filter bits 数据（纯位数组，不包含 k 值）
    /// - `num_probes`: 哈希函数数量（k值，从 metadata 中获取）
    ///
    /// # 格式
    /// ```text
    /// [bits_data: n bytes]
    /// ```
    ///
    /// # 返回
    /// - BloomFilterBitsReader 实例
    pub fn new(data: &[u8], num_probes: u8) -> Result<Self, FilterError> {
        if num_probes == 0 || num_probes > 30 {
            return Err(FilterError::InvalidData(format!(
                "Invalid num_probes: {}",
                num_probes
            )));
        }

        let bits = Bytes::copy_from_slice(data);
        let bits_len = if bits.is_empty() { 0 } else { bits.len() * 8 };

        Ok(Self {
            bits,
            k: num_probes as u32,
            bits_len,
        })
    }

    /// 检查位是否被设置
    ///
    /// # 参数
    /// - `bit_pos`: 位位置
    ///
    /// # 返回
    /// - 是否被设置
    fn get_bit(&self, bit_pos: usize) -> bool {
        let byte_index = bit_pos / 8;
        let bit_index = bit_pos % 8;

        if byte_index < self.bits.len() {
            (self.bits[byte_index] & (1 << bit_index)) != 0
        } else {
            false
        }
    }
}

impl FilterBitsReader for BloomFilterBitsReader {
    fn may_match(&self, entry: Bytes) -> bool {
        if self.bits.is_empty() {
            return true; // 空过滤器，保守返回 true
        }

        // 计算 entry 的哈希值（64 位）
        let h = hash::hash(&entry);

        // 使用 double_hash 生成两个哈希值
        let (h1, h2) = hash::double_hash(h, self.bits_len);

        // 检查所有 k 个哈希位置
        for i in 0..self.k {
            // hash_i = (h1 + i * h2) % bits_len
            let bit_pos = ((h1 + (i as u64) * h2) % (self.bits_len as u64)) as usize;

            if !self.get_bit(bit_pos) {
                return false; // 有任何一个 bit 为 0，则一定不存在
            }
        }

        true // 所有 bit 都为 1，可能存在
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculate_k() {
        assert_eq!(BloomFilterBitsBuilder::calculate_k(10), 6); // 10 * 0.69 = 6.9 ≈ 6
        assert_eq!(BloomFilterBitsBuilder::calculate_k(8), 5);
        assert_eq!(BloomFilterBitsBuilder::calculate_k(1), 1); // 最小值
        assert_eq!(BloomFilterBitsBuilder::calculate_k(50), 30); // 最大值限制
    }

    #[test]
    fn test_bloom_filter_bits_builder_new() {
        let builder = BloomFilterBitsBuilder::new(10);
        assert_eq!(builder.bits_per_key, 10);
        assert_eq!(builder.hash_entries.len(), 0);
    }

    #[test]
    fn test_bloom_filter_bits_builder_add_key() {
        let mut builder = BloomFilterBitsBuilder::new(10);
        builder.add_key(Bytes::from("hello"));
        builder.add_key(Bytes::from("world"));

        assert_eq!(builder.estimate_entries_added(), 2);
    }

    #[test]
    fn test_bloom_filter_bits_builder_finish() {
        let mut builder = BloomFilterBitsBuilder::new(10);
        builder.add_key(Bytes::from("hello"));
        builder.add_key(Bytes::from("world"));

        let result = builder.finish();
        assert!(result.is_ok());

        let result = result.unwrap();
        // filter_bits 应该包含位数组数据
        assert!(!result.filter_bits.is_empty());
        // metadata_params 应该包含 num_probes
        match result.metadata_params {
            crate::sstable::filter::bits::builder::FilterMetadataParams::Bloom { num_probes } => {
                assert!(num_probes >= 1 && num_probes <= 30);
            }
            _ => panic!("Expected Bloom metadata params"),
        }
    }

    #[test]
    fn test_bloom_filter_bits_builder_calculate_space() {
        let builder = BloomFilterBitsBuilder::new(10);

        // 100 个 entries，每个 10 bits = 1000 bits = 125 bytes（不包含 k，k 在 metadata 中）
        let space = builder.calculate_space(100);
        assert!(space >= 125);

        // 空 entries
        let space_empty = builder.calculate_space(0);
        assert_eq!(space_empty, 0); // 空过滤器，0 字节
    }

    #[test]
    fn test_bloom_filter_bits_reader_new() {
        // 创建一个简单的 filter
        let mut builder = BloomFilterBitsBuilder::new(10);
        builder.add_key(Bytes::from("test"));
        let result = builder.finish().unwrap();

        // 从 metadata_params 获取 num_probes
        let num_probes = match result.metadata_params {
            crate::sstable::filter::bits::builder::FilterMetadataParams::Bloom { num_probes } => {
                num_probes
            }
            _ => panic!("Expected Bloom metadata params"),
        };

        // 解码（使用 filter_bits 和 num_probes）
        let reader = BloomFilterBitsReader::new(&result.filter_bits, num_probes);
        assert!(reader.is_ok());
    }

    #[test]
    fn test_bloom_filter_bits_reader_may_match() {
        // 创建 filter
        let mut builder = BloomFilterBitsBuilder::new(10);
        builder.add_key(Bytes::from("hello"));
        builder.add_key(Bytes::from("world"));
        let result = builder.finish().unwrap();

        // 从 metadata_params 获取 num_probes
        let num_probes = match result.metadata_params {
            crate::sstable::filter::bits::builder::FilterMetadataParams::Bloom { num_probes } => {
                num_probes
            }
            _ => panic!("Expected Bloom metadata params"),
        };

        // 创建 reader
        let reader = BloomFilterBitsReader::new(&result.filter_bits, num_probes).unwrap();

        // 查询存在的 keys
        assert!(reader.may_match(Bytes::from("hello")));
        assert!(reader.may_match(Bytes::from("world")));

        // 查询不存在的 key（可能有误判，但不会漏判）
        // 这里不检查 false positive，因为可能有误判
    }

    #[test]
    fn test_bloom_filter_bits_reader_empty_filter() {
        // 空 filter（空的 filter_bits，num_probes = 6）
        let empty_data = vec![]; // 空的 filter_bits
        let num_probes = 6;
        let reader = BloomFilterBitsReader::new(&empty_data, num_probes).unwrap();

        // 空过滤器应该返回 true（保守策略）
        assert!(reader.may_match(Bytes::from("anything")));
    }

    #[test]
    fn test_bloom_filter_bits_reader_invalid_data() {
        // num_probes 为 0
        assert!(BloomFilterBitsReader::new(&[], 0).is_err());

        // num_probes 过大
        assert!(BloomFilterBitsReader::new(&[], 31).is_err());

        // 正常情况（空 filter_bits 但 num_probes 有效）
        assert!(BloomFilterBitsReader::new(&[], 6).is_ok());
    }

    #[test]
    fn test_bloom_filter_roundtrip() {
        // 创建 filter
        let mut builder = BloomFilterBitsBuilder::new(10);
        builder.add_key(Bytes::from("key1"));
        builder.add_key(Bytes::from("key2"));
        builder.add_key(Bytes::from("key3"));
        let result = builder.finish().unwrap();

        // 从 metadata_params 获取 num_probes
        let num_probes = match result.metadata_params {
            crate::sstable::filter::bits::builder::FilterMetadataParams::Bloom { num_probes } => {
                num_probes
            }
            _ => panic!("Expected Bloom metadata params"),
        };

        // 解码
        let reader = BloomFilterBitsReader::new(&result.filter_bits, num_probes).unwrap();

        // 验证所有添加的 keys 都能找到
        assert!(reader.may_match(Bytes::from("key1")));
        assert!(reader.may_match(Bytes::from("key2")));
        assert!(reader.may_match(Bytes::from("key3")));
    }

    #[test]
    fn test_estimated_fp_rate() {
        let builder = BloomFilterBitsBuilder::new(10);

        // 1000 个 entries，10 bits/key，理论误判率约 1%
        let fp_rate = builder.estimated_fp_rate(1000, 1250 + 1); // 1000 * 10 / 8 + 1
        assert!(fp_rate > 0.0);
        assert!(fp_rate < 0.02); // 应该在 2% 以内
    }
}
