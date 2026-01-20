//! 哈希函数
//!
//! 提供 Bloom Filter 使用的非加密哈希实现，以及基于双哈希的多位置生成工具。

/// MurmurHash2 参数常量
const MURMUR_SEED: u32 = 0xbc9f1d34;
const MURMUR_M: u32 = 0x5bd1e995;
const MURMUR_R: u32 = 24;

/// MurmurHash2（32-bit）。
///
/// 主要用于兼容或测试场景；实现遵循 MurmurHash2 的经典流程。
pub fn murmur_hash2(data: &[u8], seed: u32) -> u32 {
    let len = data.len() as u32;
    let mut h = seed ^ len;

    let mut i = 0;
    while i + 4 <= data.len() {
        let mut k = u32::from_le_bytes([data[i], data[i + 1], data[i + 2], data[i + 3]]);

        k = k.wrapping_mul(MURMUR_M);
        k ^= k >> MURMUR_R;
        k = k.wrapping_mul(MURMUR_M);

        h = h.wrapping_mul(MURMUR_M);
        h ^= k;

        i += 4;
    }

    // 处理尾部不足 4 字节的数据
    match data.len() - i {
        3 => {
            h ^= (data[i + 2] as u32) << 16;
            h ^= (data[i + 1] as u32) << 8;
            h ^= data[i] as u32;
            h = h.wrapping_mul(MURMUR_M);
        }
        2 => {
            h ^= (data[i + 1] as u32) << 8;
            h ^= data[i] as u32;
            h = h.wrapping_mul(MURMUR_M);
        }
        1 => {
            h ^= data[i] as u32;
            h = h.wrapping_mul(MURMUR_M);
        }
        _ => {}
    }

    // 最终混合
    h ^= h >> 13;
    h = h.wrapping_mul(MURMUR_M);
    h ^= h >> 15;

    h
}

/// 双哈希拆分。
///
/// 将一个 64 位哈希拆分为 `(h1, h2)`，用于通过
/// `h(i) = h1 + i * h2` 生成多个 Bloom Filter 位置。
/// 确保 `h2` 为奇数，以避免过短的周期。
pub fn double_hash(h: u64, _bits: usize) -> (u64, u64) {
    let h1 = h & 0xFFFF_FFFF;
    let mut h2 = h >> 32;

    if h2.is_multiple_of(2) {
        h2 += 1;
    }

    (h1, h2)
}

/// XXH3（64-bit）。
///
/// 使用 `xxhash-rust` 提供的实现；当前 Bloom Filter 的主哈希函数。
pub fn xxh3_hash(data: &[u8]) -> u64 {
    xxhash_rust::xxh3::xxh3_64(data)
}

/// 计算 key 的 Bloom Filter 哈希值。
///
/// 目前直接使用 XXH3（64-bit）。
pub fn hash(key: &[u8]) -> u64 {
    xxh3_hash(key)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_murmur_hash2_empty() {
        let result = murmur_hash2(&[], 0);
        assert_eq!(result, 0);

        let result2 = murmur_hash2(&[], 1);
        assert_ne!(result2, 0);
    }

    #[test]
    fn test_murmur_hash2_consistency() {
        let data = b"hello world";
        let hash1 = murmur_hash2(data, 123);
        let hash2 = murmur_hash2(data, 123);
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_murmur_hash2_different_seeds() {
        let data = b"test data";
        let hash1 = murmur_hash2(data, 1);
        let hash2 = murmur_hash2(data, 2);
        assert_ne!(hash1, hash2);
    }

    #[test]
    fn test_murmur_hash2_different_data() {
        let hash1 = murmur_hash2(b"data1", 0);
        let hash2 = murmur_hash2(b"data2", 0);
        assert_ne!(hash1, hash2);
    }

    #[test]
    fn test_double_hash() {
        let h = 0x1234567890ABCDEFu64;
        let (h1, h2) = double_hash(h, 1000);

        assert_eq!(h1, 0x90ABCDEF);
        assert_eq!(h2, 0x12345679);
        assert!(h2 % 2 == 1);
    }

    #[test]
    fn test_double_hash_even_h2() {
        let h = 0x1234567812345678u64;
        let (_, h2) = double_hash(h, 1000);
        assert_eq!(h2, 0x12345679);
        assert!(h2 % 2 == 1);
    }

    #[test]
    fn test_xxh3_hash() {
        let data = b"hello world";
        let hash1 = xxh3_hash(data);
        let hash2 = xxh3_hash(data);
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_hash_consistency() {
        let data = b"test key";
        let hash1 = hash(data);
        let hash2 = hash(data);
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_double_hash_generate_positions() {
        let h = hash(b"test key");
        let (h1, h2) = double_hash(h, 1000);

        let mut positions = Vec::new();
        for i in 0..10 {
            let pos = (h1 + i * h2) % 1000;
            positions.push(pos);
        }

        let first = positions[0];
        assert!(positions.iter().any(|&x| x != first));
    }
}
