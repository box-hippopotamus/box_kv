use crate::sstable::{FOOTER_SIZE, MAGIC, MAGIC_SIZE, Result, SSTableError};

/// Represents the location and size of a block within an SSTable file.
///
/// A `BlockHandle` is used to index blocks (Data Blocks, Index Blocks, Filter Blocks)
/// by storing their file offset and size. This allows efficient random access to
/// specific blocks without reading the entire file.
///
/// # Encoding Format
/// Both `offset` and `size` are encoded as variable-length integers (varint) to
/// minimize storage overhead for small values.
///
/// # Examples
/// ```ignore
/// let handle = BlockHandle::new(1024, 4096);
/// let encoded = handle.encode();
/// let (decoded, bytes_read) = BlockHandle::decode(&encoded)?;
/// assert_eq!(handle, decoded);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockHandle {
    /// File offset where the block starts (in bytes from the beginning of the file).
    pub offset: u64,
    /// Size of the block in bytes.
    pub size: u64,
}

impl BlockHandle {
    /// Creates a new `BlockHandle` with the specified offset and size.
    ///
    /// # Arguments
    /// * `offset` - File offset in bytes where the block starts
    /// * `size` - Size of the block in bytes
    pub fn new(offset: u64, size: u64) -> Self {
        Self { offset, size }
    }

    /// Encodes the `BlockHandle` into a byte vector using variable-length integer encoding.
    ///
    /// # Format
    /// The encoding consists of two varint-encoded values:
    /// 1. `offset` (varint)
    /// 2. `size` (varint)
    ///
    /// # Returns
    /// A `Vec<u8>` containing the encoded bytes. The length is variable and depends
    /// on the magnitude of `offset` and `size`.
    ///
    /// # Examples
    /// ```ignore
    /// let handle = BlockHandle::new(100, 200);
    /// let encoded = handle.encode();
    /// assert_eq!(encoded.len(), handle.encoded_size());
    /// ```
    pub fn encode(&self) -> Vec<u8> {
        let cap = self.encoded_size();
        let mut buf = Vec::with_capacity(cap);

        varint::encode(self.offset, &mut buf);
        varint::encode(self.size, &mut buf);

        buf
    }

    /// Decodes a `BlockHandle` from a byte slice.
    ///
    /// # Arguments
    /// * `data` - Byte slice containing the encoded `BlockHandle`
    ///
    /// # Returns
    /// A tuple containing:
    /// - `BlockHandle` - The decoded handle
    /// - `usize` - Number of bytes consumed from the input slice
    ///
    /// # Errors
    /// Returns `SSTableError::Decode` if:
    /// - The input data is incomplete (truncated varint)
    /// - The varint encoding is invalid or exceeds 64 bits
    ///
    /// # Examples
    /// ```ignore
    /// let handle = BlockHandle::new(100, 200);
    /// let encoded = handle.encode();
    /// let (decoded, bytes_read) = BlockHandle::decode(&encoded)?;
    /// assert_eq!(handle, decoded);
    /// assert_eq!(bytes_read, encoded.len());
    /// ```
    pub fn decode(data: &[u8]) -> Result<(Self, usize)> {
        let (offset, offset_read) = varint::decode(data)?;
        let (size, size_read) = varint::decode(&data[offset_read..])?;
        Ok((Self { offset, size }, offset_read + size_read))
    }

    /// Returns the total number of bytes required to encode this `BlockHandle`.
    ///
    /// This is the sum of the varint-encoded sizes of `offset` and `size`.
    ///
    /// # Returns
    /// The encoded size in bytes (always between 2 and 20 bytes for valid u64 values).
    ///
    /// # Examples
    /// ```ignore
    /// let handle = BlockHandle::new(100, 200);
    /// let size = handle.encoded_size();
    /// let encoded = handle.encode();
    /// assert_eq!(size, encoded.len());
    /// ```
    pub fn encoded_size(&self) -> usize {
        varint::encoded_size(self.offset) + varint::encoded_size(self.size)
    }
}

/// Footer block that stores metadata index locations at the end of an SSTable file.
///
/// The Footer is a fixed-size (48 bytes) structure written at the end of every SSTable file.
/// It contains pointers to the Meta Index Block and Index Block, which are essential
/// for locating data blocks during reads.
///
/// # File Layout
/// The Footer is always the last `FOOTER_SIZE` (48) bytes of an SSTable file:
/// ```text
/// [meta_index_handle (varint)][index_handle (varint)][padding][magic (8 bytes)]
/// ```
///
/// # Magic Number
/// The magic number (`MAGIC`) serves as a file format identifier and corruption
/// detection mechanism. It is stored as the last 8 bytes in big-endian format.
///
/// # Examples
/// ```ignore
/// let meta_handle = BlockHandle::new(100, 200);
/// let index_handle = BlockHandle::new(300, 400);
/// let footer = Footer::new(meta_handle, index_handle);
///
/// let mut buf = [0u8; FOOTER_SIZE];
/// footer.encode(&mut buf);
///
/// let decoded = Footer::decode(&buf)?;
/// assert_eq!(footer, decoded);
/// assert!(decoded.validate_magic());
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Footer {
    /// BlockHandle pointing to the Meta Index Block (contains Filter Block index).
    pub meta_index_handle: BlockHandle,
    /// BlockHandle pointing to the Index Block (contains Data Block indices).
    pub index_handle: BlockHandle,
    /// Magic number for file format validation (stored as u64 in big-endian).
    pub magic: u64,
}

impl Footer {
    /// Creates a new `Footer` with the specified block handles.
    ///
    /// The magic number is automatically set to the current format's `MAGIC` constant.
    ///
    /// # Arguments
    /// * `meta_index` - BlockHandle for the Meta Index Block
    /// * `index` - BlockHandle for the Index Block
    pub fn new(meta_index: BlockHandle, index: BlockHandle) -> Self {
        Self {
            meta_index_handle: meta_index,
            index_handle: index,
            magic: crate::sstable::MAGIC,
        }
    }

    /// Encodes the Footer into a fixed-size 48-byte buffer.
    ///
    /// This method writes directly into the provided buffer to avoid unnecessary
    /// allocations. The buffer must be exactly `FOOTER_SIZE` bytes.
    ///
    /// # Arguments
    /// * `dst` - A mutable reference to a fixed-size array of `FOOTER_SIZE` (48) bytes
    ///
    /// # Layout
    /// The encoded footer has the following structure:
    /// ```text
    /// +----------------------+-------------------+----------+------------------+
    /// | meta_index_handle    | index_handle      | padding  | magic (8 bytes)  |
    /// | (varint, variable)   | (varint, variable)| (zeros)  | (big-endian u64)|
    /// +----------------------+-------------------+----------+------------------+
    /// ```
    ///
    /// The padding ensures the magic number always starts at a fixed offset from
    /// the end of the file, allowing efficient footer reading.
    ///
    /// # Panics
    /// This function will panic if the combined size of the encoded handles plus
    /// the magic number exceeds `FOOTER_SIZE`. In practice, this should never
    /// happen with reasonable file sizes (varints are very compact).
    ///
    /// # Examples
    /// ```ignore
    /// let footer = Footer::new(
    ///     BlockHandle::new(100, 200),
    ///     BlockHandle::new(300, 400)
    /// );
    /// let mut buf = [0u8; FOOTER_SIZE];
    /// footer.encode(&mut buf);
    /// // buf now contains the encoded footer
    /// ```
    pub fn encode(&self, dst: &mut [u8; FOOTER_SIZE]) {
        let meta_index_size = self.meta_index_handle.encoded_size();
        let index_size = self.index_handle.encoded_size();
        let (meta_index_buf, rest) = dst.split_at_mut(meta_index_size);
        let (index_buf, rest) = rest.split_at_mut(index_size);
        let padding_size = FOOTER_SIZE - (meta_index_size + index_size + MAGIC_SIZE);
        let (padding_buf, magic_buf) = rest.split_at_mut(padding_size);

        let meta_index_encode = self.meta_index_handle.encode();
        meta_index_buf[..].copy_from_slice(&meta_index_encode[..]);

        let index_encode = self.index_handle.encode();
        index_buf[..].copy_from_slice(&index_encode[..]);

        padding_buf.fill(0);
        magic_buf[..].copy_from_slice(&MAGIC.to_be_bytes());
    }

    /// Decodes a `Footer` from a fixed-size byte array.
    ///
    /// This method reads the footer structure from the last `FOOTER_SIZE` bytes
    /// of an SSTable file. The input must be exactly 48 bytes.
    ///
    /// # Arguments
    /// * `data` - A fixed-size array of exactly `FOOTER_SIZE` (48) bytes containing
    ///            the encoded footer
    ///
    /// # Returns
    /// A decoded `Footer` structure containing the block handles and magic number.
    ///
    /// # Errors
    /// Returns `SSTableError::Decode` if:
    /// - The varint encoding for block handles is invalid or truncated
    /// - The magic number extraction fails (should not happen with correct size)
    ///
    /// Returns `SSTableError::Corrupted` if:
    /// - The magic number slice cannot be extracted (internal error)
    ///
    /// # Examples
    /// ```ignore
    /// let footer = Footer::new(
    ///     BlockHandle::new(100, 200),
    ///     BlockHandle::new(300, 400)
    /// );
    /// let mut buf = [0u8; FOOTER_SIZE];
    /// footer.encode(&mut buf);
    ///
    /// let decoded = Footer::decode(&buf)?;
    /// assert_eq!(footer, decoded);
    /// ```
    pub fn decode(data: &[u8; FOOTER_SIZE]) -> Result<Self> {
        let (meta_index_handle, meta_read) = BlockHandle::decode(data)?;
        let (index_handle, _index_read) = BlockHandle::decode(&data[meta_read..])?;

        // Safely extract magic bytes from the end of the footer
        let magic_bytes: [u8; MAGIC_SIZE] =
            data[FOOTER_SIZE - MAGIC_SIZE..].try_into().map_err(|_| {
                SSTableError::Corrupted(format!(
                    "Invalid footer magic size: expected {}, got {}",
                    MAGIC_SIZE,
                    data[FOOTER_SIZE - MAGIC_SIZE..].len()
                ))
            })?;
        let magic = u64::from_be_bytes(magic_bytes);

        Ok(Self {
            meta_index_handle,
            index_handle,
            magic,
        })
    }

    /// Validates that the magic number matches the expected format identifier.
    ///
    /// This is used to detect file format mismatches or corruption. The magic number
    /// should always match `MAGIC` for valid SSTable files.
    ///
    /// # Returns
    /// * `true` - Magic number matches (file format is valid)
    /// * `false` - Magic number mismatch (file may be corrupted or wrong format)
    ///
    /// # Examples
    /// ```ignore
    /// let footer = Footer::new(handle1, handle2);
    /// assert!(footer.validate_magic());
    ///
    /// // Corrupted footer
    /// let mut corrupted = footer.clone();
    /// corrupted.magic = 0;
    /// assert!(!corrupted.validate_magic());
    /// ```
    pub fn validate_magic(&self) -> bool {
        self.magic == MAGIC
    }
}

/// Variable-length integer encoding (Varint) for compact serialization.
///
/// Varint encoding uses a variable number of bytes to represent integers, with
/// smaller values requiring fewer bytes. This is particularly efficient for
/// storing file offsets and sizes, which are often small in practice.
///
/// # Encoding Format
/// Each byte uses 7 bits for data and 1 bit as a continuation flag:
/// - Bit 7 (MSB) = 1: More bytes follow
/// - Bit 7 (MSB) = 0: This is the last byte
/// - Bits 0-6: Data bits
///
/// # Examples
/// - Value 127 (0x7F): Encoded as `[0x7F]` (1 byte)
/// - Value 128 (0x80): Encoded as `[0x80, 0x01]` (2 bytes)
/// - Value 300: Encoded as `[0xAC, 0x02]` (2 bytes)
///
/// # Performance
/// - Small values (< 128): 1 byte
/// - Medium values (< 16384): 2 bytes
/// - Large values (u64::MAX): Up to 10 bytes
pub mod varint {
    pub use super::*;

    /// Encodes a `u64` value as a varint and appends it to the buffer.
    ///
    /// # Arguments
    /// * `value` - The 64-bit unsigned integer to encode
    /// * `buf` - Mutable buffer to append the encoded bytes to
    ///
    /// # Encoding Process
    /// The value is encoded in 7-bit chunks, with the MSB of each byte indicating
    /// whether more bytes follow. The least significant bits are encoded first.
    ///
    /// # Examples
    /// ```ignore
    /// let mut buf = Vec::new();
    /// varint::encode(300, &mut buf);
    /// assert_eq!(buf, vec![0xAC, 0x02]);
    ///
    /// let mut buf2 = Vec::new();
    /// varint::encode(127, &mut buf2);
    /// assert_eq!(buf2, vec![0x7F]);
    /// ```
    pub fn encode(value: u64, buf: &mut Vec<u8>) {
        let mut v = value;
        while v >= 0x80 {
            buf.push((v as u8) | 0x80);
            v >>= 7;
        }
        buf.push(v as u8);
    }

    /// Decodes a varint-encoded value from a byte slice.
    ///
    /// # Arguments
    /// * `data` - Byte slice containing the varint-encoded value
    ///
    /// # Returns
    /// A tuple containing:
    /// - `u64` - The decoded value
    /// - `usize` - Number of bytes consumed from the input slice
    ///
    /// # Errors
    /// Returns `SSTableError::Decode` if:
    /// - The input slice is empty
    /// - The varint encoding exceeds 64 bits (malformed data)
    /// - The varint is incomplete (truncated data, continuation bit set on last byte)
    ///
    /// # Examples
    /// ```ignore
    /// let mut buf = Vec::new();
    /// varint::encode(300, &mut buf);
    ///
    /// let (value, bytes_read) = varint::decode(&buf)?;
    /// assert_eq!(value, 300);
    /// assert_eq!(bytes_read, buf.len());
    /// ```
    pub fn decode(data: &[u8]) -> Result<(u64, usize)> {
        if data.is_empty() {
            return Err(SSTableError::Decode("empty varint data".into()));
        }

        let mut result = 0u64;
        let mut shift = 0;
        for (i, &byte) in data.iter().enumerate() {
            if shift >= 64 {
                return Err(SSTableError::Decode(format!(
                    "varint too long: exceeds 64 bits at byte {}",
                    i
                )));
            }
            result |= ((byte & 0x7F) as u64) << shift;
            if (byte & 0x80) == 0 {
                return Ok((result, i + 1));
            }
            shift += 7;
        }
        Err(SSTableError::Decode(format!(
            "incomplete varint: expected more bytes after {} bytes",
            data.len()
        )))
    }

    /// Calculates the number of bytes required to encode a value as varint.
    ///
    /// This is an O(1) operation that uses bit manipulation to determine the
    /// encoded size without actually encoding the value.
    ///
    /// # Arguments
    /// * `value` - The value to calculate the encoded size for
    ///
    /// # Returns
    /// The number of bytes needed to encode the value (between 1 and 10 for u64).
    ///
    /// # Algorithm
    /// Uses the number of significant bits divided by 7 (rounded up) to determine
    /// the number of bytes needed. Special case: value 0 requires 1 byte.
    ///
    /// # Examples
    /// ```ignore
    /// assert_eq!(varint::encoded_size(0), 1);
    /// assert_eq!(varint::encoded_size(127), 1);
    /// assert_eq!(varint::encoded_size(128), 2);
    /// assert_eq!(varint::encoded_size(300), 2);
    /// assert_eq!(varint::encoded_size(u64::MAX), 10);
    /// ```
    pub fn encoded_size(value: u64) -> usize {
        let bit_len = 64 - value.leading_zeros() as usize;
        if bit_len == 0 { 1 } else { (bit_len + 6) / 7 }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ============================================================================
    // Varint Tests
    // ============================================================================

    #[test]
    fn test_varint_encode_decode_roundtrip() {
        let test_cases = vec![
            0u64,
            1,
            127,
            128,
            255,
            256,
            300,
            1024,
            65535,
            65536,
            1_000_000,
            u32::MAX as u64,
            u64::MAX / 2,
            u64::MAX,
        ];

        for value in test_cases {
            let mut buf = Vec::new();
            varint::encode(value, &mut buf);

            let (decoded, bytes_read) = varint::decode(&buf).unwrap();
            assert_eq!(value, decoded, "Roundtrip failed for value {}", value);
            assert_eq!(
                bytes_read,
                buf.len(),
                "Bytes read mismatch for value {}",
                value
            );
        }
    }

    #[test]
    fn test_varint_encoded_size() {
        assert_eq!(varint::encoded_size(0), 1);
        assert_eq!(varint::encoded_size(127), 1);
        assert_eq!(varint::encoded_size(128), 2);
        assert_eq!(varint::encoded_size(255), 2);
        assert_eq!(varint::encoded_size(16383), 2);
        assert_eq!(varint::encoded_size(16384), 3);
        assert_eq!(varint::encoded_size(u32::MAX as u64), 5);
        assert_eq!(varint::encoded_size(u64::MAX), 10);
    }

    #[test]
    fn test_varint_encode_size_matches_actual() {
        let test_cases = vec![0, 127, 128, 300, 1024, 65535, 1_000_000, u64::MAX];

        for value in test_cases {
            let expected_size = varint::encoded_size(value);
            let mut buf = Vec::new();
            varint::encode(value, &mut buf);
            assert_eq!(
                expected_size,
                buf.len(),
                "Encoded size mismatch for value {}: expected {}, got {}",
                value,
                expected_size,
                buf.len()
            );
        }
    }

    #[test]
    fn test_varint_decode_empty_input() {
        let result = varint::decode(&[]);
        assert!(result.is_err());
        match result {
            Err(SSTableError::Decode(msg)) => {
                assert!(msg.contains("empty"));
            }
            _ => panic!("Expected Decode error"),
        }
    }

    #[test]
    fn test_varint_decode_incomplete() {
        // Create an incomplete varint (continuation bit set on last byte)
        let incomplete = vec![0x80, 0x80, 0x80]; // All bytes have continuation bit set
        let result = varint::decode(&incomplete);
        assert!(result.is_err());
        match result {
            Err(SSTableError::Decode(msg)) => {
                assert!(msg.contains("incomplete"));
            }
            _ => panic!("Expected Decode error"),
        }
    }

    #[test]
    fn test_varint_specific_encodings() {
        // Test known encodings
        let mut buf = Vec::new();
        varint::encode(300, &mut buf);
        assert_eq!(buf, vec![0xAC, 0x02]);

        buf.clear();
        varint::encode(127, &mut buf);
        assert_eq!(buf, vec![0x7F]);

        buf.clear();
        varint::encode(128, &mut buf);
        assert_eq!(buf, vec![0x80, 0x01]);
    }

    // ============================================================================
    // BlockHandle Tests
    // ============================================================================

    #[test]
    fn test_block_handle_new() {
        let handle = BlockHandle::new(100, 200);
        assert_eq!(handle.offset, 100);
        assert_eq!(handle.size, 200);
    }

    #[test]
    fn test_block_handle_encode_decode_roundtrip() {
        let test_cases = vec![
            BlockHandle::new(0, 0),
            BlockHandle::new(100, 200),
            BlockHandle::new(1024, 4096),
            BlockHandle::new(u32::MAX as u64, u32::MAX as u64),
            BlockHandle::new(u64::MAX / 2, u64::MAX / 2),
        ];

        for handle in test_cases {
            let encoded = handle.encode();
            let (decoded, bytes_read) = BlockHandle::decode(&encoded).unwrap();

            assert_eq!(handle, decoded, "Roundtrip failed for handle {:?}", handle);
            assert_eq!(
                bytes_read,
                encoded.len(),
                "Bytes read mismatch for handle {:?}",
                handle
            );
        }
    }

    #[test]
    fn test_block_handle_encoded_size() {
        let handle1 = BlockHandle::new(127, 127); // Both fit in 1 byte
        assert_eq!(handle1.encoded_size(), 2);

        let handle2 = BlockHandle::new(128, 128); // Both need 2 bytes
        assert_eq!(handle2.encoded_size(), 4);

        let handle3 = BlockHandle::new(0, u64::MAX); // 1 + 10 bytes
        assert_eq!(handle3.encoded_size(), 11);
    }

    #[test]
    fn test_block_handle_encoded_size_matches_actual() {
        let handles = vec![
            BlockHandle::new(0, 0),
            BlockHandle::new(100, 200),
            BlockHandle::new(1024, 4096),
            BlockHandle::new(u64::MAX, u64::MAX),
        ];

        for handle in handles {
            let expected_size = handle.encoded_size();
            let encoded = handle.encode();
            assert_eq!(
                expected_size,
                encoded.len(),
                "Encoded size mismatch for handle {:?}",
                handle
            );
        }
    }

    #[test]
    fn test_block_handle_decode_incomplete() {
        // Incomplete varint for offset
        let incomplete = vec![0x80]; // Continuation bit set, but no more bytes
        let result = BlockHandle::decode(&incomplete);
        assert!(result.is_err());
    }

    #[test]
    fn test_block_handle_decode_partial() {
        // Complete offset, incomplete size
        let mut buf = Vec::new();
        varint::encode(100, &mut buf);
        buf.push(0x80); // Incomplete size varint
        let result = BlockHandle::decode(&buf);
        assert!(result.is_err());
    }

    // ============================================================================
    // Footer Tests
    // ============================================================================

    #[test]
    fn test_footer_new() {
        let meta_handle = BlockHandle::new(100, 200);
        let index_handle = BlockHandle::new(300, 400);
        let footer = Footer::new(meta_handle, index_handle);

        assert_eq!(footer.meta_index_handle, meta_handle);
        assert_eq!(footer.index_handle, index_handle);
        assert_eq!(footer.magic, MAGIC);
    }

    #[test]
    fn test_footer_encode_decode_roundtrip() {
        let test_cases = vec![
            (BlockHandle::new(0, 0), BlockHandle::new(0, 0)),
            (BlockHandle::new(100, 200), BlockHandle::new(300, 400)),
            (BlockHandle::new(1024, 4096), BlockHandle::new(8192, 16384)),
            (
                BlockHandle::new(u32::MAX as u64, u32::MAX as u64),
                BlockHandle::new(u32::MAX as u64, u32::MAX as u64),
            ),
        ];

        for (meta_handle, index_handle) in test_cases {
            let footer = Footer::new(meta_handle, index_handle);
            let mut buf = [0u8; FOOTER_SIZE];
            footer.encode(&mut buf);

            let decoded = Footer::decode(&buf).unwrap();
            assert_eq!(footer, decoded, "Roundtrip failed for footer");
            assert!(decoded.validate_magic());
        }
    }

    #[test]
    fn test_footer_magic_validation() {
        let footer = Footer::new(BlockHandle::new(100, 200), BlockHandle::new(300, 400));
        assert!(footer.validate_magic());

        // Corrupted magic
        let mut corrupted = footer.clone();
        corrupted.magic = 0;
        assert!(!corrupted.validate_magic());

        let mut corrupted2 = footer.clone();
        corrupted2.magic = u64::MAX;
        assert!(!corrupted2.validate_magic());
    }

    #[test]
    fn test_footer_encode_fixed_size() {
        let footer = Footer::new(BlockHandle::new(100, 200), BlockHandle::new(300, 400));
        let mut buf = [0u8; FOOTER_SIZE];
        footer.encode(&mut buf);

        // Footer should always be exactly FOOTER_SIZE bytes
        // (This is guaranteed by the type system, but we verify the magic is at the end)
        let magic_start = FOOTER_SIZE - MAGIC_SIZE;
        let magic_bytes = &buf[magic_start..];
        let decoded_magic = u64::from_be_bytes(magic_bytes.try_into().unwrap());
        assert_eq!(decoded_magic, MAGIC);
    }

    #[test]
    fn test_footer_decode_invalid_block_handles() {
        // Create a buffer with invalid varint encoding
        // All bytes have continuation bit set, which is invalid (incomplete varint)
        let mut buf = [0x80u8; FOOTER_SIZE];
        // Set magic to something valid to avoid magic validation error
        let magic_bytes = MAGIC.to_be_bytes();
        buf[FOOTER_SIZE - MAGIC_SIZE..].copy_from_slice(&magic_bytes);

        let result = Footer::decode(&buf);
        assert!(result.is_err());
        match result {
            Err(SSTableError::Decode(_)) => {
                // Expected: varint decode error
            }
            _ => panic!("Expected Decode error, got different error"),
        }
    }

    #[test]
    fn test_footer_padding_is_zero() {
        let footer = Footer::new(BlockHandle::new(100, 200), BlockHandle::new(300, 400));
        let mut buf = [0xFFu8; FOOTER_SIZE]; // Fill with non-zero
        footer.encode(&mut buf);

        // Find where padding starts
        let meta_size = footer.meta_index_handle.encoded_size();
        let index_size = footer.index_handle.encoded_size();
        let padding_start = meta_size + index_size;
        let padding_end = FOOTER_SIZE - MAGIC_SIZE;

        // Verify padding is all zeros
        for i in padding_start..padding_end {
            assert_eq!(buf[i], 0, "Padding byte at index {} should be zero", i);
        }
    }

    #[test]
    fn test_footer_large_block_handles() {
        // Test with maximum varint sizes
        let footer = Footer::new(
            BlockHandle::new(u64::MAX, u64::MAX),
            BlockHandle::new(u64::MAX, u64::MAX),
        );

        let mut buf = [0u8; FOOTER_SIZE];
        footer.encode(&mut buf);

        let decoded = Footer::decode(&buf).unwrap();
        assert_eq!(footer, decoded);
    }

    #[test]
    fn test_footer_small_block_handles() {
        // Test with minimum sizes (1-byte varints)
        let footer = Footer::new(BlockHandle::new(0, 0), BlockHandle::new(0, 0));

        let mut buf = [0u8; FOOTER_SIZE];
        footer.encode(&mut buf);

        let decoded = Footer::decode(&buf).unwrap();
        assert_eq!(footer, decoded);
    }

    #[test]
    fn test_footer_magic_position() {
        let footer = Footer::new(BlockHandle::new(100, 200), BlockHandle::new(300, 400));
        let mut buf = [0u8; FOOTER_SIZE];
        footer.encode(&mut buf);

        // Magic should always be at the last MAGIC_SIZE bytes
        let magic_bytes = &buf[FOOTER_SIZE - MAGIC_SIZE..];
        let magic = u64::from_be_bytes(magic_bytes.try_into().unwrap());
        assert_eq!(magic, MAGIC);
    }

    // ============================================================================
    // Integration Tests
    // ============================================================================

    #[test]
    fn test_varint_blockhandle_footer_integration() {
        // Test the full chain: varint -> BlockHandle -> Footer
        let meta_handle = BlockHandle::new(12345, 67890);
        let index_handle = BlockHandle::new(11111, 22222);

        // Encode BlockHandles
        let meta_encoded = meta_handle.encode();
        let index_encoded = index_handle.encode();

        // Decode them back
        let (meta_decoded, _) = BlockHandle::decode(&meta_encoded).unwrap();
        let (index_decoded, _) = BlockHandle::decode(&index_encoded).unwrap();

        assert_eq!(meta_handle, meta_decoded);
        assert_eq!(index_handle, index_decoded);

        // Create Footer
        let footer = Footer::new(meta_handle, index_handle);
        let mut buf = [0u8; FOOTER_SIZE];
        footer.encode(&mut buf);

        // Decode Footer
        let footer_decoded = Footer::decode(&buf).unwrap();
        assert_eq!(footer, footer_decoded);
        assert!(footer_decoded.validate_magic());
    }

    #[test]
    fn test_edge_case_zero_values() {
        // Test all zero values
        let footer = Footer::new(BlockHandle::new(0, 0), BlockHandle::new(0, 0));
        let mut buf = [0u8; FOOTER_SIZE];
        footer.encode(&mut buf);

        let decoded = Footer::decode(&buf).unwrap();
        assert_eq!(footer, decoded);
        assert_eq!(decoded.meta_index_handle.offset, 0);
        assert_eq!(decoded.meta_index_handle.size, 0);
    }

    #[test]
    fn test_edge_case_max_values() {
        // Test maximum u64 values
        let footer = Footer::new(
            BlockHandle::new(u64::MAX, u64::MAX),
            BlockHandle::new(u64::MAX, u64::MAX),
        );
        let mut buf = [0u8; FOOTER_SIZE];
        footer.encode(&mut buf);

        let decoded = Footer::decode(&buf).unwrap();
        assert_eq!(footer, decoded);
        assert_eq!(decoded.meta_index_handle.offset, u64::MAX);
        assert_eq!(decoded.meta_index_handle.size, u64::MAX);
    }
}
