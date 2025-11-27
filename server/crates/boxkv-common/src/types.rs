use bytes::Bytes;
use std::cmp::{Ordering, min};
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::mem::size_of;

/// Maximum length of key to display in debug logs.
const MAX_KEY_DEBUG_LEN: usize = 64;
/// Maximum length of value to display in debug logs.
const MAX_VALUE_DEBUG_LEN: usize = 64;

/// Type tags for serialization/deserialization in WAL and SSTable formats.
pub const NORMAL_VALUE_TYPE: u8 = 0;
pub const TOMBSTONE_VALUE_TYPE: u8 = 1;
pub const EXPIRING_VALUE_TYPE: u8 = 2;

/// Represents the type of value stored in an LSM-tree entry.
///
/// # Variants
/// - `Normal`: A standard key-value pair (PUT operation).
/// - `Tombstone`: A deletion marker (DELETE operation). No actual data is stored.
/// - `Expiring`: A value with an expiration timestamp (TTL support).
///
/// # Serialization
/// Each variant has a unique type tag for wire format encoding:
/// - Normal = 0
/// - Tombstone = 1
/// - Expiring = 2
#[derive(Clone, PartialEq)]
#[repr(u8)]
pub enum ValueType {
    /// Standard value containing raw bytes.
    Normal(Bytes) = NORMAL_VALUE_TYPE,

    /// Deletion marker. Indicates the key has been deleted but not yet compacted.
    Tombstone = TOMBSTONE_VALUE_TYPE,

    /// Value with TTL. Contains both data and an expiration timestamp (Unix epoch seconds).
    Expiring {
        data: Bytes,
        expire_at: u64, // Unix timestamp in seconds
    } = EXPIRING_VALUE_TYPE,
}

const VALUE_TOMBSTONE_LEN: usize = 0;
const VALUE_EXPIRING_AT_LEN: usize = size_of::<u64>();

impl ValueType {
    /// Returns the type tag for serialization.
    ///
    /// Used in WAL and SSTable formats to identify the variant during deserialization.
    pub fn type_tag(&self) -> u8 {
        match self {
            ValueType::Normal(_) => NORMAL_VALUE_TYPE,
            ValueType::Tombstone => TOMBSTONE_VALUE_TYPE,
            ValueType::Expiring { .. } => EXPIRING_VALUE_TYPE,
        }
    }

    /// Returns the total serialized length (data + metadata).
    ///
    /// This is the number of bytes occupied in the WAL/SSTable payload section
    /// for this value, excluding the type tag.
    ///
    /// # Examples
    /// - Normal("hello") → 5 bytes
    /// - Tombstone → 0 bytes
    /// - Expiring { data: "hello", expire_at: 123 } → 13 bytes (8 + 5)
    pub fn serialized_len(&self) -> usize {
        self.data_len() + self.meta_len()
    }

    /// Returns the length of the user data in bytes.
    ///
    /// For Tombstone, this is always 0.
    pub fn data_len(&self) -> usize {
        match self {
            ValueType::Normal(bytes) => bytes.len(),
            ValueType::Tombstone => VALUE_TOMBSTONE_LEN,
            ValueType::Expiring { data, .. } => data.len(),
        }
    }

    /// Returns the length of metadata in bytes (excluding the user data).
    ///
    /// - Normal: 0 (no metadata)
    /// - Tombstone: 0 (no metadata)
    /// - Expiring: 8 (expire_at timestamp)
    pub fn meta_len(&self) -> usize {
        match self {
            ValueType::Normal(_) => 0,
            ValueType::Tombstone => 0,
            ValueType::Expiring { .. } => VALUE_EXPIRING_AT_LEN,
        }
    }

    /// Checks if this value represents a deletion marker.
    pub fn is_tombstone(&self) -> bool {
        matches!(self, ValueType::Tombstone)
    }
}

impl Debug for ValueType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Normal(bytes) => {
                let debug_len = min(bytes.len(), MAX_VALUE_DEBUG_LEN);
                write!(
                    f,
                    "Normal(len={}, data={:?})",
                    bytes.len(),
                    &String::from_utf8_lossy(&bytes[..debug_len])
                )
            }
            Self::Tombstone => write!(f, "Tombstone"),
            Self::Expiring { data, expire_at } => {
                let debug_len = min(data.len(), MAX_VALUE_DEBUG_LEN);
                write!(
                    f,
                    "Expiring(expire_at={}, len={}, data={:?})",
                    expire_at,
                    data.len(),
                    &String::from_utf8_lossy(&data[..debug_len])
                )
            }
        }
    }
}

/// Represents a single versioned record in the LSM-tree.
///
/// An `Entry` is the fundamental unit of data stored in the engine. It consists of:
/// - A key (arbitrary bytes)
/// - A value (Normal data, Tombstone, or Expiring value)
/// - A sequence number (monotonically increasing, used for MVCC)
///
/// # Ordering Semantics
/// Entries are ordered by:
/// 1. **Key** (Ascending) - Primary sort key
/// 2. **Sequence Number** (Descending) - Newer versions appear first
///
/// This ordering is critical for LSM-tree operations:
/// - During compaction, newer versions shadow older ones
/// - Point queries can stop at the first match (latest version)
/// - Range scans naturally iterate over the latest versions
///
/// # Examples
/// ```ignore
/// let e1 = Entry::new_normal(100, key.clone(), value1);
/// let e2 = Entry::new_normal(200, key.clone(), value2);
///
/// assert!(e2 < e1); // seq=200 comes before seq=100
/// ```
#[derive(Clone)]
pub struct Entry {
    key: Bytes,
    val: ValueType,
    seq: u64,
}

const ENTRY_SEQ_LEN: usize = size_of::<u64>();

impl Entry {
    /// Creates a new entry with the given sequence number, key, and value type.
    ///
    /// This is the internal constructor. Use `new_normal`, `new_tombstone`, or
    /// `new_expiring` for specific value types.
    pub fn new(seq: u64, key: Bytes, val: ValueType) -> Self {
        Self { key, val, seq }
    }

    /// Creates a deletion marker entry (Tombstone).
    ///
    /// Tombstones are used to mark deleted keys in the LSM-tree. They are
    /// removed during compaction when they are the oldest version of a key.
    pub fn new_tombstone(seq: u64, key: Bytes) -> Self {
        Self::new(seq, key, ValueType::Tombstone)
    }

    /// Creates a standard value entry (Normal).
    pub fn new_normal(seq: u64, key: Bytes, val: Bytes) -> Self {
        Self::new(seq, key, ValueType::Normal(val))
    }

    /// Creates an expiring value entry with TTL.
    ///
    /// # Arguments
    /// * `seq` - Sequence number
    /// * `key` - Key bytes
    /// * `val` - Value bytes
    /// * `expire_at` - Unix timestamp (seconds) when this entry expires
    pub fn new_expiring(seq: u64, key: Bytes, val: Bytes, expire_at: u64) -> Self {
        Self::new(
            seq,
            key,
            ValueType::Expiring {
                data: val,
                expire_at,
            },
        )
    }

    /// Returns `true` if this entry is a deletion marker.
    pub fn is_tombstone(&self) -> bool {
        self.val.is_tombstone()
    }

    /// Returns the estimated memory size of this entry in bytes.
    ///
    /// This includes:
    /// - Key length
    /// - Value serialized length (data + metadata)
    /// - Sequence number (8 bytes)
    ///
    /// Used primarily for Memtable size calculations to trigger flush operations.
    pub fn estimated_size(&self) -> usize {
        self.key.len() + self.val.serialized_len() + ENTRY_SEQ_LEN
    }

    /// Returns a reference to the key.
    pub fn key(&self) -> &Bytes {
        &self.key
    }

    /// Returns a reference to the value type.
    pub fn val(&self) -> &ValueType {
        &self.val
    }

    /// Returns the sequence number.
    pub fn seq(&self) -> u64 {
        self.seq
    }
}

impl Debug for Entry {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        // Truncate key output for readability in logs
        let debug_len = min(self.key.len(), MAX_KEY_DEBUG_LEN);

        f.debug_struct("Entry")
            .field("key", &String::from_utf8_lossy(&self.key[..debug_len]))
            .field("val", &self.val)
            .field("seq", &self.seq)
            .finish()
    }
}

impl PartialEq for Entry {
    fn eq(&self, other: &Self) -> bool {
        // Consistent with Ord: checks both Key and Seq.
        //
        // Note: If Seq matches, Key MUST match in a valid LSM system (Seq is globally unique).
        // We check Key here to strictly adhere to the PartialOrd/Ord contract.
        // Short-circuiting (`&&`) ensures no performance penalty if Seqs differ.
        self.seq == other.seq && self.key == other.key
    }
}

impl Eq for Entry {}

impl PartialOrd for Entry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Entry {
    fn cmp(&self, other: &Self) -> Ordering {
        // Critical LSM Ordering:
        // 1. Compare Key (Ascending)
        // 2. Compare Seq (Descending)
        //
        // Descending Seq ensures that when scanning, the latest version of a key
        // appears first, allowing for efficient lookups (finding the first match is enough).
        self.key.cmp(&other.key).then(other.seq.cmp(&self.seq))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_entry_ordering() {
        let key1 = Bytes::from("key1");
        let key2 = Bytes::from("key2");

        // Case 1: Same key, different seq. Newer seq (larger) should come first.
        let e1_seq100 = Entry::new_normal(100, key1.clone(), Bytes::from("v1"));
        let e1_seq200 = Entry::new_normal(200, key1.clone(), Bytes::from("v2"));

        // e1_seq200 (newer) < e1_seq100 (older) because we want newer items first in sort
        assert_eq!(e1_seq200.cmp(&e1_seq100), Ordering::Less);
        assert!(e1_seq200 < e1_seq100);

        // Case 2: Different key. key1 < key2 (Ascending).
        let e2_seq300 = Entry::new_normal(300, key2.clone(), Bytes::from("v3"));
        assert_eq!(e1_seq200.cmp(&e2_seq300), Ordering::Less);
        assert!(e1_seq200 < e2_seq300);

        // Case 3: Vector sort test
        let mut entries = vec![
            e1_seq100.clone(), // key1, seq 100
            e2_seq300.clone(), // key2, seq 300
            e1_seq200.clone(), // key1, seq 200
        ];
        entries.sort();

        // Expected order:
        // 1. key1, seq 200 (Newer)
        // 2. key1, seq 100 (Older)
        // 3. key2, seq 300
        assert_eq!(entries[0].seq(), 200);
        assert_eq!(entries[0].key(), &key1);

        assert_eq!(entries[1].seq(), 100);
        assert_eq!(entries[1].key(), &key1);

        assert_eq!(entries[2].seq(), 300);
        assert_eq!(entries[2].key(), &key2);
    }

    #[test]
    fn test_entry_size() {
        let key = Bytes::from("key"); // 3 bytes
        let val = Bytes::from("value"); // 5 bytes
        let entry = Entry::new_normal(1, key, val);

        // 3 (key) + 5 (value) + 8 (seq) = 16
        assert_eq!(entry.estimated_size(), 16);

        let tombstone = Entry::new_tombstone(1, Bytes::from("key"));
        // 3 (key) + 0 (tombstone) + 8 (seq) = 11
        assert_eq!(tombstone.estimated_size(), 11);
    }

    #[test]
    fn test_entry_equality() {
        let key = Bytes::from("key");
        let val1 = Bytes::from("val1");
        let val2 = Bytes::from("val2");

        let e1 = Entry::new_normal(100, key.clone(), val1);
        let e2 = Entry::new_normal(100, key.clone(), val2); // Same key+seq, different value

        // In LSM, same key+seq means same entry. Value should be ignored in equality.
        assert_eq!(e1, e2);

        let e3 = Entry::new_normal(101, key.clone(), Bytes::from("val1"));
        assert_ne!(e1, e3); // Different seq
    }
}
