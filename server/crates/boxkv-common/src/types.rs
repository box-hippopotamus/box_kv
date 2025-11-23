use std::fmt;
use std::fmt::{Debug, Formatter};
use std::cmp::{min, Ordering};
use std::mem::size_of;
use bytes::Bytes;

/// Maximum length of key to display in debug logs.
const MAX_KEY_DEBUG_LEN: usize = 64;
/// Maximum length of value to display in debug logs.
const MAX_VALUE_DEBUG_LEN: usize = 64;

/// Represents the type of value stored in an LSM-tree entry.
///
/// - `Normal`: Represents a standard key-value pair (Put operation).
/// - `Tombstone`: Represents a deletion marker (Delete operation).
#[derive(Clone)]
pub enum ValueType {
    /// A standard value containing raw bytes.
    Normal(Bytes),
    /// A marker indicating the key has been deleted.
    Tombstone,
}

impl ValueType {
    /// Returns the length of the underlying value in bytes.
    /// Returns 0 for Tombstones.
    fn len(&self) -> usize {
        match self {
            Self::Normal(bytes) => bytes.len(),
            Self::Tombstone => 0,
        }
    }

    /// Checks if the value type is a Tombstone.
    fn is_tombstone(&self) -> bool {
        matches!(self, ValueType::Tombstone)
    }
}

impl Debug for ValueType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Normal(bytes) => {
                // Truncate output to avoid flooding logs with large binary data
                let debug_len = min(bytes.len(), MAX_VALUE_DEBUG_LEN);

                write!(f, "Normal: len={}, {:?}", bytes.len(),
                       &String::from_utf8_lossy(&bytes[..debug_len]))
            },
            Self::Tombstone => write!(f, "Tombstone"),
        }
    }
}

/// Represents an atomic record in the storage engine.
///
/// An `Entry` consists of a key, a value (or tombstone), a sequence number,
/// and a timestamp.
///
/// # Ordering
/// Entries are ordered by:
/// 1. Key (Ascending)
/// 2. Sequence Number (Descending) - Newer versions appear first.
///
/// This ordering is critical for LSM-tree lookups to find the latest version efficiently.
#[derive(Clone)]
pub struct Entry {
    key: Bytes,
    value: ValueType,
    seq: u64,
    timestamp: u64,
}

impl Entry {
    /// Private constructor for internal use.
    fn new(key: Bytes, value: ValueType, seq: u64, timestamp: u64) -> Self {
        Self {
            key,
            value,
            seq,
            timestamp,
        }
    }

    /// Creates a new deletion marker entry (Tombstone).
    pub fn new_tombstone(key: Bytes, seq: u64, timestamp: u64) -> Self {
        Self::new(key, ValueType::Tombstone, seq, timestamp)
    }

    /// Creates a new standard value entry.
    pub fn new_normal(key: Bytes, value: Bytes, seq: u64, timestamp: u64) -> Self {
        Self::new(key, ValueType::Normal(value), seq, timestamp)
    }

    /// Checks if this entry represents a deletion.
    pub fn is_tombstone(&self) -> bool {
        self.value.is_tombstone()
    }

    /// Returns the estimated memory size of this entry.
    ///
    /// This includes the size of the key, value, and fixed-size metadata (seq + timestamp).
    /// It is primarily used for Memtable size calculations to trigger flush operations.
    pub fn estimated_size(&self) -> usize {
        self.key.len() 
        + self.value.len() 
        + size_of::<u64>() // seq
        + size_of::<u64>() // timestamp
    }
    
    /// Returns a reference to the key.
    pub fn key(&self) -> &Bytes { &self.key }
    
    /// Returns a reference to the value type.
    pub fn value(&self) -> &ValueType { &self.value }
    
    /// Returns the sequence number.
    pub fn seq(&self) -> u64 { self.seq }
    
    /// Returns the timestamp.
    pub fn timestamp(&self) -> u64 { self.timestamp }
}

impl Debug for Entry {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        // Truncate key output for readability in logs
        let debug_len = min(self.key.len(), MAX_KEY_DEBUG_LEN);

        f.debug_struct("Entry")
            .field("key", &String::from_utf8_lossy(&self.key[..debug_len]))
            .field("value", &self.value)
            .field("seq", &self.seq)
            .field("timestamp", &self.timestamp)
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

        // Case 1: Same key, different seq. Newer seq (larger) should be smaller (come first).
        let e1_seq100 = Entry::new_normal(key1.clone(), Bytes::from("v1"), 100, 0);
        let e1_seq200 = Entry::new_normal(key1.clone(), Bytes::from("v2"), 200, 0);
        
        // e1_seq200 (newer) < e1_seq100 (older) because we want newer items first in sort
        assert_eq!(e1_seq200.cmp(&e1_seq100), Ordering::Less); 
        assert!(e1_seq200 < e1_seq100);

        // Case 2: Different key. key1 < key2 (Ascending).
        let e2_seq300 = Entry::new_normal(key2.clone(), Bytes::from("v3"), 300, 0);
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
        let entry = Entry::new_normal(key, val, 1, 1);
        
        // 3 + 5 + 8(seq) + 8(ts) = 24
        assert_eq!(entry.estimated_size(), 24);

        let tombstone = Entry::new_tombstone(Bytes::from("key"), 1, 1);
        // 3 + 0 + 8 + 8 = 19
        assert_eq!(tombstone.estimated_size(), 19);
    }

    #[test]
    fn test_entry_equality() {
        let key = Bytes::from("key");
        let val1 = Bytes::from("val1");
        let val2 = Bytes::from("val2");

        let e1 = Entry::new_normal(key.clone(), val1, 100, 0);
        let e2 = Entry::new_normal(key.clone(), val2, 100, 1); // timestamp/value differ, but seq/key same

        // In LSM, same key+seq means same entry. Value/TS should be ignored in Eq.
        assert_eq!(e1, e2);

        let e3 = Entry::new_normal(key.clone(), Bytes::from("val1"), 101, 0);
        assert_ne!(e1, e3); // Different seq
    }
}
