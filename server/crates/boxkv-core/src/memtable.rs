//! In-memory ordered key-value table for LSM-tree storage engine.
//!
//! # Overview
//!
//! The `MemTable` serves as the first level of storage in the LSM-tree architecture.
//! All write operations (PUT/DELETE) are first written to the WAL for durability,
//! then immediately applied to the active MemTable for fast in-memory access.
//!
//! # Architecture Role
//!
//! ```text
//! Write Path:
//!   Client → WAL (fsync) → MemTable → Return Success
//!                            ↓
//!                      (When size > threshold)
//!                            ↓
//!                    Flush to SSTable
//!
//! Read Path:
//!   Client → MemTable → Immutable MemTables → SSTables
//!            (newest)                          (oldest)
//! ```
//!
//! # Design Principles
//!
//! - **Ordered Storage**: Uses `BTreeMap` for sorted key iteration (required for SSTable flush)
//! - **Lock-Free Size Tracking**: `AtomicU64` for concurrent size checks without blocking
//! - **MVCC Support**: Each entry stores a sequence number for multi-version concurrency control
//! - **Tombstone Deletion**: Deletes are writes with a special marker (actual removal during compaction)
//!
//! # Concurrency Model
//!
//! - **Write Lock**: Required for `put()` and `delete()` operations
//! - **Read Lock**: Shared by multiple `get()` and `snapshot()` calls
//! - **No Lock**: Size checks use atomic operations
//!
//! # Memory Management
//!
//! Memory usage is tracked approximately as:
//! ```text
//! size = Σ(key_len + value_len + metadata_overhead)
//! ```
//!
//! When `size` exceeds the configured threshold (typically 4MB), the Engine
//! marks this MemTable as immutable and creates a new active one.

use std::collections::BTreeMap;
use std::mem::size_of;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use parking_lot::RwLock;

use boxkv_common::types::{Entry, ValueType};

/// Internal entry metadata stored alongside each key-value pair.
///
/// Separated from the public `Entry` type to minimize memory overhead
/// while maintaining MVCC semantics.
struct EntryInfo {
    /// Value data (Normal, Tombstone, or Expiring)
    value: ValueType,
    /// Sequence number for MVCC ordering
    seq: u64,
}

/// In-memory write buffer storing sorted key-value pairs.
///
/// This is the mutable part of the LSM-tree that receives all writes.
/// Once full, it becomes immutable and is flushed to an SSTable.
///
/// # Thread Safety
///
/// - Multiple concurrent reads are allowed (via `RwLock`)
/// - Writes block all other operations briefly
/// - Size tracking is lock-free
///
/// # Examples
///
/// ```ignore
/// let mut memtable = MemTable::new();
///
/// // Write operations
/// memtable.put(1, Bytes::from("key1"), Bytes::from("value1"));
/// memtable.put(2, Bytes::from("key2"), Bytes::from("value2"));
///
/// // Read operations
/// let entry = memtable.get(&Bytes::from("key1")).unwrap();
/// assert_eq!(entry.seq(), 1);
///
/// // Delete (writes tombstone)
/// memtable.delete(3, Bytes::from("key1"));
///
/// // Check size for flush decision
/// if memtable.size() > 4 * 1024 * 1024 {
///     let entries = memtable.snapshot();
///     // flush to SSTable...
/// }
/// ```
pub struct MemTable {
    /// Ordered map of keys to entry metadata.
    /// BTreeMap ensures keys are sorted for efficient range scans and SSTable flush.
    table: RwLock<BTreeMap<Bytes, EntryInfo>>,

    /// Approximate memory usage in bytes.
    /// Updated atomically to allow lock-free size checks.
    size: AtomicU64,
}

/// Estimated overhead per entry for sequence number and internal bookkeeping.
/// Used in size calculations to approximate total memory usage.
const ENTRY_METADATA_SIZE: usize = size_of::<u64>() * 2; // seq + timestamp

impl MemTable {
    /// Creates a new empty MemTable.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let memtable = MemTable::new();
    /// assert_eq!(memtable.size(), 0);
    /// ```
    pub fn new() -> Self {
        Self {
            table: RwLock::new(BTreeMap::new()),
            size: AtomicU64::new(0),
        }
    }

    /// Internal helper to update or insert an entry.
    ///
    /// This method handles both insertions and updates, correctly adjusting
    /// the size tracker based on the difference in serialized sizes.
    ///
    /// # Arguments
    ///
    /// * `seq` - Sequence number for MVCC (must be monotonically increasing globally)
    /// * `key` - Key bytes
    /// * `value` - Value type (Normal, Tombstone, or Expiring)
    ///
    /// # Size Calculation
    ///
    /// - **New Entry**: `size += key_len + value_len + metadata`
    /// - **Update**: `size += (new_size - old_size)` (can be negative)
    fn update(&mut self, seq: u64, key: Bytes, value: ValueType) {
        let mut writer = self.table.write();

        match writer.get_mut(&key) {
            Some(entry_info) => {
                // Key exists - update in place
                let old_size = key.len() + entry_info.value.serialized_len() + ENTRY_METADATA_SIZE;

                entry_info.value = value;
                entry_info.seq = seq;

                let new_size = key.len() + entry_info.value.serialized_len() + ENTRY_METADATA_SIZE;

                // Adjust size atomically (can be positive or negative delta)
                let diff = new_size as i64 - old_size as i64;
                if diff > 0 {
                    self.size.fetch_add(diff as u64, Ordering::SeqCst);
                } else if diff < 0 {
                    self.size.fetch_sub((-diff) as u64, Ordering::SeqCst);
                }
            }
            None => {
                // New key - insert and increase size
                let size = key.len() + value.serialized_len() + ENTRY_METADATA_SIZE;
                self.size.fetch_add(size as u64, Ordering::SeqCst);
                writer.insert(key, EntryInfo { value, seq });
            }
        }
    }

    /// Inserts or updates a key-value pair (PUT operation).
    ///
    /// If the key already exists, the old value is replaced and the size
    /// is adjusted accordingly. The sequence number must be globally unique
    /// and monotonically increasing.
    ///
    /// # Arguments
    ///
    /// * `seq` - Sequence number from the Engine's atomic counter
    /// * `key` - Key bytes (empty keys are allowed)
    /// * `value` - Value bytes (empty values are allowed)
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let mut memtable = MemTable::new();
    ///
    /// memtable.put(1, Bytes::from("user:1"), Bytes::from("Alice"));
    /// memtable.put(2, Bytes::from("user:1"), Bytes::from("Bob")); // Update
    ///
    /// let entry = memtable.get(&Bytes::from("user:1")).unwrap();
    /// assert_eq!(entry.seq(), 2); // Latest version
    /// ```
    pub fn put(&mut self, seq: u64, key: Bytes, value: Bytes) {
        self.update(seq, key, ValueType::Normal(value));
    }

    /// Marks a key as deleted by writing a tombstone (DELETE operation).
    ///
    /// This does NOT remove the key from the MemTable. Instead, it writes
    /// a special `Tombstone` marker. The actual deletion happens during
    /// compaction when we know no older versions exist.
    ///
    /// # Why Tombstones?
    ///
    /// In LSM-trees, deletions must be visible to older levels. A tombstone
    /// ensures that reads check all levels and correctly return "not found"
    /// even if older SSTables contain the deleted key.
    ///
    /// # Arguments
    ///
    /// * `seq` - Sequence number for the delete operation
    /// * `key` - Key to delete
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let mut memtable = MemTable::new();
    ///
    /// memtable.put(1, Bytes::from("temp"), Bytes::from("data"));
    /// memtable.delete(2, Bytes::from("temp"));
    ///
    /// let entry = memtable.get(&Bytes::from("temp")).unwrap();
    /// assert!(entry.is_tombstone()); // Marked as deleted
    /// ```
    pub fn delete(&mut self, seq: u64, key: Bytes) {
        self.update(seq, key, ValueType::Tombstone);
    }

    /// Retrieves an entry by key.
    ///
    /// # Returns
    ///
    /// - `Some(Entry)` - Key exists (may be a tombstone)
    /// - `None` - Key not found
    ///
    /// # MVCC Behavior
    ///
    /// Only the **latest version** (highest sequence number) is stored per key.
    /// Older versions are overwritten during PUT operations.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let mut memtable = MemTable::new();
    /// memtable.put(1, Bytes::from("key1"), Bytes::from("value1"));
    ///
    /// let entry = memtable.get(&Bytes::from("key1")).unwrap();
    /// match entry.val() {
    ///     ValueType::Normal(data) => assert_eq!(data.as_ref(), b"value1"),
    ///     _ => panic!("Expected normal value"),
    /// }
    ///
    /// assert!(memtable.get(&Bytes::from("nonexistent")).is_none());
    /// ```
    pub fn get(&self, key: &Bytes) -> Option<Entry> {
        let reader = self.table.read();
        reader
            .get(key)
            .map(|entry_info| Entry::new(entry_info.seq, key.clone(), entry_info.value.clone()))
    }

    /// Returns the approximate memory usage in bytes.
    ///
    /// This is a lock-free operation using atomic loads. The value is an
    /// approximation because it doesn't account for BTreeMap's internal
    /// node overhead.
    ///
    /// # Usage
    ///
    /// The Engine uses this to decide when to flush the MemTable to disk:
    ///
    /// ```ignore
    /// if memtable.size() >= config.memtable_size_mb * 1024 * 1024 {
    ///     flush_to_sstable(memtable);
    /// }
    /// ```
    ///
    /// # Accuracy
    ///
    /// The size is approximate:
    /// - **Included**: Key bytes + Value bytes + Metadata overhead
    /// - **Not Included**: BTreeMap node pointers, allocator overhead
    ///
    /// Typical accuracy: 80-90% of actual memory usage.
    pub fn size(&self) -> u64 {
        self.size.load(Ordering::SeqCst)
    }

    /// Creates a consistent snapshot of all entries sorted by key.
    ///
    /// This clones all entries into a vector, which is necessary for:
    /// - **Flushing to SSTable**: Entries must be written in sorted order
    /// - **Range Scans**: Returning a consistent view without holding locks
    ///
    /// # Performance
    ///
    /// - **Time**: O(n) where n is the number of entries
    /// - **Space**: O(n) clone of all keys and values
    ///
    /// # Lock Behavior
    ///
    /// Holds a read lock during the snapshot operation, which:
    /// - ✅ Allows concurrent reads from other threads
    /// - ❌ Blocks writes temporarily (typically <1ms for 4MB MemTable)
    ///
    /// # Returns
    ///
    /// A vector of entries sorted by key in ascending order.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let mut memtable = MemTable::new();
    /// memtable.put(1, Bytes::from("c"), Bytes::from("3"));
    /// memtable.put(2, Bytes::from("a"), Bytes::from("1"));
    /// memtable.put(3, Bytes::from("b"), Bytes::from("2"));
    ///
    /// let snapshot = memtable.snapshot();
    /// assert_eq!(snapshot.len(), 3);
    /// // Entries are sorted by key
    /// assert_eq!(snapshot[0].key().as_ref(), b"a");
    /// assert_eq!(snapshot[1].key().as_ref(), b"b");
    /// assert_eq!(snapshot[2].key().as_ref(), b"c");
    /// ```
    pub fn snapshot(&self) -> Vec<Entry> {
        self.table
            .read()
            .iter()
            .map(|(key, entry_info)| {
                Entry::new(entry_info.seq, key.clone(), entry_info.value.clone())
            })
            .collect()
    }
}

impl Default for MemTable {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use boxkv_common::types::ValueType;

    #[test]
    fn test_memtable_new_is_empty() {
        let memtable = MemTable::new();
        assert_eq!(memtable.size(), 0);
        assert_eq!(memtable.snapshot().len(), 0);
    }

    #[test]
    fn test_memtable_put_and_get() {
        let mut memtable = MemTable::new();

        memtable.put(1, Bytes::from("key1"), Bytes::from("value1"));
        memtable.put(2, Bytes::from("key2"), Bytes::from("value2"));

        let entry1 = memtable.get(&Bytes::from("key1")).unwrap();
        assert_eq!(entry1.seq(), 1);
        assert_eq!(entry1.key().as_ref(), b"key1");
        match entry1.val() {
            ValueType::Normal(data) => assert_eq!(data.as_ref(), b"value1"),
            _ => panic!("Expected Normal value"),
        }

        let entry2 = memtable.get(&Bytes::from("key2")).unwrap();
        assert_eq!(entry2.seq(), 2);
    }

    #[test]
    fn test_memtable_get_nonexistent_key() {
        let memtable = MemTable::new();
        assert!(memtable.get(&Bytes::from("nonexistent")).is_none());
    }

    #[test]
    fn test_memtable_update_existing_key() {
        let mut memtable = MemTable::new();

        // First write
        memtable.put(1, Bytes::from("key1"), Bytes::from("old_value"));
        let entry = memtable.get(&Bytes::from("key1")).unwrap();
        assert_eq!(entry.seq(), 1);
        match entry.val() {
            ValueType::Normal(data) => assert_eq!(data.as_ref(), b"old_value"),
            _ => panic!("Expected Normal value"),
        }

        // Update with newer sequence number
        memtable.put(2, Bytes::from("key1"), Bytes::from("new_value"));
        let entry = memtable.get(&Bytes::from("key1")).unwrap();
        assert_eq!(entry.seq(), 2);
        match entry.val() {
            ValueType::Normal(data) => assert_eq!(data.as_ref(), b"new_value"),
            _ => panic!("Expected Normal value"),
        }
    }

    #[test]
    fn test_memtable_delete_creates_tombstone() {
        let mut memtable = MemTable::new();

        memtable.put(1, Bytes::from("key1"), Bytes::from("value1"));
        memtable.delete(2, Bytes::from("key1"));

        let entry = memtable.get(&Bytes::from("key1")).unwrap();
        assert_eq!(entry.seq(), 2);
        assert!(entry.is_tombstone());
        assert!(matches!(entry.val(), ValueType::Tombstone));
    }

    #[test]
    fn test_memtable_delete_nonexistent_key() {
        let mut memtable = MemTable::new();

        // Deleting a key that doesn't exist should still create a tombstone
        memtable.delete(1, Bytes::from("never_existed"));

        let entry = memtable.get(&Bytes::from("never_existed")).unwrap();
        assert!(entry.is_tombstone());
    }

    #[test]
    fn test_memtable_size_tracking_on_put() {
        let mut memtable = MemTable::new();
        assert_eq!(memtable.size(), 0);

        let key = Bytes::from("key1");
        let value = Bytes::from("value1");
        let expected_size = key.len() + value.len() + ENTRY_METADATA_SIZE;

        memtable.put(1, key, value);
        assert_eq!(memtable.size(), expected_size as u64);
    }

    #[test]
    fn test_memtable_size_tracking_on_update() {
        let mut memtable = MemTable::new();

        // First write: 4 + 6 + 16 = 26 bytes
        memtable.put(1, Bytes::from("key1"), Bytes::from("value1"));
        let size_after_first = memtable.size();
        assert_eq!(size_after_first, 26);

        // Update with longer value: 4 + 12 + 16 = 32 bytes
        memtable.put(2, Bytes::from("key1"), Bytes::from("longer_value"));
        let size_after_update = memtable.size();
        assert_eq!(size_after_update, 32);

        // Update with shorter value: 4 + 3 + 16 = 23 bytes
        memtable.put(3, Bytes::from("key1"), Bytes::from("abc"));
        let size_after_shrink = memtable.size();
        assert_eq!(size_after_shrink, 23);
    }

    #[test]
    fn test_memtable_size_tracking_on_delete() {
        let mut memtable = MemTable::new();

        // Put: 4 + 6 + 16 = 26 bytes
        memtable.put(1, Bytes::from("key1"), Bytes::from("value1"));
        assert_eq!(memtable.size(), 26);

        // Delete: Tombstone has no value data, so: 4 + 0 + 16 = 20 bytes
        memtable.delete(2, Bytes::from("key1"));
        assert_eq!(memtable.size(), 20);
    }

    #[test]
    fn test_memtable_snapshot_ordering() {
        let mut memtable = MemTable::new();

        // Insert in non-sorted order
        memtable.put(1, Bytes::from("zebra"), Bytes::from("z"));
        memtable.put(2, Bytes::from("apple"), Bytes::from("a"));
        memtable.put(3, Bytes::from("mango"), Bytes::from("m"));
        memtable.put(4, Bytes::from("banana"), Bytes::from("b"));

        let snapshot = memtable.snapshot();
        assert_eq!(snapshot.len(), 4);

        // Snapshot should be sorted by key (BTreeMap guarantees this)
        assert_eq!(snapshot[0].key().as_ref(), b"apple");
        assert_eq!(snapshot[1].key().as_ref(), b"banana");
        assert_eq!(snapshot[2].key().as_ref(), b"mango");
        assert_eq!(snapshot[3].key().as_ref(), b"zebra");
    }

    #[test]
    fn test_memtable_snapshot_includes_tombstones() {
        let mut memtable = MemTable::new();

        memtable.put(1, Bytes::from("key1"), Bytes::from("value1"));
        memtable.delete(2, Bytes::from("key2"));
        memtable.put(3, Bytes::from("key3"), Bytes::from("value3"));

        let snapshot = memtable.snapshot();
        assert_eq!(snapshot.len(), 3);

        assert!(matches!(snapshot[0].val(), ValueType::Normal(_)));
        assert!(matches!(snapshot[1].val(), ValueType::Tombstone));
        assert!(matches!(snapshot[2].val(), ValueType::Normal(_)));
    }

    #[test]
    fn test_memtable_empty_key_and_value() {
        let mut memtable = MemTable::new();

        memtable.put(1, Bytes::from(""), Bytes::from(""));

        let entry = memtable.get(&Bytes::from("")).unwrap();
        assert_eq!(entry.key().len(), 0);
        match entry.val() {
            ValueType::Normal(data) => assert_eq!(data.len(), 0),
            _ => panic!("Expected Normal value"),
        }
    }

    #[test]
    fn test_memtable_large_values() {
        let mut memtable = MemTable::new();

        let large_key = vec![b'k'; 1024]; // 1KB key
        let large_value = vec![b'v'; 1024 * 1024]; // 1MB value

        memtable.put(
            1,
            Bytes::from(large_key.clone()),
            Bytes::from(large_value.clone()),
        );

        let entry = memtable.get(&Bytes::from(large_key)).unwrap();
        match entry.val() {
            ValueType::Normal(data) => {
                assert_eq!(data.len(), 1024 * 1024);
                assert_eq!(data.as_ref(), large_value.as_slice());
            }
            _ => panic!("Expected Normal value"),
        }

        // Size should reflect the large entry
        let expected_size = 1024 + 1024 * 1024 + ENTRY_METADATA_SIZE;
        assert_eq!(memtable.size(), expected_size as u64);
    }

    #[test]
    fn test_memtable_binary_keys_and_values() {
        let mut memtable = MemTable::new();

        // Binary data with all byte values
        let binary_key: Vec<u8> = (0..=255).collect();
        let binary_value: Vec<u8> = (0..=255).rev().collect();

        memtable.put(
            1,
            Bytes::from(binary_key.clone()),
            Bytes::from(binary_value.clone()),
        );

        let entry = memtable.get(&Bytes::from(binary_key)).unwrap();
        match entry.val() {
            ValueType::Normal(data) => assert_eq!(data.as_ref(), binary_value.as_slice()),
            _ => panic!("Expected Normal value"),
        }
    }

    #[test]
    fn test_memtable_sequence_number_ordering() {
        let mut memtable = MemTable::new();

        // Write with increasing sequence numbers
        memtable.put(100, Bytes::from("key1"), Bytes::from("v1"));
        memtable.put(200, Bytes::from("key2"), Bytes::from("v2"));
        memtable.put(150, Bytes::from("key3"), Bytes::from("v3"));

        let entry1 = memtable.get(&Bytes::from("key1")).unwrap();
        let entry2 = memtable.get(&Bytes::from("key2")).unwrap();
        let entry3 = memtable.get(&Bytes::from("key3")).unwrap();

        assert_eq!(entry1.seq(), 100);
        assert_eq!(entry2.seq(), 200);
        assert_eq!(entry3.seq(), 150);

        // Update key1 with higher seq
        memtable.put(250, Bytes::from("key1"), Bytes::from("v1_new"));
        let entry1_updated = memtable.get(&Bytes::from("key1")).unwrap();
        assert_eq!(entry1_updated.seq(), 250);
    }

    #[test]
    fn test_memtable_multiple_updates_same_key() {
        let mut memtable = MemTable::new();

        let key = Bytes::from("counter");

        // Simulate multiple updates
        for seq in 1..=10 {
            let value = format!("value_{}", seq);
            memtable.put(seq, key.clone(), Bytes::from(value));
        }

        // Should only keep the latest version
        let entry = memtable.get(&key).unwrap();
        assert_eq!(entry.seq(), 10);
        match entry.val() {
            ValueType::Normal(data) => assert_eq!(data.as_ref(), b"value_10"),
            _ => panic!("Expected Normal value"),
        }

        // Snapshot should contain only 1 entry
        let snapshot = memtable.snapshot();
        assert_eq!(snapshot.len(), 1);
    }

    #[test]
    fn test_memtable_mixed_operations() {
        let mut memtable = MemTable::new();

        // PUT
        memtable.put(1, Bytes::from("user:1"), Bytes::from("Alice"));
        memtable.put(2, Bytes::from("user:2"), Bytes::from("Bob"));
        memtable.put(3, Bytes::from("user:3"), Bytes::from("Charlie"));

        // DELETE
        memtable.delete(4, Bytes::from("user:2"));

        // UPDATE
        memtable.put(5, Bytes::from("user:1"), Bytes::from("Alice Updated"));

        // PUT new key
        memtable.put(6, Bytes::from("user:4"), Bytes::from("Diana"));

        let snapshot = memtable.snapshot();
        assert_eq!(snapshot.len(), 4); // user:1, user:2(tombstone), user:3, user:4

        // Verify user:1 was updated
        let user1 = memtable.get(&Bytes::from("user:1")).unwrap();
        assert_eq!(user1.seq(), 5);
        match user1.val() {
            ValueType::Normal(data) => assert_eq!(data.as_ref(), b"Alice Updated"),
            _ => panic!("Expected Normal value"),
        }

        // Verify user:2 is tombstone
        let user2 = memtable.get(&Bytes::from("user:2")).unwrap();
        assert!(user2.is_tombstone());
    }

    #[test]
    fn test_memtable_default_trait() {
        let memtable: MemTable = Default::default();
        assert_eq!(memtable.size(), 0);
        assert_eq!(memtable.snapshot().len(), 0);
    }

    #[test]
    fn test_memtable_size_consistency_after_many_operations() {
        let mut memtable = MemTable::new();

        // Track expected size manually
        let mut expected_size = 0u64;

        // Insert 100 entries
        for i in 0..100 {
            let key = Bytes::from(format!("key_{:03}", i));
            let value = Bytes::from(format!("value_{:03}", i));
            let entry_size = key.len() + value.len() + ENTRY_METADATA_SIZE;
            expected_size += entry_size as u64;

            memtable.put(i, key, value);
        }

        assert_eq!(memtable.size(), expected_size);

        // Update 50 entries (size should change)
        for i in 0..50 {
            let key = Bytes::from(format!("key_{:03}", i));
            let old_value_len = format!("value_{:03}", i).len();
            let new_value = Bytes::from("updated");

            expected_size -= old_value_len as u64;
            expected_size += new_value.len() as u64;

            memtable.put(100 + i, key, new_value);
        }

        assert_eq!(memtable.size(), expected_size);

        // Delete 25 entries (tombstones have no value data)
        for i in 50..75 {
            let key = Bytes::from(format!("key_{:03}", i));
            let old_value_len = format!("value_{:03}", i).len();

            expected_size -= old_value_len as u64;

            memtable.delete(150 + i, key);
        }

        assert_eq!(memtable.size(), expected_size);
    }
}
