mod common;

use std::fs;

use boxkv_common::time::current_timestamp_secs;
use boxkv_common::types::ValueType;
use boxkv_core::wal::Wal;
use boxkv_storage::LocalFileSystem;
use bytes::Bytes;
use common::*;
use tempfile::TempDir;

#[test]
fn test_wal_recovery() {
    // Initialize global config
    init_global();

    // Setup temp directory
    let tmp = TempDir::new().expect("create tmp dir");
    let wal_dir = tmp.path().join("wal");
    fs::create_dir_all(&wal_dir).expect("mkdir wal");

    // 1) Write WAL directly (simulate crash before flush)
    let fs_local = LocalFileSystem;
    let mut wal = Wal::create(&fs_local, wal_dir.clone(), 1).expect("create wal");

    let now = current_timestamp_secs();
    wal.append_normal(10, Bytes::from("k1"), Bytes::from("v1"))
        .expect("append normal");
    wal.append_tombstone(20, Bytes::from("k2"))
        .expect("append tombstone");
    wal.append_expire(30, Bytes::from("k3"), Bytes::from("v3"), now + 3600)
        .expect("append expire");
    wal.sync().expect("sync wal");

    // 2) Open DB and verify recovery logic replays WAL into memtable
    let db = open_db(tmp.path());

    assert_eq!(
        db.get(Bytes::from("k1"), &empty_plan()).unwrap(),
        Some(Bytes::from("v1"))
    );
    assert_eq!(db.get(Bytes::from("k2"), &empty_plan()).unwrap(), None); // tombstone
    assert_eq!(
        db.get(Bytes::from("k3"), &empty_plan()).unwrap(),
        Some(Bytes::from("v3"))
    ); // not expired
}
