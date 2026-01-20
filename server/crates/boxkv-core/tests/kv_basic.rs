mod common;

use boxkv_core::{BoxKV, WriteBatch};
use bytes::Bytes;
use common::*;

#[test]
fn kv_put_get_delete_batch_flush_reopen_ttl_snapshot() {
    init_global();
    let tmp = new_tmp();

    let db = open_db(tmp.path());

    // put/get
    db.put(Bytes::from("k1"), Bytes::from("v1"), &empty_plan())
        .unwrap();
    assert_eq!(
        db.get(Bytes::from("k1"), &empty_plan()).unwrap(),
        Some(Bytes::from("v1"))
    );
    assert_eq!(db.get(Bytes::from("k-miss"), &empty_plan()).unwrap(), None);

    // delete
    db.delete(Bytes::from("k1"), &empty_plan()).unwrap();
    assert_eq!(db.get(Bytes::from("k1"), &empty_plan()).unwrap(), None);

    // write batch
    let mut wb = WriteBatch::new();
    wb.put(Bytes::from("a1"), Bytes::from("x1"));
    wb.put(Bytes::from("a2"), Bytes::from("x2"));
    wb.delete(Bytes::from("a3"));
    db.write(wb, &empty_plan()).unwrap();
    assert_eq!(
        db.get(Bytes::from("a1"), &empty_plan()).unwrap(),
        Some(Bytes::from("x1"))
    );
    assert_eq!(
        db.get(Bytes::from("a2"), &empty_plan()).unwrap(),
        Some(Bytes::from("x2"))
    );
    assert_eq!(db.get(Bytes::from("a3"), &empty_plan()).unwrap(), None);

    // snapshot（多版本 Memtable 支持 MVCC 隔离）
    db.put(Bytes::from("s1"), Bytes::from("v1"), &empty_plan())
        .unwrap();
    let snap = db.snapshot().unwrap();
    db.put(Bytes::from("s1"), Bytes::from("v2"), &empty_plan())
        .unwrap();

    // DB 应看到最新值
    assert_eq!(
        db.get(Bytes::from("s1"), &empty_plan()).unwrap(),
        Some(Bytes::from("v2"))
    );
    // Snapshot 应看到旧版本（快照隔离）
    assert_eq!(snap.get(Bytes::from("s1")), Some(Bytes::from("v1")));

    // ttl
    db.put_with_ttl(Bytes::from("t1"), Bytes::from("tv"), 1, &empty_plan())
        .unwrap();
    assert_eq!(
        db.get(Bytes::from("t1"), &empty_plan()).unwrap(),
        Some(Bytes::from("tv"))
    );
    sleep_ms(1200);
    assert_eq!(db.get(Bytes::from("t1"), &empty_plan()).unwrap(), None);

    // flush and reopen
    db.flush().unwrap();
    drop(db);
    let db2 = open_db(tmp.path());
    assert_eq!(
        db2.get(Bytes::from("a1"), &empty_plan()).unwrap(),
        Some(Bytes::from("x1"))
    );
    assert_eq!(
        db2.get(Bytes::from("a2"), &empty_plan()).unwrap(),
        Some(Bytes::from("x2"))
    );
}
