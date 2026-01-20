mod common;

use boxkv_core::{BoxKV, WriteBatch};
use bytes::Bytes;
use common::*;
use std::sync::Arc;
use std::thread;

#[test]
fn concurrent_put_and_get() {
    init_global();
    let tmp = new_tmp();
    let db = Arc::new(open_db(tmp.path()));

    let threads = 4;
    let per_thread = 2000;

    let mut handles = Vec::new();
    for t in 0..threads {
        let db = db.clone();
        let h = thread::spawn(move || {
            for i in 0..per_thread {
                let k = format!("k-{}-{}", t, i);
                let v = format!("v-{}-{}", t, i);
                db.put(Bytes::from(k), Bytes::from(v), &empty_plan())
                    .unwrap();
            }
        });
        handles.push(h);
    }

    for h in handles {
        h.join().unwrap();
    }

    // Validate few samples
    for t in 0..threads {
        for i in [0, per_thread / 2, per_thread - 1] {
            let k = format!("k-{}-{}", t, i);
            let v = format!("v-{}-{}", t, i);
            assert_eq!(
                db.get(Bytes::from(k), &empty_plan()).unwrap(),
                Some(Bytes::from(v))
            );
        }
    }
}

#[test]
fn concurrent_write_batches() {
    init_global();
    let tmp = new_tmp();
    let db = Arc::new(open_db(tmp.path()));

    let threads = 4;
    let rounds = 20;
    let batch_size = 200;

    let mut handles = Vec::new();
    for t in 0..threads {
        let db = db.clone();
        let h = thread::spawn(move || {
            for r in 0..rounds {
                let mut wb = WriteBatch::new();
                for i in 0..batch_size {
                    let k = format!("bk-{}-{}-{}", t, r, i);
                    let v = format!("bv-{}-{}-{}", t, r, i);
                    wb.put(Bytes::from(k), Bytes::from(v));
                }
                db.write(wb, &empty_plan()).unwrap();
            }
        });
        handles.push(h);
    }

    for h in handles {
        h.join().unwrap();
    }

    // Validate some keys exist
    for t in 0..threads {
        for r in [0, rounds - 1] {
            let k = format!("bk-{}-{}-0", t, r);
            assert!(db.get(Bytes::from(k), &empty_plan()).unwrap().is_some());
        }
    }
}
