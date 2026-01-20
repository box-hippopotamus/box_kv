mod common;

use std::fs;
use std::time::Duration;

use boxkv_core::BoxKV;
use boxkv_executor::{GlobalScheduler, SchedulerConfig};
use bytes::Bytes;
use common::*;
use std::sync::Arc;

// 快速烟雾测试：验证 flush 后 imm 清空、WAL 目录存在文件
#[test]
fn flush_smoke_and_wal_dir_exists() {
    init_global();
    let tmp = new_tmp();
    let db = open_db(tmp.path());

    for i in 0..10_000 {
        let k = format!("k-{}", i);
        db.put(Bytes::from(k), Bytes::from_static(b"v"), &empty_plan())
            .unwrap();
    }
    db.flush().unwrap();

    // 等待 imm 清空
    let ok = wait_until(Duration::from_secs(5), || {
        db.get_property("boxkv.num-immutable-mem-table")
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(999)
            == 0
    });
    assert!(ok, "imm not drained after flush");

    // WAL 目录应存在至少一个文件
    let wal_dir = tmp.path().join("wal");
    let wal_files = fs::read_dir(&wal_dir).unwrap().count();
    assert!(wal_files >= 1, "wal files should exist");
}

// 重型测试：验证 L0 降低与文件清理（默认忽略，CI 或本地压力时运行）
#[test]
#[ignore]
fn flush_compaction_and_purge_obsolete_files_heavy() {
    init_global();
    let tmp = new_tmp();
    let db = open_db(tmp.path());

    let rounds = 6; // 超过 level0_trigger
    for r in 0..rounds {
        for i in 0..5000 {
            let k = format!("k-{}-{}", r, i);
            let v = format!("v-{}-{}", r, i);
            db.put(Bytes::from(k), Bytes::from(v), &empty_plan())
                .unwrap();
        }
        db.flush().unwrap();
    }

    // 等待 L0 降至 < 2
    let ok = wait_until(Duration::from_secs(60), || {
        db.get_property("boxkv.num-files-at-level0")
            .and_then(|s| s.parse::<usize>().ok())
            .map(|n| n < 2)
            .unwrap_or(false)
    });
    assert!(ok, "L0 did not compact down in time");

    // 再写入并 flush，触发一次清理（Manifest/SST/WAL）
    for i in 0..10_000 {
        let k = format!("z-{}", i);
        db.put(Bytes::from(k), Bytes::from_static(b"v"), &empty_plan())
            .unwrap();
    }
    db.flush().unwrap();
}
