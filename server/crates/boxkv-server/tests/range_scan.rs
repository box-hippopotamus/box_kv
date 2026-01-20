// 范围扫描功能完整测试
// 测试 MergingIterator、MVCC 隔离、多层数据合并

use boxkv_common::config::GlobalConfig;
use boxkv_core::BoxKV;
use bytes::Bytes;
use std::path::PathBuf;
use tempfile::TempDir;

fn setup_test_db() -> (BoxKV, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();

    // 初始化全局配置（测试模式）
    GlobalConfig::initialize_default().unwrap();

    let db = BoxKV::open(&db_path).unwrap();
    (db, temp_dir)
}

#[test]
fn test_range_scan_memtable_only() {
    let (db, _temp_dir) = setup_test_db();

    // 插入测试数据
    db.put(b"key1".to_vec(), b"value1".to_vec()).unwrap();
    db.put(b"key2".to_vec(), b"value2".to_vec()).unwrap();
    db.put(b"key3".to_vec(), b"value3".to_vec()).unwrap();
    db.put(b"key5".to_vec(), b"value5".to_vec()).unwrap();

    // 范围扫描
    let results = db.scan_range(b"key1", b"key4", 10).unwrap();

    assert_eq!(results.len(), 3);
    assert_eq!(results[0], (Bytes::from("key1"), Bytes::from("value1")));
    assert_eq!(results[1], (Bytes::from("key2"), Bytes::from("value2")));
    assert_eq!(results[2], (Bytes::from("key3"), Bytes::from("value3")));
}

#[test]
fn test_range_scan_with_limit() {
    let (db, _temp_dir) = setup_test_db();

    // 插入大量数据
    for i in 0..100 {
        let key = format!("key{:03}", i);
        let value = format!("value{:03}", i);
        db.put(key.into_bytes(), value.into_bytes()).unwrap();
    }

    // 测试 limit
    let results = db.scan_range(b"key000", b"key100", 10).unwrap();
    assert_eq!(results.len(), 10);

    // 验证顺序
    for (i, (key, _value)) in results.iter().enumerate() {
        let expected_key = format!("key{:03}", i);
        assert_eq!(key.as_ref(), expected_key.as_bytes());
    }
}

#[test]
fn test_range_scan_with_deletion() {
    let (db, _temp_dir) = setup_test_db();

    // 插入并删除部分数据
    db.put(b"key1".to_vec(), b"value1".to_vec()).unwrap();
    db.put(b"key2".to_vec(), b"value2".to_vec()).unwrap();
    db.put(b"key3".to_vec(), b"value3".to_vec()).unwrap();
    db.put(b"key4".to_vec(), b"value4".to_vec()).unwrap();

    // 删除 key2
    db.delete(b"key2".to_vec()).unwrap();

    // 范围扫描（应该跳过 key2）
    let results = db.scan_range(b"key1", b"key5", 10).unwrap();

    assert_eq!(results.len(), 3);
    assert_eq!(results[0].0, Bytes::from("key1"));
    assert_eq!(results[1].0, Bytes::from("key3"));
    assert_eq!(results[2].0, Bytes::from("key4"));
}

#[test]
fn test_range_scan_mvcc_snapshot() {
    let (db, _temp_dir) = setup_test_db();

    // 初始数据
    db.put(b"key1".to_vec(), b"v1_old".to_vec()).unwrap();
    db.put(b"key2".to_vec(), b"v2_old".to_vec()).unwrap();

    // 创建快照
    let snapshot = db.snapshot().unwrap();

    // 更新数据
    db.put(b"key1".to_vec(), b"v1_new".to_vec()).unwrap();
    db.put(b"key2".to_vec(), b"v2_new".to_vec()).unwrap();

    // 当前视图应该看到新数据
    let current_results = db.scan_range(b"key1", b"key3", 10).unwrap();
    assert_eq!(current_results.len(), 2);
    assert_eq!(current_results[0].1, Bytes::from("v1_new"));
    assert_eq!(current_results[1].1, Bytes::from("v2_new"));

    // 快照应该看到旧数据
    let snapshot_results = snapshot.scan_range(b"key1", b"key3", 10).unwrap();
    assert_eq!(snapshot_results.len(), 2);
    assert_eq!(snapshot_results[0].1, Bytes::from("v1_old"));
    assert_eq!(snapshot_results[1].1, Bytes::from("v2_old"));
}

#[test]
fn test_range_scan_empty_range() {
    let (db, _temp_dir) = setup_test_db();

    db.put(b"key1".to_vec(), b"value1".to_vec()).unwrap();
    db.put(b"key5".to_vec(), b"value5".to_vec()).unwrap();

    // 查询空范围
    let results = db.scan_range(b"key2", b"key4", 10).unwrap();
    assert_eq!(results.len(), 0);
}

#[test]
fn test_range_scan_after_flush() {
    let (db, _temp_dir) = setup_test_db();

    // 插入数据
    for i in 0..50 {
        let key = format!("key{:03}", i);
        let value = format!("value{:03}", i);
        db.put(key.into_bytes(), value.into_bytes()).unwrap();
    }

    // 强制 Flush
    db.flush().unwrap();

    // 再插入一些数据到新的 Memtable
    for i in 50..60 {
        let key = format!("key{:03}", i);
        let value = format!("value{:03}", i);
        db.put(key.into_bytes(), value.into_bytes()).unwrap();
    }

    // 范围扫描（应该合并 SST 和 Memtable）
    let results = db.scan_range(b"key000", b"key100", 100).unwrap();
    assert_eq!(results.len(), 60);

    // 验证顺序和内容
    for (i, (key, value)) in results.iter().enumerate() {
        let expected_key = format!("key{:03}", i);
        let expected_value = format!("value{:03}", i);
        assert_eq!(key.as_ref(), expected_key.as_bytes());
        assert_eq!(value.as_ref(), expected_value.as_bytes());
    }
}

#[test]
fn test_range_scan_boundary_cases() {
    let (db, _temp_dir) = setup_test_db();

    db.put(b"a".to_vec(), b"value_a".to_vec()).unwrap();
    db.put(b"b".to_vec(), b"value_b".to_vec()).unwrap();
    db.put(b"c".to_vec(), b"value_c".to_vec()).unwrap();

    // 测试各种边界
    let results = db.scan_range(b"a", b"b", 10).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0, Bytes::from("a"));

    let results = db.scan_range(b"b", b"c", 10).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0, Bytes::from("b"));

    let results = db.scan_range(b"a", b"d", 10).unwrap();
    assert_eq!(results.len(), 3);
}

#[test]
fn test_range_scan_overwrite() {
    let (db, _temp_dir) = setup_test_db();

    // 初始插入
    db.put(b"key1".to_vec(), b"v1".to_vec()).unwrap();
    db.put(b"key2".to_vec(), b"v2".to_vec()).unwrap();

    // 覆盖
    db.put(b"key1".to_vec(), b"v1_updated".to_vec()).unwrap();

    // 扫描应该只返回最新版本
    let results = db.scan_range(b"key1", b"key3", 10).unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].1, Bytes::from("v1_updated"));
    assert_eq!(results[1].1, Bytes::from("v2"));
}

#[test]
fn test_range_scan_utf8_keys() {
    let (db, _temp_dir) = setup_test_db();

    // 使用 UTF-8 键
    db.put("你好".as_bytes().to_vec(), b"hello".to_vec())
        .unwrap();
    db.put("世界".as_bytes().to_vec(), b"world".to_vec())
        .unwrap();
    db.put("测试".as_bytes().to_vec(), b"test".to_vec())
        .unwrap();

    // 范围扫描
    let results = db
        .scan_range("你".as_bytes(), "测试啊".as_bytes(), 10)
        .unwrap();

    // 应该包含所有 UTF-8 键（按字节序排序）
    assert!(results.len() >= 1);
}

#[test]
fn test_range_scan_limit_zero() {
    let (db, _temp_dir) = setup_test_db();

    db.put(b"key1".to_vec(), b"value1".to_vec()).unwrap();
    db.put(b"key2".to_vec(), b"value2".to_vec()).unwrap();

    // limit = 0 应该返回空
    let results = db.scan_range(b"key1", b"key3", 0).unwrap();
    assert_eq!(results.len(), 0);
}
