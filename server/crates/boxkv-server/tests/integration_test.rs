use tonic::transport::Channel;

use boxkv_server::generated::boxkv::box_kv_client::BoxKvClient;
use boxkv_server::generated::boxkv::{
    DeleteRequest, GetRequest, OpType, PutRequest, WriteBatchOp, WriteBatchRequest,
};
use boxkv_server::test_support::spawn_server_for_test;

async fn connect_with_retry(addr: std::net::SocketAddr) -> BoxKvClient<Channel> {
    let url = format!("http://{}", addr);
    let mut last_err = None;
    for _ in 0..50u32 {
        match BoxKvClient::connect(url.clone()).await {
            Ok(c) => return c,
            Err(e) => {
                last_err = Some(e);
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        }
    }
    panic!("连接失败: {:?}", last_err);
}

#[tokio::test]
async fn test_put_and_get() {
    let handle = spawn_server_for_test().await.expect("启动服务失败");
    let mut client: BoxKvClient<Channel> = connect_with_retry(handle.addr).await;
    let put = PutRequest {
        key: b"test_key".to_vec(),
        value: b"test_value".to_vec(),
        expiry: None,
        plan: None,
    };
    client.put(put).await.expect("Put 失败");
    let get = GetRequest {
        key: b"test_key".to_vec(),
        plan: None,
    };
    let resp = client.get(get).await.expect("Get 失败").into_inner();
    assert!(resp.found);
    assert_eq!(resp.value, b"test_value");
    let _ = handle.shutdown_tx.send(());
}

#[tokio::test]
async fn test_delete() {
    let handle = spawn_server_for_test().await.expect("启动服务失败");
    let mut client: BoxKvClient<Channel> = connect_with_retry(handle.addr).await;
    let put = PutRequest {
        key: b"del_key".to_vec(),
        value: b"del_value".to_vec(),
        expiry: None,
        plan: None,
    };
    client.put(put).await.expect("Put 失败");
    let del = DeleteRequest {
        key: b"del_key".to_vec(),
        plan: None,
    };
    client.delete(del).await.expect("Delete 失败");
    let get = GetRequest {
        key: b"del_key".to_vec(),
        plan: None,
    };
    let resp = client.get(get).await.expect("Get 失败").into_inner();
    assert!(!resp.found);
    let _ = handle.shutdown_tx.send(());
}

#[tokio::test]
async fn test_write_batch() {
    let handle = spawn_server_for_test().await.expect("启动服务失败");
    let mut client: BoxKvClient<Channel> = connect_with_retry(handle.addr).await;
    let batch = WriteBatchRequest {
        ops: vec![
            WriteBatchOp {
                op: OpType::OpPut as i32,
                key: b"b1".to_vec(),
                value: b"v1".to_vec(),
                expiry: None,
            },
            WriteBatchOp {
                op: OpType::OpPut as i32,
                key: b"b2".to_vec(),
                value: b"v2".to_vec(),
                expiry: None,
            },
        ],
        plan: None,
    };
    client.write_batch(batch).await.expect("WriteBatch 失败");
    let get = GetRequest {
        key: b"b1".to_vec(),
        plan: None,
    };
    let resp = client.get(get).await.expect("Get 失败").into_inner();
    assert!(resp.found);
    assert_eq!(resp.value, b"v1");
    let _ = handle.shutdown_tx.send(());
}
