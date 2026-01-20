/// 测试专用代码生成脚本
/// 
/// 说明：为 tests 目录生成仅包含客户端的 Protobuf 代码，
/// 便于集成测试直接引入 gRPC 客户端。
fn main() {
    let out_dir = std::path::PathBuf::from("tests");
    
    tonic_build::configure()
        .build_server(false)
        .build_client(true)
        .out_dir(&out_dir)
        .compile_protos(
            &["../../proto/boxkv.proto"],
            &["../../proto"],
        )
        .unwrap();
    
    println!("测试客户端代码生成完成");
}
