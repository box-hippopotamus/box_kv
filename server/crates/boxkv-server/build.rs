// 说明：构建时生成 gRPC 代码与反射所需的 descriptor，输出到 src/generated。
// 提示：仅在 proto 变更时重新运行，避免无谓的重复生成。
fn main() {
    let out_dir = std::path::PathBuf::from("src/generated");
    let _ = std::fs::create_dir_all(&out_dir);

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .out_dir(&out_dir)
        .file_descriptor_set_path(out_dir.join("descriptor.bin"))
        .compile_protos(&["../../proto/boxkv.proto"], &["../../proto"])
        .unwrap();

    println!("cargo:rerun-if-changed=../../proto/boxkv.proto");
}
