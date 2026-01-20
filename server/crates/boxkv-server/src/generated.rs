/// 预生成的 Protobuf 代码
///
/// 说明：此处仅做聚合与导出；如需更新，请使用构建脚本重新生成。

#[allow(clippy::all)]
#[allow(warnings)]
pub mod boxkv {
    include!("generated/boxkv.v1.rs");
}

/// gRPC 反射所需的描述符（用于服务自省）
pub const FILE_DESCRIPTOR_SET: &[u8] = include_bytes!("generated/descriptor.bin");
