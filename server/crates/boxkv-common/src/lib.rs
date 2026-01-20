//! BoxKV 通用库：编解码、配置、基础类型与时间工具。
//!
//! 说明：提供跨组件共享的轻量工具与配置定义，不包含业务逻辑。
pub mod codec;
pub mod config;
pub mod time;
pub mod types;
pub mod varint;
