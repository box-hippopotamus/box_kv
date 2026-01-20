//! Host ABI - Wasm 可调用函数注册

mod commands;
mod db_view;
mod handle_ops;
mod read_ops;

pub mod catalog;

use crate::context::CallContext;
use crate::error::Result;
use wasmtime::Linker;

/// Host ABI 注册器
pub struct HostAbi;

impl HostAbi {
    /// 注册所有 Host 函数
    pub fn register_all(linker: &mut Linker<CallContext>) -> Result<()> {
        handle_ops::register(linker)?;
        db_view::register(linker)?;
        commands::register(linker)?;
        read_ops::register(linker)?;

        tracing::info!("Host ABI registered to namespace 'boxkv_host'");
        Ok(())
    }
}
