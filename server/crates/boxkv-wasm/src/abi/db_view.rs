//! DB 视图访问 Host 函数

use crate::context::CallContext;
use crate::error::Result;
use wasmtime::{Caller, Linker};

pub fn register(linker: &mut Linker<CallContext>) -> Result<()> {
    let ns = "boxkv_host";

    linker.func_wrap(
        ns,
        "db_open_value_handle",
        |mut caller: Caller<'_, CallContext>, key_ptr: u32, key_len: u32| -> i32 {
            db_open_value_handle_impl(&mut caller, key_ptr, key_len)
        },
    )?;

    Ok(())
}

fn db_open_value_handle_impl(
    caller: &mut Caller<'_, CallContext>,
    key_ptr: u32,
    key_len: u32,
) -> i32 {
    let memory = match caller.get_export("memory").and_then(|e| e.into_memory()) {
        Some(m) => m,
        None => return -4,
    };

    let start = key_ptr as usize;
    let end = start + key_len as usize;
    let mem_size = memory.data_size(&*caller);

    if end > mem_size {
        return -3;
    }

    let key = match memory.data(&*caller).get(start..end) {
        Some(k) => k.to_vec(),
        None => return -3,
    };

    caller.data_mut().db_open_value_handle(&key).unwrap_or(-4)
}
