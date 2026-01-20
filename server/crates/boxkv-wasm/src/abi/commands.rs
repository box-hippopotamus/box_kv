//! 命令推送 Host 函数

use crate::context::CallContext;
use crate::error::Result;
use wasmtime::{Caller, Linker};

pub fn register(linker: &mut Linker<CallContext>) -> Result<()> {
    let ns = "boxkv_host";

    linker.func_wrap(
        ns,
        "cmd_set_key",
        |mut caller: Caller<'_, CallContext>, ptr: u32, len: u32| -> i32 {
            cmd_set_key_impl(&mut caller, ptr, len)
        },
    )?;

    linker.func_wrap(
        ns,
        "cmd_set_value",
        |mut caller: Caller<'_, CallContext>, ptr: u32, len: u32| -> i32 {
            cmd_set_value_impl(&mut caller, ptr, len)
        },
    )?;

    linker.func_wrap(
        ns,
        "cmd_set_ttl",
        |mut caller: Caller<'_, CallContext>, ttl_secs: u64| -> i32 {
            caller
                .data_mut()
                .cmd_set_ttl(ttl_secs)
                .map(|_| 0)
                .unwrap_or(-6)
        },
    )?;

    linker.func_wrap(
        ns,
        "cmd_set_expires_at",
        |mut caller: Caller<'_, CallContext>, ts: u64| -> i32 {
            caller
                .data_mut()
                .cmd_set_expires_at(ts)
                .map(|_| 0)
                .unwrap_or(-6)
        },
    )?;

    linker.func_wrap(
        ns,
        "cmd_clear_ttl",
        |mut caller: Caller<'_, CallContext>| -> i32 {
            caller.data_mut().cmd_clear_ttl().map(|_| 0).unwrap_or(-6)
        },
    )?;

    linker.func_wrap(
        ns,
        "cmd_set_reason",
        |mut caller: Caller<'_, CallContext>, ptr: u32, len: u32| -> i32 {
            cmd_set_reason_impl(&mut caller, ptr, len)
        },
    )?;

    Ok(())
}

fn cmd_set_key_impl(caller: &mut Caller<'_, CallContext>, ptr: u32, len: u32) -> i32 {
    let memory = match caller.get_export("memory").and_then(|e| e.into_memory()) {
        Some(m) => m,
        None => return -4,
    };

    let key = match read_guest_memory(caller, &memory, ptr, len) {
        Ok(k) => k,
        Err(code) => return code,
    };

    caller
        .data_mut()
        .cmd_set_key(bytes::Bytes::from(key))
        .map(|_| 0)
        .unwrap_or(-6)
}

fn cmd_set_value_impl(caller: &mut Caller<'_, CallContext>, ptr: u32, len: u32) -> i32 {
    let memory = match caller.get_export("memory").and_then(|e| e.into_memory()) {
        Some(m) => m,
        None => return -4,
    };

    let value = match read_guest_memory(caller, &memory, ptr, len) {
        Ok(v) => v,
        Err(code) => return code,
    };

    caller
        .data_mut()
        .cmd_set_value(bytes::Bytes::from(value))
        .map(|_| 0)
        .unwrap_or(-6)
}

fn cmd_set_reason_impl(caller: &mut Caller<'_, CallContext>, ptr: u32, len: u32) -> i32 {
    let memory = match caller.get_export("memory").and_then(|e| e.into_memory()) {
        Some(m) => m,
        None => return -4,
    };

    let reason = match read_guest_memory(caller, &memory, ptr, len) {
        Ok(r) => r,
        Err(code) => return code,
    };

    let reason_str = String::from_utf8_lossy(&reason).to_string();
    caller.data_mut().cmd_set_reason(reason_str);
    0
}

fn read_guest_memory(
    caller: &mut Caller<'_, CallContext>,
    memory: &wasmtime::Memory,
    ptr: u32,
    len: u32,
) -> std::result::Result<Vec<u8>, i32> {
    let start = ptr as usize;
    let end = start + len as usize;
    let mem_size = memory.data_size(&*caller);

    if end > mem_size {
        return Err(-3);
    }

    let data = memory.data(&*caller).get(start..end).ok_or(-3)?;

    Ok(data.to_vec())
}
