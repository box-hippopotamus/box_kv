//! 读路径 Host 函数（OnRead/ScanFilter）

use crate::context::CallContext;
use crate::error::Result;
use wasmtime::{Caller, Linker};

pub fn register(linker: &mut Linker<CallContext>) -> Result<()> {
    let ns = "boxkv_host";

    // OnRead: 设置变换后的值
    linker.func_wrap(
        ns,
        "read_set_value",
        |mut caller: Caller<'_, CallContext>, ptr: u32, len: u32| -> i32 {
            read_set_value_impl(&mut caller, ptr, len)
        },
    )?;

    // OnRead: 设置拒绝原因
    linker.func_wrap(
        ns,
        "read_set_reason",
        |mut caller: Caller<'_, CallContext>, ptr: u32, len: u32| -> i32 {
            read_set_reason_impl(&mut caller, ptr, len)
        },
    )?;

    // ScanFilter: 标记为 Drop
    linker.func_wrap(
        ns,
        "scan_set_drop",
        |mut caller: Caller<'_, CallContext>| -> i32 {
            caller.data_mut().read_set_drop();
            0
        },
    )?;

    Ok(())
}

fn read_set_value_impl(caller: &mut Caller<'_, CallContext>, ptr: u32, len: u32) -> i32 {
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
        .read_set_transformed_value(bytes::Bytes::from(value));
    0
}

fn read_set_reason_impl(caller: &mut Caller<'_, CallContext>, ptr: u32, len: u32) -> i32 {
    let memory = match caller.get_export("memory").and_then(|e| e.into_memory()) {
        Some(m) => m,
        None => return -4,
    };

    let reason = match read_guest_memory(caller, &memory, ptr, len) {
        Ok(r) => r,
        Err(code) => return code,
    };

    let reason_str = String::from_utf8_lossy(&reason).to_string();
    caller.data_mut().read_set_reject_reason(reason_str);
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
