//! 句柄操作 Host 函数

use crate::context::CallContext;
use crate::error::Result;
use wasmtime::{Caller, Linker};

pub fn register(linker: &mut Linker<CallContext>) -> Result<()> {
    let ns = "boxkv_host";

    // 上下文句柄
    linker.func_wrap(
        ns,
        "ctx_key_handle",
        |caller: Caller<'_, CallContext>| -> u32 { caller.data().ctx_key_handle() },
    )?;

    linker.func_wrap(
        ns,
        "ctx_value_handle",
        |caller: Caller<'_, CallContext>| -> u32 { caller.data().ctx_value_handle() },
    )?;

    linker.func_wrap(
        ns,
        "ctx_value_kind",
        |caller: Caller<'_, CallContext>| -> u32 { caller.data().ctx_value_kind() },
    )?;

    linker.func_wrap(
        ns,
        "ctx_expires_at",
        |caller: Caller<'_, CallContext>| -> u64 { caller.data().ctx_expires_at() },
    )?;

    linker.func_wrap(
        ns,
        "ctx_sequence",
        |caller: Caller<'_, CallContext>| -> u64 { caller.data().ctx_sequence() },
    )?;

    // 字节操作
    linker.func_wrap(
        ns,
        "bytes_len",
        |caller: Caller<'_, CallContext>, handle: u32| -> i32 {
            match caller.data().bytes_len(handle) {
                Ok(len) => len as i32,
                Err(code) => code,
            }
        },
    )?;

    linker.func_wrap(
        ns,
        "bytes_read",
        |mut caller: Caller<'_, CallContext>,
         handle: u32,
         offset: u32,
         len: u32,
         dst_ptr: u32|
         -> i32 { bytes_read_impl(&mut caller, handle, offset, len, dst_ptr) },
    )?;

    linker.func_wrap(
        ns,
        "bytes_starts_with",
        |mut caller: Caller<'_, CallContext>,
         handle: u32,
         needle_ptr: u32,
         needle_len: u32|
         -> i32 { bytes_starts_with_impl(&mut caller, handle, needle_ptr, needle_len) },
    )?;

    linker.func_wrap(
        ns,
        "bytes_equals",
        |mut caller: Caller<'_, CallContext>,
         handle: u32,
         needle_ptr: u32,
         needle_len: u32|
         -> i32 { bytes_equals_impl(&mut caller, handle, needle_ptr, needle_len) },
    )?;

    linker.func_wrap(
        ns,
        "bytes_find",
        |mut caller: Caller<'_, CallContext>,
         handle: u32,
         needle_ptr: u32,
         needle_len: u32,
         start_off: u32|
         -> i32 { bytes_find_impl(&mut caller, handle, needle_ptr, needle_len, start_off) },
    )?;

    linker.func_wrap(
        ns,
        "bytes_close",
        |mut caller: Caller<'_, CallContext>, handle: u32| -> i32 {
            caller
                .data_mut()
                .bytes_close(handle)
                .map(|_| 0)
                .unwrap_or(-1)
        },
    )?;

    Ok(())
}

fn bytes_read_impl(
    caller: &mut Caller<'_, CallContext>,
    handle: u32,
    offset: u32,
    len: u32,
    dst_ptr: u32,
) -> i32 {
    let memory = match caller.get_export("memory").and_then(|e| e.into_memory()) {
        Some(m) => m,
        None => return -4,
    };

    let mut buf = vec![0u8; len as usize];
    let actual = match caller.data_mut().bytes_read(handle, offset, len, &mut buf) {
        Ok(n) => n,
        Err(code) => return code,
    };

    if let Err(e) = memory.write(&mut *caller, dst_ptr as usize, &buf[..actual as usize]) {
        tracing::error!("Failed to write to guest memory: {}", e);
        return -2;
    }

    actual as i32
}

fn bytes_starts_with_impl(
    caller: &mut Caller<'_, CallContext>,
    handle: u32,
    needle_ptr: u32,
    needle_len: u32,
) -> i32 {
    let memory = match caller.get_export("memory").and_then(|e| e.into_memory()) {
        Some(m) => m,
        None => return -4,
    };

    let needle = match read_guest_memory(caller, &memory, needle_ptr, needle_len) {
        Ok(n) => n,
        Err(code) => return code,
    };

    caller
        .data()
        .bytes_starts_with(handle, &needle)
        .unwrap_or(-1)
}

fn bytes_equals_impl(
    caller: &mut Caller<'_, CallContext>,
    handle: u32,
    needle_ptr: u32,
    needle_len: u32,
) -> i32 {
    let memory = match caller.get_export("memory").and_then(|e| e.into_memory()) {
        Some(m) => m,
        None => return -4,
    };

    let needle = match read_guest_memory(caller, &memory, needle_ptr, needle_len) {
        Ok(n) => n,
        Err(code) => return code,
    };

    caller.data().bytes_equals(handle, &needle).unwrap_or(-1)
}

fn bytes_find_impl(
    caller: &mut Caller<'_, CallContext>,
    handle: u32,
    needle_ptr: u32,
    needle_len: u32,
    start_off: u32,
) -> i32 {
    let memory = match caller.get_export("memory").and_then(|e| e.into_memory()) {
        Some(m) => m,
        None => return -4,
    };

    let needle = match read_guest_memory(caller, &memory, needle_ptr, needle_len) {
        Ok(n) => n,
        Err(code) => return code,
    };

    caller
        .data()
        .bytes_find(handle, &needle, start_off)
        .unwrap_or(-1)
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
