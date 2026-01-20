//! 句柄表管理
use boxkv_core::hooks::WriteCommand;
use bytes::Bytes;
use slab::Slab;

/// 句柄表
pub struct HandleTable {
    /// Key 数据（固定句柄 1）
    key: Bytes,

    /// Value 数据（固定句柄 2）
    value: Bytes,

    /// 过期时间
    expires_at: Option<u64>,

    /// 动态句柄表（kv_get 返回值）
    dynamic: Slab<Bytes>,
}

impl HandleTable {
    pub fn new() -> Self {
        Self {
            key: Bytes::new(),
            value: Bytes::new(),
            expires_at: None,
            dynamic: Slab::new(),
        }
    }

    /// 注册 key
    pub fn register_key(&mut self, key: Bytes) {
        self.key = key;
    }

    /// 注册 value
    pub fn register_value(&mut self, value: Bytes) {
        self.value = value;
    }

    /// 设置过期时间
    pub fn set_expires_at(&mut self, ts: u64) {
        self.expires_at = Some(ts);
    }

    /// 注册动态句柄
    pub fn register_dynamic(&mut self, data: Bytes) -> Result<u32, i32> {
        let idx = self.dynamic.insert(data);
        Ok((100 + idx) as u32)
    }

    /// 获取 key 句柄
    pub fn key_handle(&self) -> u32 {
        1
    }

    /// 获取 value 句柄
    pub fn value_handle(&self) -> u32 {
        2
    }

    /// 获取 value 类型
    pub fn value_kind(&self) -> u32 {
        if self.value.is_empty() {
            2 // Tombstone
        } else if self.expires_at.is_some() {
            1 // Expiring
        } else {
            0 // Normal
        }
    }

    /// 获取过期时间
    pub fn expires_at(&self) -> u64 {
        self.expires_at.unwrap_or(0)
    }

    /// 获取过期时间（Option 形式）
    pub fn expires_at_opt(&self) -> Option<u64> {
        self.expires_at
    }

    /// 获取 key 字节（公开给 runtime）
    pub fn key_bytes(&self) -> &Bytes {
        &self.key
    }

    /// 获取 value 字节（公开给 runtime）
    pub fn value_bytes(&self) -> &Bytes {
        &self.value
    }

    /// 获取数据引用
    fn get_data(&self, handle: u32) -> Result<&Bytes, i32> {
        match handle {
            1 => Ok(&self.key),
            2 => Ok(&self.value),
            h if h >= 100 => {
                let idx = (h - 100) as usize;
                self.dynamic.get(idx).ok_or(-1) // 无效句柄
            }
            _ => Err(-1), // 无效句柄
        }
    }

    /// 字节长度
    pub fn bytes_len(&self, handle: u32) -> Result<u32, i32> {
        self.get_data(handle).map(|d| d.len() as u32)
    }

    /// 读取字节范围
    pub fn bytes_read(
        &self,
        handle: u32,
        offset: u32,
        len: u32,
        dst: &mut [u8],
    ) -> Result<u32, i32> {
        let data = self.get_data(handle)?;

        let start = offset as usize;
        if start > data.len() {
            return Err(-3); // 越界
        }

        let end = (offset + len).min(data.len() as u32) as usize;
        let actual_len = end - start;

        if dst.len() < actual_len {
            return Err(-2); // 目标缓冲不足
        }

        dst[..actual_len].copy_from_slice(&data[start..end]);
        Ok(actual_len as u32)
    }

    /// 前缀匹配
    pub fn bytes_starts_with(&self, handle: u32, prefix: &[u8]) -> Result<i32, i32> {
        let data = self.get_data(handle)?;
        Ok(if data.starts_with(prefix) { 1 } else { 0 })
    }

    /// 相等比较
    pub fn bytes_equals(&self, handle: u32, needle: &[u8]) -> Result<i32, i32> {
        let data = self.get_data(handle)?;
        Ok(if data.as_ref() == needle { 1 } else { 0 })
    }

    /// 查找子串
    pub fn bytes_find(&self, handle: u32, needle: &[u8], start_off: u32) -> Result<i32, i32> {
        let data = self.get_data(handle)?;

        let start = start_off as usize;
        if start > data.len() {
            return Ok(-1); // 未找到
        }

        if needle.is_empty() {
            return Ok(start as i32);
        }

        data[start..]
            .windows(needle.len())
            .position(|window| window == needle)
            .map(|pos| (start + pos) as i32)
            .ok_or(0)
            .or(Ok(-1))
    }

    /// 关闭句柄
    pub fn close(&mut self, handle: u32) -> Result<(), i32> {
        if handle >= 100 {
            let idx = (handle - 100) as usize;
            if self.dynamic.contains(idx) {
                self.dynamic.remove(idx);
                Ok(())
            } else {
                Err(-1) // 无效句柄
            }
        } else {
            // key/value 句柄不需要关闭
            Ok(())
        }
    }

    /// 应用变换指令（管道语义）
    pub fn apply_commands(&mut self, commands: &[WriteCommand]) {
        for cmd in commands {
            match cmd {
                WriteCommand::SetKey(k) => {
                    self.key = k.clone();
                }
                WriteCommand::SetValue(v) => {
                    self.value = v.clone();
                }
                WriteCommand::SetTTL(ttl) => {
                    let now = boxkv_common::time::current_timestamp_secs();
                    self.expires_at = Some(now + ttl);
                }
                WriteCommand::SetExpiresAt(ts) => {
                    self.expires_at = Some(*ts);
                }
                WriteCommand::ClearTTL => {
                    self.expires_at = None;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_handle_basic() {
        let mut table = HandleTable::new();
        table.register_key(Bytes::from("key"));
        table.register_value(Bytes::from("value"));

        assert_eq!(table.key_handle(), 1);
        assert_eq!(table.value_handle(), 2);
        assert_eq!(table.bytes_len(1).unwrap(), 3);
        assert_eq!(table.bytes_len(2).unwrap(), 5);
    }

    #[test]
    fn test_bytes_read() {
        let mut table = HandleTable::new();
        table.register_key(Bytes::from("hello"));

        let mut buf = vec![0u8; 5];
        let n = table.bytes_read(1, 0, 5, &mut buf).unwrap();
        assert_eq!(n, 5);
        assert_eq!(&buf, b"hello");

        // 越界读取
        let n = table.bytes_read(1, 0, 10, &mut buf).unwrap();
        assert_eq!(n, 5); // 只返回实际长度
    }

    #[test]
    fn test_dynamic_handle() {
        let mut table = HandleTable::new();
        let h1 = table.register_dynamic(Bytes::from("data1")).unwrap();
        let h2 = table.register_dynamic(Bytes::from("data2")).unwrap();

        assert_eq!(h1, 100);
        assert_eq!(h2, 101);
        assert_eq!(table.bytes_len(h1).unwrap(), 5);
        assert_eq!(table.bytes_len(h2).unwrap(), 5);

        // 关闭句柄
        table.close(h1).unwrap();
        assert!(table.bytes_len(h1).is_err());
    }
}
