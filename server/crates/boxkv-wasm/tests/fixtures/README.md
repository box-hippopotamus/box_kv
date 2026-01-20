# Wasm Test Fixtures

这个目录包含用于集成测试的预编译 Wasm 模块。

## 文件说明

### simple_transform.wasm
**功能**：简单的 key/value 转换插件
- **PreWrite Hook**：为所有 key 添加前缀 "prefix_"
- **OnRead Hook**：移除前缀 "prefix_"，恢复原始 key

**WAT 源码**：
```wat
(module
  (import "boxkv_host" "get_key" (func $get_key (param i32 i32) (result i32)))
  (import "boxkv_host" "set_key" (func $set_key (param i32 i32)))
  (import "boxkv_host" "get_value" (func $get_value (param i32 i32) (result i32)))
  (memory (export "memory") 1)
  
  ;; PreWrite: 添加前缀
  (func (export "pre_write") (result i32)
    ;; 实现：读取 key，添加 "prefix_" 前缀，写回
    (i32.const 0)  ;; Accept
  )
  
  ;; OnRead: 移除前缀
  (func (export "on_read") (result i32)
    ;; 实现：读取 key，移除 "prefix_" 前缀，写回
    (i32.const 0)  ;; Accept
  )
)
```

### filter.wasm
**功能**：过滤插件，拒绝特定模式的 key
- **PreWrite Hook**：拒绝包含 "forbidden" 的 key
- **OnRead Hook**：拒绝读取包含 "secret" 的 key

**WAT 源码**：
```wat
(module
  (import "boxkv_host" "get_key" (func $get_key (param i32 i32) (result i32)))
  (import "boxkv_host" "reject" (func $reject (param i32 i32)))
  (memory (export "memory") 1)
  
  ;; PreWrite: 检查并拒绝
  (func (export "pre_write") (result i32)
    ;; 实现：检查 key 是否包含 "forbidden"
    ;; 如果包含，返回 2 (Reject)
    ;; 否则返回 0 (Accept)
    (i32.const 0)
  )
  
  ;; OnRead: 检查并拒绝
  (func (export "on_read") (result i32)
    ;; 实现：检查 key 是否包含 "secret"
    ;; 如果包含，返回 1 (Reject)
    ;; 否则返回 0 (Accept)
    (i32.const 0)
  )
)
```

### audit.wasm
**功能**：审计插件，记录所有写操作
- **PostWrite Hook**：记录写操作到日志

**WAT 源码**：
```wat
(module
  (import "boxkv_host" "get_key" (func $get_key (param i32 i32) (result i32)))
  (import "boxkv_host" "get_value" (func $get_value (param i32 i32) (result i32)))
  (memory (export "memory") 1)
  
  ;; PostWrite: 记录日志
  (func (export "post_write")
    ;; 实现：读取 key 和 value，记录到日志
    ;; （实际实现可能需要调用外部日志接口）
  )
)
```

## 编译方法

使用 `wat2wasm` 工具编译：

```bash
# 安装 WABT (WebAssembly Binary Toolkit)
# Ubuntu/Debian: sudo apt-get install wabt
# macOS: brew install wabt
# Windows: 从 https://github.com/WebAssembly/wabt/releases 下载

# 编译
wat2wasm simple_transform.wat -o simple_transform.wasm
wat2wasm filter.wat -o filter.wasm
wat2wasm audit.wat -o audit.wasm
```

## 临时占位符

在实际的 Wasm 模块编译完成之前，这些文件包含最小的有效 Wasm 模块作为占位符。
