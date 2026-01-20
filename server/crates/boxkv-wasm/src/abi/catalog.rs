//! ABI 函数目录 - 统一管理允许的 Host 函数

/// Host ABI 命名空间
pub const HOST_NAMESPACE: &str = "boxkv_host";

/// ABI 函数规范
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AbiFuncSpec {
    pub name: &'static str,
    pub params: &'static [AbiValType],
    pub results: &'static [AbiValType],
}

/// ABI 值类型（简化的 ValType，用于校验）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AbiValType {
    I32,
    I64,
    F32,
    F64,
}

impl AbiValType {
    /// 从 wasmparser::ValType 转换
    pub fn from_wasmparser(vt: wasmparser::ValType) -> Option<Self> {
        use wasmparser::ValType;
        match vt {
            ValType::I32 => Some(Self::I32),
            ValType::I64 => Some(Self::I64),
            ValType::F32 => Some(Self::F32),
            ValType::F64 => Some(Self::F64),
            _ => None, // V128, Ref 等不支持
        }
    }
}

/// 获取所有允许的 Host 函数列表
pub fn allowed_functions() -> &'static [AbiFuncSpec] {
    &[
        // ========== handle_ops.rs ==========
        AbiFuncSpec {
            name: "ctx_key_handle",
            params: &[],
            results: &[AbiValType::I32],
        },
        AbiFuncSpec {
            name: "ctx_value_handle",
            params: &[],
            results: &[AbiValType::I32],
        },
        AbiFuncSpec {
            name: "ctx_value_kind",
            params: &[],
            results: &[AbiValType::I32],
        },
        AbiFuncSpec {
            name: "ctx_expires_at",
            params: &[],
            results: &[AbiValType::I64],
        },
        AbiFuncSpec {
            name: "ctx_sequence",
            params: &[],
            results: &[AbiValType::I64],
        },
        AbiFuncSpec {
            name: "bytes_len",
            params: &[AbiValType::I32],
            results: &[AbiValType::I32],
        },
        AbiFuncSpec {
            name: "bytes_read",
            params: &[
                AbiValType::I32,
                AbiValType::I32,
                AbiValType::I32,
                AbiValType::I32,
            ],
            results: &[AbiValType::I32],
        },
        AbiFuncSpec {
            name: "bytes_starts_with",
            params: &[AbiValType::I32, AbiValType::I32, AbiValType::I32],
            results: &[AbiValType::I32],
        },
        AbiFuncSpec {
            name: "bytes_equals",
            params: &[AbiValType::I32, AbiValType::I32, AbiValType::I32],
            results: &[AbiValType::I32],
        },
        AbiFuncSpec {
            name: "bytes_find",
            params: &[
                AbiValType::I32,
                AbiValType::I32,
                AbiValType::I32,
                AbiValType::I32,
            ],
            results: &[AbiValType::I32],
        },
        AbiFuncSpec {
            name: "bytes_close",
            params: &[AbiValType::I32],
            results: &[AbiValType::I32],
        },
        // ========== db_view.rs ==========
        AbiFuncSpec {
            name: "db_open_value_handle",
            params: &[AbiValType::I32, AbiValType::I32],
            results: &[AbiValType::I32],
        },
        // ========== commands.rs ==========
        AbiFuncSpec {
            name: "cmd_set_key",
            params: &[AbiValType::I32, AbiValType::I32],
            results: &[AbiValType::I32],
        },
        AbiFuncSpec {
            name: "cmd_set_value",
            params: &[AbiValType::I32, AbiValType::I32],
            results: &[AbiValType::I32],
        },
        AbiFuncSpec {
            name: "cmd_set_ttl",
            params: &[AbiValType::I64],
            results: &[AbiValType::I32],
        },
        AbiFuncSpec {
            name: "cmd_set_expires_at",
            params: &[AbiValType::I64],
            results: &[AbiValType::I32],
        },
        AbiFuncSpec {
            name: "cmd_clear_ttl",
            params: &[],
            results: &[AbiValType::I32],
        },
        AbiFuncSpec {
            name: "cmd_set_reason",
            params: &[AbiValType::I32, AbiValType::I32],
            results: &[AbiValType::I32],
        },
        // ========== read_ops.rs ==========
        AbiFuncSpec {
            name: "read_set_value",
            params: &[AbiValType::I32, AbiValType::I32],
            results: &[AbiValType::I32],
        },
        AbiFuncSpec {
            name: "read_set_reason",
            params: &[AbiValType::I32, AbiValType::I32],
            results: &[AbiValType::I32],
        },
        AbiFuncSpec {
            name: "scan_set_drop",
            params: &[],
            results: &[AbiValType::I32],
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_function_count() {
        // 确保函数列表完整
        assert_eq!(allowed_functions().len(), 21);
    }

    #[test]
    fn test_no_duplicate_names() {
        use std::collections::HashSet;
        let names: HashSet<_> = allowed_functions().iter().map(|f| f.name).collect();
        assert_eq!(
            names.len(),
            allowed_functions().len(),
            "Duplicate function names detected"
        );
    }

    #[test]
    fn test_all_functions_have_valid_signatures() {
        for func in allowed_functions() {
            // 确保名称非空
            assert!(!func.name.is_empty());
            // 确保最多1个返回值（Wasm MVP 限制）
            assert!(func.results.len() <= 1);
        }
    }
}
