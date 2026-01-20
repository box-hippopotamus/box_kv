//! Wasm 模块 ABI 校验器

use crate::abi::catalog::{AbiValType, HOST_NAMESPACE, allowed_functions};
use crate::error::{Result, WasmError};
use std::collections::HashMap;
use wasmparser::{FuncType, Parser, Payload, TypeRef, ValType};

/// ABI 校验策略
#[derive(Debug, Clone)]
pub struct AbiPolicy {
    /// 主机命名空间
    pub host_namespace: String,
    /// 允许的额外模块
    pub allowed_extra_modules: Vec<String>,
    /// 是否强制签名校验
    pub enforce_signature_check: bool,
}

impl Default for AbiPolicy {
    fn default() -> Self {
        Self {
            host_namespace: HOST_NAMESPACE.to_string(),
            allowed_extra_modules: vec![],
            enforce_signature_check: true,
        }
    }
}

/// 校验 Wasm 模块的导入是否符合 ABI 策略
pub fn validate_abi(wasm_bytes: &[u8], policy: &AbiPolicy) -> Result<()> {
    // 构建 ABI 函数目录（name -> spec）
    let allowed_funcs: HashMap<_, _> = allowed_functions()
        .iter()
        .map(|spec| (spec.name, spec))
        .collect();

    // 解析 TypeSection 和 ImportSection
    let mut type_signatures: Vec<FuncSignature> = vec![];
    let mut violations = vec![];

    for payload in Parser::new(0).parse_all(wasm_bytes) {
        let payload = payload
            .map_err(|e| WasmError::CompilationFailed(format!("Failed to parse wasm: {}", e)))?;

        match payload {
            Payload::TypeSection(reader) => {
                for rec_group in reader {
                    let rec_group = rec_group.map_err(|e| {
                        WasmError::CompilationFailed(format!("Failed to read type: {}", e))
                    })?;

                    for subty in rec_group.types() {
                        match &subty.composite_type {
                            wasmparser::CompositeType {
                                inner: wasmparser::CompositeInnerType::Func(ft),
                                ..
                            } => {
                                type_signatures.push(FuncSignature {
                                    params: ft.params().iter().copied().collect(),
                                    results: ft.results().iter().copied().collect(),
                                });
                            }
                            _ => {}
                        }
                    }
                }
            }
            Payload::ImportSection(reader) => {
                for import in reader {
                    let import = import.map_err(|e| {
                        WasmError::CompilationFailed(format!("Failed to read import: {}", e))
                    })?;

                    // 检查1：只允许函数导入
                    let func_type_idx = match import.ty {
                        TypeRef::Func(idx) => idx,
                        _ => {
                            violations.push(format!(
                                "Non-function import: {}.{} (type: {:?})",
                                import.module, import.name, import.ty
                            ));
                            continue;
                        }
                    };

                    // 检查2：命名空间校验
                    let is_allowed_module = import.module == policy.host_namespace
                        || policy
                            .allowed_extra_modules
                            .contains(&import.module.to_string());

                    if !is_allowed_module {
                        violations.push(format!(
                            "Disallowed module: {}.{} (only '{}' is allowed)",
                            import.module, import.name, policy.host_namespace
                        ));
                        continue;
                    }

                    // 只对 host_namespace 做函数名和签名校验
                    if import.module == policy.host_namespace {
                        // 检查3：函数名是否在白名单中
                        let spec = match allowed_funcs.get(import.name) {
                            Some(s) => s,
                            None => {
                                violations.push(format!(
                                    "Disallowed function: {}.{} (not in ABI catalog)",
                                    import.module, import.name
                                ));
                                continue;
                            }
                        };

                        // 检查4：签名匹配（可选）
                        if policy.enforce_signature_check {
                            let actual_sig = &type_signatures[func_type_idx as usize];
                            if !signature_matches(actual_sig, spec.params, spec.results) {
                                violations.push(format!(
                                    "Signature mismatch: {}.{} - expected ({:?}) -> ({:?}), got ({:?}) -> ({:?})",
                                    import.module,
                                    import.name,
                                    spec.params,
                                    spec.results,
                                    actual_sig.params,
                                    actual_sig.results
                                ));
                            }
                        }
                    }
                }
            }
            _ => {}
        }
    }

    if violations.is_empty() {
        Ok(())
    } else {
        Err(WasmError::AbiViolation(format!(
            "Plugin ABI validation failed ({} violations):\n{}",
            violations.len(),
            violations.join("\n")
        )))
    }
}

#[derive(Debug, Clone)]
struct FuncSignature {
    params: Vec<ValType>,
    results: Vec<ValType>,
}

/// 比较实际签名与预期签名
fn signature_matches(
    actual: &FuncSignature,
    expected_params: &[AbiValType],
    expected_results: &[AbiValType],
) -> bool {
    if actual.params.len() != expected_params.len()
        || actual.results.len() != expected_results.len()
    {
        return false;
    }

    for (actual_p, expected_p) in actual.params.iter().zip(expected_params.iter()) {
        if !valtype_matches(*actual_p, *expected_p) {
            return false;
        }
    }

    for (actual_r, expected_r) in actual.results.iter().zip(expected_results.iter()) {
        if !valtype_matches(*actual_r, *expected_r) {
            return false;
        }
    }

    true
}

fn valtype_matches(actual: ValType, expected: AbiValType) -> bool {
    match AbiValType::from_wasmparser(actual) {
        Some(abi_val) => abi_val == expected,
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 生成一个只导入 boxkv_host::ctx_key_handle 的最小模块
    fn minimal_valid_wasm() -> Vec<u8> {
        let wat = r#"
            (module
                (import "boxkv_host" "ctx_key_handle" (func (result i32)))
            )
        "#;
        wat::parse_str(wat).unwrap()
    }

    /// 生成一个导入非法模块的 wasm
    fn wasm_with_invalid_module() -> Vec<u8> {
        let wat = r#"
            (module
                (import "evil_module" "hack" (func))
            )
        "#;
        wat::parse_str(wat).unwrap()
    }

    /// 生成一个导入非法函数名的 wasm
    fn wasm_with_invalid_function() -> Vec<u8> {
        let wat = r#"
            (module
                (import "boxkv_host" "non_existent_func" (func))
            )
        "#;
        wat::parse_str(wat).unwrap()
    }

    #[test]
    fn test_valid_module_passes() {
        let wasm = minimal_valid_wasm();
        let policy = AbiPolicy::default();
        let result = validate_abi(&wasm, &policy);
        assert!(result.is_ok(), "Valid module should pass: {:?}", result);
    }

    #[test]
    fn test_invalid_module_rejected() {
        let wasm = wasm_with_invalid_module();
        let policy = AbiPolicy::default();
        let result = validate_abi(&wasm, &policy);
        assert!(result.is_err());
        if let Err(WasmError::AbiViolation(msg)) = result {
            assert!(msg.contains("evil_module"));
        } else {
            panic!("Expected AbiViolation error");
        }
    }

    #[test]
    fn test_invalid_function_rejected() {
        let wasm = wasm_with_invalid_function();
        let policy = AbiPolicy::default();
        let result = validate_abi(&wasm, &policy);
        assert!(result.is_err());
        if let Err(WasmError::AbiViolation(msg)) = result {
            assert!(msg.contains("non_existent_func"));
        } else {
            panic!("Expected AbiViolation error");
        }
    }

    #[test]
    fn test_extra_modules_allowed() {
        let wasm = wasm_with_invalid_module(); // 导入 evil_module
        let mut policy = AbiPolicy::default();
        policy.allowed_extra_modules.push("evil_module".to_string());
        let result = validate_abi(&wasm, &policy);
        // 因为允许了 evil_module，应该通过（不校验该模块的函数名）
        assert!(
            result.is_ok(),
            "Extra module should be allowed: {:?}",
            result
        );
    }
}
