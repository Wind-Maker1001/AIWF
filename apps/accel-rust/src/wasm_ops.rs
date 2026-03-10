use crate::{
    api_types::UdfWasmReq,
    transform_support::{value_to_f64, value_to_string},
};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STD;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use wasmtime::{
    Engine as WasmEngine, Linker as WasmLinker, Module as WasmModule, Store as WasmStore,
};

pub(crate) struct WasmUdfRuntime {
    store: WasmStore<()>,
    memory: Option<wasmtime::Memory>,
    transform_f64: Option<wasmtime::TypedFunc<f64, f64>>,
    transform_i64: Option<wasmtime::TypedFunc<i64, i64>>,
    transform_str: Option<wasmtime::TypedFunc<(i32, i32), i64>>,
    alloc: Option<wasmtime::TypedFunc<i32, i32>>,
    dealloc: Option<wasmtime::TypedFunc<(i32, i32), ()>>,
}

pub(crate) fn wasm_unpack_ptr_len(v: i64) -> (usize, usize) {
    let ptr = ((v >> 32) as u32) as usize;
    let len = (v as u32) as usize;
    (ptr, len)
}

pub(crate) fn init_wasm_udf_runtime(b64: &str) -> Result<WasmUdfRuntime, String> {
    let bytes = BASE64_STD
        .decode(b64.as_bytes())
        .map_err(|e| format!("decode wasm_base64: {e}"))?;
    let engine = WasmEngine::default();
    let module =
        WasmModule::new(&engine, bytes).map_err(|e| format!("compile wasm module: {e}"))?;
    let linker = WasmLinker::new(&engine);
    let mut store = WasmStore::new(&engine, ());
    let instance = linker
        .instantiate(&mut store, &module)
        .map_err(|e| format!("instantiate wasm: {e}"))?;
    let memory = instance.get_memory(&mut store, "memory");
    let transform_f64 = instance
        .get_typed_func::<f64, f64>(&mut store, "transform_f64")
        .ok()
        .or_else(|| {
            instance
                .get_typed_func::<f64, f64>(&mut store, "transform")
                .ok()
        });
    let transform_i64 = instance
        .get_typed_func::<i64, i64>(&mut store, "transform_i64")
        .ok();
    let transform_str = instance
        .get_typed_func::<(i32, i32), i64>(&mut store, "transform_str")
        .ok();
    let alloc = instance
        .get_typed_func::<i32, i32>(&mut store, "alloc")
        .ok();
    let dealloc = instance
        .get_typed_func::<(i32, i32), ()>(&mut store, "dealloc")
        .ok();
    if transform_f64.is_none() && transform_i64.is_none() && transform_str.is_none() {
        return Err(
            "wasm exports missing: need one of transform_f64/transform/transform_i64/transform_str"
                .to_string(),
        );
    }
    Ok(WasmUdfRuntime {
        store,
        memory,
        transform_f64,
        transform_i64,
        transform_str,
        alloc,
        dealloc,
    })
}

pub(crate) fn wasm_call_string(
    runtime: &mut WasmUdfRuntime,
    input: &str,
) -> Result<String, String> {
    let Some(transform) = runtime.transform_str.as_ref() else {
        return Err("transform_str export missing".to_string());
    };
    let Some(alloc) = runtime.alloc.as_ref() else {
        return Err("alloc export missing".to_string());
    };
    let Some(memory) = runtime.memory.as_ref() else {
        return Err("memory export missing".to_string());
    };
    let in_bytes = input.as_bytes();
    let in_len = i32::try_from(in_bytes.len()).map_err(|_| "input string too large".to_string())?;
    let in_ptr = alloc
        .call(&mut runtime.store, in_len)
        .map_err(|e| format!("wasm alloc failed: {e}"))?;
    {
        let data = memory.data_mut(&mut runtime.store);
        let start = in_ptr as usize;
        let end = start.saturating_add(in_bytes.len());
        if end > data.len() {
            return Err("wasm memory overflow while writing input".to_string());
        }
        data[start..end].copy_from_slice(in_bytes);
    }
    let packed = transform
        .call(&mut runtime.store, (in_ptr, in_len))
        .map_err(|e| format!("wasm transform_str call failed: {e}"))?;
    let (out_ptr, out_len) = wasm_unpack_ptr_len(packed);
    let out_bytes = {
        let data = memory.data(&runtime.store);
        let end = out_ptr.saturating_add(out_len);
        if end > data.len() {
            return Err("wasm memory overflow while reading output".to_string());
        }
        data[out_ptr..end].to_vec()
    };
    if let Some(dealloc) = runtime.dealloc.as_ref() {
        let _ = dealloc.call(&mut runtime.store, (in_ptr, in_len));
        if out_len <= i32::MAX as usize && out_ptr <= i32::MAX as usize {
            let _ = dealloc.call(&mut runtime.store, (out_ptr as i32, out_len as i32));
        }
    }
    String::from_utf8(out_bytes).map_err(|e| format!("wasm output is not utf8: {e}"))
}

pub(crate) fn run_udf_wasm_v1(req: UdfWasmReq) -> Result<Value, String> {
    let op = req
        .op
        .unwrap_or_else(|| "identity".to_string())
        .to_lowercase();
    let mut wasm_error: Option<String> = None;
    let mut wasm_mode = "sandboxed_builtin".to_string();
    let mut runtime = req
        .wasm_base64
        .as_ref()
        .map(|b64| match init_wasm_udf_runtime(b64) {
            Ok(rt) => {
                wasm_mode = "wasm_abi".to_string();
                Some(rt)
            }
            Err(e) => {
                wasm_error = Some(e);
                None
            }
        })
        .unwrap_or(None);
    let mut used_abi: HashMap<String, usize> = HashMap::new();

    let mut out = Vec::new();
    for r in req.rows {
        let Some(mut obj) = r.as_object().cloned() else {
            continue;
        };
        let src = obj.get(&req.field).cloned().unwrap_or(Value::Null);
        let nv = if let Some(rt) = runtime.as_mut() {
            if let Some(v) = src.as_i64()
                && let Some(f) = rt.transform_i64.as_ref()
            {
                match f.call(&mut rt.store, v) {
                    Ok(x) => {
                        *used_abi.entry("i64".to_string()).or_insert(0) += 1;
                        Value::Number(x.into())
                    }
                    Err(e) => {
                        wasm_error = Some(format!("wasm i64 call failed: {e}"));
                        Value::Null
                    }
                }
            } else if let Some(v) = src.as_u64()
                && let Some(f) = rt.transform_i64.as_ref()
            {
                match i64::try_from(v) {
                    Ok(vv) => match f.call(&mut rt.store, vv) {
                        Ok(x) => {
                            *used_abi.entry("i64".to_string()).or_insert(0) += 1;
                            Value::Number(x.into())
                        }
                        Err(e) => {
                            wasm_error = Some(format!("wasm i64 call failed: {e}"));
                            Value::Null
                        }
                    },
                    Err(_) => Value::Null,
                }
            } else if let Some(v) = value_to_f64(&src)
                && let Some(f) = rt.transform_f64.as_ref()
            {
                match f.call(&mut rt.store, v) {
                    Ok(x) => {
                        *used_abi.entry("f64".to_string()).or_insert(0) += 1;
                        json!(x)
                    }
                    Err(e) => {
                        wasm_error = Some(format!("wasm f64 call failed: {e}"));
                        Value::Null
                    }
                }
            } else if rt.transform_str.is_some() {
                let s = value_to_string(&src);
                match wasm_call_string(rt, &s) {
                    Ok(s2) => {
                        *used_abi.entry("string".to_string()).or_insert(0) += 1;
                        Value::String(s2)
                    }
                    Err(e) => {
                        wasm_error = Some(e);
                        Value::Null
                    }
                }
            } else {
                wasm_error = Some("wasm runtime has no ABI matching input value type".to_string());
                Value::Null
            }
        } else {
            match op.as_str() {
                "identity" => src,
                "double" => value_to_f64(&src)
                    .map(|x| json!(x * 2.0))
                    .unwrap_or(Value::Null),
                "negate" => value_to_f64(&src).map(|x| json!(-x)).unwrap_or(Value::Null),
                "trim" => Value::String(value_to_string(&src).trim().to_string()),
                "upper" => Value::String(value_to_string(&src).to_uppercase()),
                _ => return Err(format!("unsupported udf op: {op}")),
            }
        };
        obj.insert(req.output_field.clone(), nv);
        out.push(Value::Object(obj));
    }
    let wasm_hash = req.wasm_base64.as_ref().map(|s| {
        let mut h = Sha256::new();
        h.update(s.as_bytes());
        format!("{:x}", h.finalize())
    });
    let out_len = out.len();
    Ok(json!({
        "ok": true,
        "operator": "udf_wasm_v1",
        "status": "done",
        "run_id": req.run_id,
        "rows": out,
        "stats": {
            "input_rows": out_len,
            "mode": wasm_mode,
            "op": op,
            "wasm_hash": wasm_hash,
            "used_abi": used_abi,
            "note": "supported ABI: transform_f64(f64)->f64 or transform_i64(i64)->i64 or transform_str(ptr,len)->i64(high32=ptr,low32=len)+alloc/dealloc/memory",
            "wasm_error": wasm_error
        }
    }))
}
