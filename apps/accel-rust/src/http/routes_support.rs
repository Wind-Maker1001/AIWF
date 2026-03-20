use axum::{
    Json,
    body::{Body, Bytes, to_bytes},
    http::{Method, StatusCode},
    response::{IntoResponse, Response},
};
use serde_json::{Value, json};
use url::form_urlencoded;

pub(super) fn bad_request_response(operator: &str, error: &str) -> Response {
    (
        StatusCode::BAD_REQUEST,
        Json(json!({
            "ok": false,
            "operator": operator,
            "status": "failed",
            "error": error,
        })),
    )
        .into_response()
}

pub(super) fn forbidden_response(operator: &str, tenant_id: Option<&str>) -> Response {
    (
        StatusCode::FORBIDDEN,
        Json(json!({
            "ok": false,
            "operator": operator,
            "status": "failed",
            "error": format!(
                "operator_forbidden: tenant={} operator={}",
                tenant_id.unwrap_or("default"),
                operator
            ),
        })),
    )
        .into_response()
}

pub(super) fn tenant_from_query(query: Option<&str>) -> Option<String> {
    query.and_then(|raw| {
        form_urlencoded::parse(raw.as_bytes()).find_map(|(key, value)| {
            if key == "tenant_id" {
                let tenant = value.trim().to_string();
                if tenant.is_empty() {
                    None
                } else {
                    Some(tenant)
                }
            } else {
                None
            }
        })
    })
}

pub(super) fn operator_name_from_path(path: &str) -> Option<String> {
    let suffix = path.strip_prefix("/operators/")?.trim_matches('/');
    if suffix.is_empty() {
        return None;
    }
    if let Some(name) = suffix.strip_prefix("transform_rows_v2/") {
        if matches!(name, "submit" | "stream" | "cache_stats" | "cache_clear") {
            return Some("transform_rows_v2".to_string());
        }
    }
    if let Some(action) = suffix.strip_prefix("rules_package_v1/") {
        let action = action.trim_matches('/');
        if !action.is_empty() {
            return Some(format!("rules_package_{action}_v1"));
        }
    }
    let normalized = suffix.replace('/', "_");
    if let Some(base) = normalized.strip_suffix("_apply") {
        return Some(base.to_string());
    }
    if let Some(base) = normalized.strip_suffix("_validate") {
        return Some(base.to_string());
    }
    Some(normalized)
}

pub(super) fn tenant_from_value(value: &Value) -> Option<String> {
    value
        .as_object()
        .and_then(|obj| obj.get("tenant_id"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|tenant| !tenant.is_empty())
        .map(ToOwned::to_owned)
}

pub(super) async fn request_body_bytes(method: &Method, body: Body) -> Result<Bytes, String> {
    if *method == Method::GET || *method == Method::HEAD {
        return Ok(Bytes::new());
    }
    to_bytes(body, 64 * 1024 * 1024)
        .await
        .map_err(|err| format!("failed to inspect operator request body: {err}"))
}

pub(super) fn tenant_from_body_bytes(bytes: &Bytes) -> Option<String> {
    if bytes.is_empty() {
        return None;
    }
    serde_json::from_slice::<Value>(bytes)
        .ok()
        .and_then(|parsed| tenant_from_value(&parsed))
}
