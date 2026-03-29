use crate::api_types::{WorkflowContractV1Req, WorkflowContractV1Resp};
use crate::operator_catalog::resolve_operator_metadata;
use serde_json::{Value, json};
use std::{
    collections::{BTreeSet, HashMap},
    fs,
    path::{Path, PathBuf},
    sync::OnceLock,
};

const WORKFLOW_CONTRACT_SCHEMA_VERSION: &str = "workflow_contract_validation.v1";
const WORKFLOW_GRAPH_CONTRACT_AUTHORITY: &str = "contracts/workflow/workflow.schema.json";
const NODE_CONFIG_VALIDATION_ERROR_CONTRACT_AUTHORITY: &str =
    "contracts/desktop/node_config_validation_errors.v1.json";
const NODE_CONFIG_CONTRACT_AUTHORITY: &str = "contracts/desktop/node_config_contracts.v1.json";
const RUST_OPERATOR_MANIFEST_AUTHORITY: &str = "contracts/rust/operators_manifest.v1.json";
const DEFAULT_WORKFLOW_VERSION: &str = "1.0.0";
const DEFAULT_VALIDATION_SCOPE: &str = "authoring";
const WORKFLOW_RUNTIME_ONLY_NODE_TYPES: &[&str] = &["compute_rust", "md_output"];

#[derive(Clone, Default)]
struct NodeConfigContractData {
    contract_types: Vec<String>,
    validators_by_type: HashMap<String, Vec<Value>>,
}

#[derive(Clone, Default)]
struct WorkflowValidationAssets {
    node_contracts: NodeConfigContractData,
    rust_manifest_types: Vec<String>,
    known_node_types: Vec<String>,
}

fn repo_root() -> PathBuf {
    if let Ok(configured) = std::env::var("AIWF_REPO_ROOT") {
        let trimmed = configured.trim();
        if !trimmed.is_empty() {
            return PathBuf::from(trimmed);
        }
    }
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    manifest_dir
        .parent()
        .and_then(|item| item.parent())
        .map(Path::to_path_buf)
        .unwrap_or_else(|| manifest_dir.to_path_buf())
}

fn node_config_contract_path() -> PathBuf {
    repo_root()
        .join("contracts")
        .join("desktop")
        .join("node_config_contracts.v1.json")
}

fn rust_operator_manifest_path() -> PathBuf {
    repo_root()
        .join("contracts")
        .join("rust")
        .join("operators_manifest.v1.json")
}

fn load_node_config_contracts() -> Result<NodeConfigContractData, String> {
    let path = node_config_contract_path();
    let payload: Value = serde_json::from_str(
        &fs::read_to_string(&path)
            .map_err(|err| format!("failed to read node config contract: {err}"))?,
    )
    .map_err(|err| format!("failed to parse node config contract: {err}"))?;
    let nodes = payload
        .get("nodes")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut contract_types = Vec::new();
    let mut validators_by_type = HashMap::new();
    for item in nodes {
        let Some(obj) = item.as_object() else {
            continue;
        };
        let node_type = obj
            .get("type")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or("");
        if node_type.is_empty() {
            continue;
        }
        let validators = obj
            .get("validators")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        contract_types.push(node_type.to_string());
        validators_by_type.insert(node_type.to_string(), validators);
    }
    contract_types.sort();
    Ok(NodeConfigContractData {
        contract_types,
        validators_by_type,
    })
}

fn load_rust_manifest_types() -> Result<Vec<String>, String> {
    let path = rust_operator_manifest_path();
    let payload: Value = serde_json::from_str(
        &fs::read_to_string(&path)
            .map_err(|err| format!("failed to read rust operator manifest: {err}"))?,
    )
    .map_err(|err| format!("failed to parse rust operator manifest: {err}"))?;
    let operators = payload
        .get("operators")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut types = BTreeSet::new();
    for item in operators {
        let Some(obj) = item.as_object() else {
            continue;
        };
        let workflow_exposable = obj
            .get("workflow_exposable")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        if !workflow_exposable {
            continue;
        }
        if let Some(operator) = obj.get("operator").and_then(Value::as_str) {
            let normalized = operator.trim();
            if !normalized.is_empty() {
                types.insert(normalized.to_string());
            }
        }
    }
    Ok(types.into_iter().collect())
}

fn workflow_validation_assets() -> Result<&'static WorkflowValidationAssets, String> {
    static ASSETS: OnceLock<Result<WorkflowValidationAssets, String>> = OnceLock::new();
    let assets = ASSETS.get_or_init(|| {
        let node_contracts = load_node_config_contracts()?;
        let rust_manifest_types = load_rust_manifest_types()?;
        let mut known = BTreeSet::new();
        for item in &node_contracts.contract_types {
            known.insert(item.clone());
        }
        for item in &rust_manifest_types {
            known.insert(item.clone());
        }
        for item in WORKFLOW_RUNTIME_ONLY_NODE_TYPES {
            known.insert((*item).to_string());
        }
        Ok(WorkflowValidationAssets {
            node_contracts,
            rust_manifest_types,
            known_node_types: known.into_iter().collect(),
        })
    });
    match assets {
        Ok(value) => Ok(value),
        Err(err) => Err(err.clone()),
    }
}

fn make_error_item(path: impl Into<String>, code: &str, message: impl Into<String>) -> Value {
    json!({
        "path": path.into(),
        "code": code,
        "message": message.into(),
    })
}

fn is_plain_object(value: &Value) -> bool {
    matches!(value, Value::Object(_))
}

fn normalized_scope(value: Option<&str>) -> String {
    let raw = value.unwrap_or(DEFAULT_VALIDATION_SCOPE).trim().to_lowercase();
    match raw.as_str() {
        "authoring" | "publish" | "governance_write" | "run" => raw,
        _ => DEFAULT_VALIDATION_SCOPE.to_string(),
    }
}

fn get_value_at_path<'a>(root: &'a Value, raw_path: &str) -> Option<&'a Value> {
    let path = raw_path.trim();
    if path.is_empty() {
        return Some(root);
    }
    let mut current = root;
    for segment in path.split('.') {
        if segment.trim().is_empty() {
            continue;
        }
        let next = current.as_object()?.get(segment.trim())?;
        current = next;
    }
    Some(current)
}

fn has_meaningful_value(value: Option<&Value>) -> bool {
    match value {
        None => false,
        Some(Value::Null) => false,
        Some(Value::String(text)) => !text.trim().is_empty(),
        Some(_) => true,
    }
}

fn validate_json_compatible_value(value: Option<&Value>, label: &str, errors: &mut Vec<Value>) {
    let Some(value) = value else {
        return;
    };
    match value {
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
        Value::Array(items) => {
            for (index, item) in items.iter().enumerate() {
                validate_json_compatible_value(Some(item), &format!("{label}[{index}]"), errors);
            }
        }
        Value::Object(map) => {
            for (key, item) in map {
                if key.trim().is_empty() {
                    errors.push(make_error_item(label, "empty_key", format!("{label} keys must not be empty")));
                    continue;
                }
                validate_json_compatible_value(Some(item), &format!("{label}.{key}"), errors);
            }
        }
    }
}

fn validate_optional_string_array_non_empty(
    value: Option<&Value>,
    label: &str,
    errors: &mut Vec<Value>,
) {
    let Some(value) = value else {
        return;
    };
    let Some(items) = value.as_array() else {
        errors.push(make_error_item(label, "type_array", format!("{label} must be an array")));
        return;
    };
    for (index, item) in items.iter().enumerate() {
        let Some(text) = item.as_str() else {
            errors.push(make_error_item(
                format!("{label}[{index}]"),
                "type_string",
                format!("{label}[{index}] must be a string"),
            ));
            continue;
        };
        if text.trim().is_empty() {
            errors.push(make_error_item(
                format!("{label}[{index}]"),
                "string_empty",
                format!("{label}[{index}] must not be empty"),
            ));
        }
    }
}

fn validate_rules_object(value: Option<&Value>, label: &str, errors: &mut Vec<Value>) {
    let Some(value) = value else {
        return;
    };
    let Some(obj) = value.as_object() else {
        errors.push(make_error_item(label, "type_object", format!("{label} must be an object")));
        return;
    };
    for key in ["required_columns", "forbidden_columns", "unique_columns"] {
        validate_optional_string_array_non_empty(obj.get(key), &format!("{label}.{key}"), errors);
    }
}

fn validate_manifest_object(value: Option<&Value>, label: &str, errors: &mut Vec<Value>) {
    let Some(value) = value else {
        return;
    };
    let Some(obj) = value.as_object() else {
        errors.push(make_error_item(label, "type_object", format!("{label} must be an object")));
        return;
    };
    for key in ["name", "version", "api_version", "entry", "command"] {
        if let Some(value) = obj.get(key) {
            let Some(text) = value.as_str() else {
                errors.push(make_error_item(
                    format!("{label}.{key}"),
                    "type_string",
                    format!("{label}.{key} must be a string"),
                ));
                continue;
            };
            if text.trim().is_empty() {
                errors.push(make_error_item(
                    format!("{label}.{key}"),
                    "string_empty",
                    format!("{label}.{key} must not be empty"),
                ));
            }
        }
    }
    if let Some(value) = obj.get("enabled")
        && !value.is_boolean()
    {
        errors.push(make_error_item(
            format!("{label}.enabled"),
            "type_boolean",
            format!("{label}.enabled must be a boolean"),
        ));
    }
    validate_optional_string_array_non_empty(obj.get("capabilities"), &format!("{label}.capabilities"), errors);
    if let Some(value) = obj.get("args") {
        let Some(items) = value.as_array() else {
            errors.push(make_error_item(
                format!("{label}.args"),
                "type_array",
                format!("{label}.args must be an array"),
            ));
            return;
        };
        for (index, item) in items.iter().enumerate() {
            if !item.is_string() {
                errors.push(make_error_item(
                    format!("{label}.args[{index}]"),
                    "type_string",
                    format!("{label}.args[{index}] must be a string"),
                ));
            }
        }
    }
}

fn validate_computed_fields(value: Option<&Value>, label: &str, errors: &mut Vec<Value>) {
    let Some(value) = value else {
        return;
    };
    let Some(items) = value.as_array() else {
        errors.push(make_error_item(label, "type_array", format!("{label} must be an array")));
        return;
    };
    for (index, item) in items.iter().enumerate() {
        let Some(obj) = item.as_object() else {
            errors.push(make_error_item(
                format!("{label}[{index}]"),
                "type_object",
                format!("{label}[{index}] must be an object"),
            ));
            continue;
        };
        let target = obj
            .get("as")
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .or_else(|| obj.get("name").and_then(Value::as_str).filter(|value| !value.trim().is_empty()))
            .or_else(|| obj.get("field").and_then(Value::as_str).filter(|value| !value.trim().is_empty()));
        if target.is_none() {
            errors.push(make_error_item(
                format!("{label}[{index}]"),
                "missing_one_of",
                format!("{label}[{index}] requires one of as/name/field"),
            ));
        }
        if let Some(expr) = obj.get("expr")
            && !expr.is_string()
        {
            errors.push(make_error_item(
                format!("{label}[{index}].expr"),
                "type_string",
                format!("{label}[{index}].expr must be a string"),
            ));
        }
    }
}

fn validate_workflow_steps(value: Option<&Value>, label: &str, errors: &mut Vec<Value>) {
    let Some(value) = value else {
        return;
    };
    let Some(items) = value.as_array() else {
        errors.push(make_error_item(label, "type_array", format!("{label} must be an array")));
        return;
    };
    for (index, item) in items.iter().enumerate() {
        let Some(obj) = item.as_object() else {
            errors.push(make_error_item(
                format!("{label}[{index}]"),
                "type_object",
                format!("{label}[{index}] must be an object"),
            ));
            continue;
        };
        let id = obj
            .get("id")
            .and_then(Value::as_str)
            .map(str::trim)
            .unwrap_or("");
        if id.is_empty() {
            errors.push(make_error_item(
                format!("{label}[{index}].id"),
                "required",
                format!("{label}[{index}].id is required"),
            ));
        }
        validate_optional_string_array_non_empty(obj.get("depends_on"), &format!("{label}[{index}].depends_on"), errors);
        if let Some(value) = obj.get("operator")
            && !value.is_string()
        {
            errors.push(make_error_item(
                format!("{label}[{index}].operator"),
                "type_string",
                format!("{label}[{index}].operator must be a string"),
            ));
        }
    }
}

fn validate_constraint_defs(value: Option<&Value>, label: &str, errors: &mut Vec<Value>) {
    let Some(value) = value else {
        return;
    };
    let Some(items) = value.as_array() else {
        errors.push(make_error_item(label, "type_array", format!("{label} must be an array")));
        return;
    };
    for (index, item) in items.iter().enumerate() {
        let Some(obj) = item.as_object() else {
            errors.push(make_error_item(
                format!("{label}[{index}]"),
                "type_object",
                format!("{label}[{index}] must be an object"),
            ));
            continue;
        };
        let kind = obj
            .get("kind")
            .and_then(Value::as_str)
            .map(str::trim)
            .unwrap_or("");
        if kind.is_empty() {
            errors.push(make_error_item(
                format!("{label}[{index}].kind"),
                "required",
                format!("{label}[{index}].kind is required"),
            ));
            continue;
        }
        if !["sum_equals", "non_negative"].contains(&kind) {
            errors.push(make_error_item(
                format!("{label}[{index}].kind"),
                "enum_not_allowed",
                format!("{label}[{index}].kind must be one of: sum_equals, non_negative"),
            ));
            continue;
        }
        if kind == "sum_equals" {
            validate_optional_string_array_non_empty(obj.get("left"), &format!("{label}[{index}].left"), errors);
            if let Some(value) = obj.get("right") {
                let Some(text) = value.as_str() else {
                    errors.push(make_error_item(
                        format!("{label}[{index}].right"),
                        "type_string",
                        format!("{label}[{index}].right must be a string"),
                    ));
                    continue;
                };
                if text.trim().is_empty() {
                    errors.push(make_error_item(
                        format!("{label}[{index}].right"),
                        "string_empty",
                        format!("{label}[{index}].right must not be empty"),
                    ));
                }
            }
            if let Some(value) = obj.get("tolerance") {
                let Some(number) = value.as_f64() else {
                    errors.push(make_error_item(
                        format!("{label}[{index}].tolerance"),
                        "type_number",
                        format!("{label}[{index}].tolerance must be a number"),
                    ));
                    continue;
                };
                if number < 0.0 {
                    errors.push(make_error_item(
                        format!("{label}[{index}].tolerance"),
                        "min_value",
                        format!("{label}[{index}].tolerance must be >= 0"),
                    ));
                }
            }
        }
        if kind == "non_negative"
            && let Some(value) = obj.get("field")
        {
            let Some(text) = value.as_str() else {
                errors.push(make_error_item(
                    format!("{label}[{index}].field"),
                    "type_string",
                    format!("{label}[{index}].field must be a string"),
                ));
                continue;
            };
            if text.trim().is_empty() {
                errors.push(make_error_item(
                    format!("{label}[{index}].field"),
                    "string_empty",
                    format!("{label}[{index}].field must not be empty"),
                ));
            }
        }
    }
}

fn validate_aggregate_defs(value: Option<&Value>, label: &str, errors: &mut Vec<Value>) {
    let Some(value) = value else {
        return;
    };
    let Some(items) = value.as_array() else {
        errors.push(make_error_item(label, "type_array", format!("{label} must be an array")));
        return;
    };
    for (index, item) in items.iter().enumerate() {
        let Some(obj) = item.as_object() else {
            errors.push(make_error_item(
                format!("{label}[{index}]"),
                "type_object",
                format!("{label}[{index}] must be an object"),
            ));
            continue;
        };
        for field in ["op", "as"] {
            let text = obj
                .get(field)
                .and_then(Value::as_str)
                .map(str::trim)
                .unwrap_or("");
            if text.is_empty() {
                errors.push(make_error_item(
                    format!("{label}[{index}].{field}"),
                    "required",
                    format!("{label}[{index}].{field} is required"),
                ));
            }
        }
    }
}

fn validate_ai_providers(value: Option<&Value>, label: &str, errors: &mut Vec<Value>) {
    let Some(value) = value else {
        return;
    };
    let Some(items) = value.as_array() else {
        errors.push(make_error_item(label, "type_array", format!("{label} must be an array")));
        return;
    };
    for (index, item) in items.iter().enumerate() {
        let Some(obj) = item.as_object() else {
            errors.push(make_error_item(
                format!("{label}[{index}]"),
                "type_object",
                format!("{label}[{index}] must be an object"),
            ));
            continue;
        };
        let has_identity = ["name", "model", "endpoint"]
            .iter()
            .any(|key| obj.get(*key).and_then(Value::as_str).map(|value| !value.trim().is_empty()).unwrap_or(false));
        if !has_identity {
            errors.push(make_error_item(
                format!("{label}[{index}]"),
                "missing_one_of",
                format!("{label}[{index}] requires one of name/model/endpoint"),
            ));
        }
        for key in ["name", "model", "endpoint"] {
            if let Some(value) = obj.get(key)
                && !value.is_string()
            {
                errors.push(make_error_item(
                    format!("{label}[{index}].{key}"),
                    "type_string",
                    format!("{label}[{index}].{key} must be a string"),
                ));
            }
        }
    }
}

fn validate_window_functions(value: Option<&Value>, label: &str, errors: &mut Vec<Value>) {
    let Some(value) = value else {
        return;
    };
    let Some(items) = value.as_array() else {
        errors.push(make_error_item(label, "type_array", format!("{label} must be an array")));
        return;
    };
    for (index, item) in items.iter().enumerate() {
        let Some(obj) = item.as_object() else {
            errors.push(make_error_item(
                format!("{label}[{index}]"),
                "type_object",
                format!("{label}[{index}] must be an object"),
            ));
            continue;
        };
        for field in ["op", "as"] {
            let text = obj
                .get(field)
                .and_then(Value::as_str)
                .map(str::trim)
                .unwrap_or("");
            if text.is_empty() {
                errors.push(make_error_item(
                    format!("{label}[{index}].{field}"),
                    "required",
                    format!("{label}[{index}].{field} is required"),
                ));
            }
        }
    }
}

fn validate_slot_bindings(value: Option<&Value>, label: &str, errors: &mut Vec<Value>) {
    let Some(value) = value else {
        return;
    };
    let Some(obj) = value.as_object() else {
        errors.push(make_error_item(label, "type_object", format!("{label} must be an object")));
        return;
    };
    for (key, item) in obj {
        if key.trim().is_empty() {
            errors.push(make_error_item(label, "empty_key", format!("{label} keys must not be empty")));
            continue;
        }
        if key == "chart_main" && is_plain_object(item) {
            let chart_label = format!("{label}.{key}");
            if let Some(categories) = item.get("categories")
                && !categories.is_array()
            {
                errors.push(make_error_item(
                    format!("{chart_label}.categories"),
                    "type_array",
                    format!("{chart_label}.categories must be an array"),
                ));
            }
            if let Some(series) = item.get("series")
                && !series.is_array()
            {
                errors.push(make_error_item(
                    format!("{chart_label}.series"),
                    "type_array",
                    format!("{chart_label}.series must be an array"),
                ));
            }
        }
    }
}

fn validate_optional_integer_min(
    value: Option<&Value>,
    label: &str,
    min: i64,
    errors: &mut Vec<Value>,
) {
    let Some(value) = value else {
        return;
    };
    let Some(number) = value.as_i64() else {
        errors.push(make_error_item(label, "type_integer", format!("{label} must be an integer")));
        return;
    };
    if number < min {
        errors.push(make_error_item(
            label,
            "min_value",
            format!("{label} must be >= {min}"),
        ));
    }
}

fn validate_contract_backed_node_config(
    node_type: &str,
    config: &Value,
    prefix: &str,
    errors: &mut Vec<Value>,
    assets: &WorkflowValidationAssets,
) {
    let Some(validators) = assets.node_contracts.validators_by_type.get(node_type) else {
        return;
    };
    for (index, rule) in validators.iter().enumerate() {
        let Some(rule_obj) = rule.as_object() else {
            errors.push(make_error_item(
                format!("{prefix}.contract[{index}]"),
                "unsupported_validator_kind",
                format!("{prefix}.contract[{index}] validator kind unsupported"),
            ));
            continue;
        };
        let kind = rule_obj
            .get("kind")
            .and_then(Value::as_str)
            .map(str::trim)
            .unwrap_or("");
        let path = rule_obj
            .get("path")
            .and_then(Value::as_str)
            .map(str::trim)
            .unwrap_or("");
        let label = if path.is_empty() {
            format!("{prefix}.contract[{index}]")
        } else {
            format!("{prefix}.{path}")
        };
        let value = if path.is_empty() {
            Some(config)
        } else {
            get_value_at_path(config, path)
        };
        match kind {
            "boolean" => {
                if let Some(value) = value
                    && !value.is_boolean()
                {
                    errors.push(make_error_item(&label, "type_boolean", format!("{label} must be a boolean")));
                }
            }
            "string" => {
                if let Some(value) = value
                    && !value.is_string()
                {
                    errors.push(make_error_item(&label, "type_string", format!("{label} must be a string")));
                }
            }
            "string_non_empty" => {
                if let Some(value) = value {
                    let Some(text) = value.as_str() else {
                        errors.push(make_error_item(&label, "type_string", format!("{label} must be a string")));
                        continue;
                    };
                    if text.trim().is_empty() {
                        errors.push(make_error_item(&label, "string_empty", format!("{label} must not be empty")));
                    }
                }
            }
            "enum" => {
                if let Some(value) = value {
                    let Some(text) = value.as_str() else {
                        errors.push(make_error_item(&label, "type_string", format!("{label} must be a string")));
                        continue;
                    };
                    let normalized = text.trim().to_lowercase();
                    if normalized.is_empty() {
                        errors.push(make_error_item(&label, "string_empty", format!("{label} must not be empty")));
                        continue;
                    }
                    let allowed = rule_obj
                        .get("allowed")
                        .and_then(Value::as_array)
                        .cloned()
                        .unwrap_or_default()
                        .into_iter()
                        .filter_map(|item| item.as_str().map(|value| value.trim().to_lowercase()))
                        .filter(|value| !value.is_empty())
                        .collect::<Vec<_>>();
                    if !allowed.contains(&normalized) {
                        errors.push(make_error_item(
                            &label,
                            "enum_not_allowed",
                            format!("{label} must be one of: {}", allowed.join(", ")),
                        ));
                    }
                }
            }
            "array" => {
                if let Some(value) = value
                    && !value.is_array()
                {
                    errors.push(make_error_item(&label, "type_array", format!("{label} must be an array")));
                }
            }
            "object" => {
                if let Some(value) = value
                    && !is_plain_object(value)
                {
                    errors.push(make_error_item(&label, "type_object", format!("{label} must be an object")));
                }
            }
            "row_objects" => {
                let Some(value) = value else {
                    continue;
                };
                let Some(items) = value.as_array() else {
                    errors.push(make_error_item(&label, "type_array", format!("{label} must be an array")));
                    continue;
                };
                for (item_index, item) in items.iter().enumerate() {
                    if !is_plain_object(item) {
                        errors.push(make_error_item(
                            format!("{label}[{item_index}]"),
                            "type_object",
                            format!("{label}[{item_index}] must be an object"),
                        ));
                    }
                }
            }
            "string_array_non_empty" => validate_optional_string_array_non_empty(value, &label, errors),
            "integer_min" => {
                let min = rule_obj.get("min").and_then(Value::as_i64).unwrap_or(0);
                validate_optional_integer_min(value, &label, min, errors);
            }
            "json_object" => {
                let Some(value) = value else {
                    continue;
                };
                let Some(obj) = value.as_object() else {
                    errors.push(make_error_item(&label, "type_object", format!("{label} must be an object")));
                    continue;
                };
                for (key, item) in obj {
                    if key.trim().is_empty() {
                        errors.push(make_error_item(&label, "empty_key", format!("{label} keys must not be empty")));
                        continue;
                    }
                    validate_json_compatible_value(Some(item), &format!("{label}.{key}"), errors);
                }
            }
            "json_compatible" => validate_json_compatible_value(value, &label, errors),
            "rules_object" => validate_rules_object(value, &label, errors),
            "computed_fields" => validate_computed_fields(value, &label, errors),
            "workflow_steps" => validate_workflow_steps(value, &label, errors),
            "constraint_defs" => validate_constraint_defs(value, &label, errors),
            "aggregate_defs" => validate_aggregate_defs(value, &label, errors),
            "window_functions" => validate_window_functions(value, &label, errors),
            "slot_bindings" => validate_slot_bindings(value, &label, errors),
            "manifest_object" => validate_manifest_object(value, &label, errors),
            "ai_providers" => validate_ai_providers(value, &label, errors),
            "conditional_required_non_empty" => {
                let expected = rule_obj
                    .get("one_of")
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default()
                    .into_iter()
                    .filter_map(|item| item.as_str().map(|value| value.trim().to_lowercase()))
                    .filter(|value| !value.is_empty())
                    .collect::<Vec<_>>();
                let when_path = rule_obj.get("when_path").and_then(Value::as_str).unwrap_or("");
                let when_value = get_value_at_path(config, when_path)
                    .and_then(Value::as_str)
                    .map(|value| value.trim().to_lowercase())
                    .unwrap_or_default();
                if !when_value.is_empty()
                    && expected.contains(&when_value)
                    && !has_meaningful_value(value)
                {
                    errors.push(make_error_item(
                        &label,
                        "conditional_required",
                        format!("{label} is required when {prefix}.{when_path} is {when_value}"),
                    ));
                }
            }
            "paired_required" => {
                let paired_path = rule_obj.get("paired_path").and_then(Value::as_str).unwrap_or("");
                let paired_value = get_value_at_path(config, paired_path);
                let has_value = has_meaningful_value(value);
                let has_paired_value = has_meaningful_value(paired_value);
                if !has_value && has_paired_value {
                    errors.push(make_error_item(
                        &label,
                        "paired_required",
                        format!("{label} is required when {prefix}.{paired_path} is provided"),
                    ));
                } else if has_value && !has_paired_value {
                    errors.push(make_error_item(
                        format!("{prefix}.{paired_path}"),
                        "paired_required",
                        format!("{prefix}.{paired_path} is required when {label} is provided"),
                    ));
                }
            }
            "op_in_allowed_ops" => {
                let allowed_path = rule_obj.get("allowed_path").and_then(Value::as_str).unwrap_or("");
                let op_value = value.and_then(Value::as_str).map(|value| value.trim().to_lowercase()).unwrap_or_default();
                let allowed = get_value_at_path(config, allowed_path)
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default()
                    .into_iter()
                    .filter_map(|item| item.as_str().map(|value| value.trim().to_lowercase()))
                    .filter(|value| !value.is_empty())
                    .collect::<Vec<_>>();
                if !op_value.is_empty() && !allowed.is_empty() && !allowed.contains(&op_value) {
                    errors.push(make_error_item(
                        &label,
                        "membership_required",
                        format!("{label} must be included in {prefix}.{allowed_path} when both are provided"),
                    ));
                }
            }
            _ => {
                errors.push(make_error_item(
                    &label,
                    "unsupported_validator_kind",
                    format!("{prefix}.contract[{index}] validator kind unsupported: {kind}"),
                ));
            }
        }
    }
}

fn build_operator_resolution(node_id: &str, node_type: &str) -> Value {
    let mut resolution = resolve_operator_metadata(node_type)
        .map(|entry| entry.to_workflow_resolution_metadata())
        .unwrap_or_else(|| {
            json!({
                "operator": node_type,
                "workflow": { "supported": false },
                "authority_source": if WORKFLOW_RUNTIME_ONLY_NODE_TYPES.contains(&node_type) {
                    "desktop_runtime_only"
                } else {
                    "node_config_contract"
                }
            })
        });
    if let Some(obj) = resolution.as_object_mut()
        && !obj.contains_key("workflow")
    {
        obj.insert("workflow".to_string(), json!({ "supported": false }));
    }
    json!({
        "node_id": node_id,
        "node_type": node_type,
        "resolution": resolution,
    })
}

fn validation_inventory(assets: &WorkflowValidationAssets) -> Value {
    json!({
        "known_node_types": assets.known_node_types,
        "contract_types": assets.node_contracts.contract_types,
        "rust_manifest_types": assets.rust_manifest_types,
        "runtime_only_types": WORKFLOW_RUNTIME_ONLY_NODE_TYPES,
        "authority_sources": [NODE_CONFIG_CONTRACT_AUTHORITY, RUST_OPERATOR_MANIFEST_AUTHORITY],
    })
}

pub(crate) fn run_workflow_contract_v1(
    req: WorkflowContractV1Req,
) -> Result<WorkflowContractV1Resp, String> {
    let assets = workflow_validation_assets()?;
    let allow_version_migration = req.allow_version_migration.unwrap_or(false);
    let require_non_empty_nodes = req.require_non_empty_nodes.unwrap_or(false);
    let validation_scope = normalized_scope(req.validation_scope.as_deref());

    let mut notes = Vec::new();
    let mut errors = Vec::new();
    let mut normalized = req.workflow_definition.clone();
    let mut operator_resolutions = Vec::new();

    if !is_plain_object(&normalized) {
        errors.push(make_error_item("workflow", "validation_error", "workflow must be an object"));
        normalized = json!({});
    }

    if let Some(obj) = normalized.as_object_mut() {
        let workflow_id = obj
            .get("workflow_id")
            .and_then(Value::as_str)
            .map(str::trim)
            .unwrap_or("");
        if workflow_id.is_empty() {
            errors.push(make_error_item(
                "workflow.workflow_id",
                "required",
                "workflow.workflow_id is required",
            ));
        }

        let version = obj
            .get("version")
            .and_then(Value::as_str)
            .map(str::trim)
            .unwrap_or("");
        if version.is_empty() {
            if allow_version_migration {
                obj.insert("version".to_string(), json!(DEFAULT_WORKFLOW_VERSION));
                notes.push(format!("workflow.version migrated to {DEFAULT_WORKFLOW_VERSION}"));
            } else {
                errors.push(make_error_item(
                    "workflow.version",
                    "required",
                    "workflow.version is required",
                ));
            }
        }

        let nodes = obj.get("nodes").and_then(Value::as_array).cloned();
        if nodes.is_none() {
            errors.push(make_error_item(
                "workflow.nodes",
                "type_array",
                "workflow.nodes must be an array",
            ));
        }

        let edges = obj.get("edges").and_then(Value::as_array).cloned();
        if edges.is_none() {
            errors.push(make_error_item(
                "workflow.edges",
                "type_array",
                "workflow.edges must be an array",
            ));
        }

        if let Some(nodes) = nodes {
            if require_non_empty_nodes && nodes.is_empty() {
                errors.push(make_error_item(
                    "workflow.nodes",
                    "array_min_items",
                    "workflow.nodes must contain at least one node",
                ));
            }

            let known_node_types = assets
                .known_node_types
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>();
            let mut unknown_node_types = BTreeSet::new();
            for (index, node) in nodes.iter().enumerate() {
                let Some(node_obj) = node.as_object() else {
                    errors.push(make_error_item(
                        format!("workflow.nodes[{index}]"),
                        "type_object",
                        format!("workflow.nodes[{index}] must be an object"),
                    ));
                    continue;
                };
                let node_id = node_obj
                    .get("id")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .unwrap_or("");
                if node_id.is_empty() {
                    errors.push(make_error_item(
                        format!("workflow.nodes[{index}].id"),
                        "required",
                        format!("workflow.nodes[{index}].id is required"),
                    ));
                }
                let node_type = node_obj
                    .get("type")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .unwrap_or("");
                if node_type.is_empty() {
                    errors.push(make_error_item(
                        format!("workflow.nodes[{index}].type"),
                        "required",
                        format!("workflow.nodes[{index}].type is required"),
                    ));
                    continue;
                }
                operator_resolutions.push(build_operator_resolution(node_id, node_type));
                if !known_node_types.contains(node_type) {
                    unknown_node_types.insert(node_type.to_string());
                    continue;
                }
                let config = node_obj
                    .get("config")
                    .cloned()
                    .unwrap_or_else(|| json!({}));
                if !is_plain_object(&config) {
                    errors.push(make_error_item(
                        format!("workflow.nodes[{index}].config"),
                        "type_object",
                        format!("workflow.nodes[{index}].config must be an object"),
                    ));
                    continue;
                }
                validate_contract_backed_node_config(
                    node_type,
                    &config,
                    &format!("workflow.nodes[{index}].config"),
                    &mut errors,
                    assets,
                );
            }

            if !unknown_node_types.is_empty() {
                errors.push(make_error_item(
                    "workflow.nodes",
                    "unknown_node_type",
                    format!(
                        "workflow contains unregistered node types: {}",
                        unknown_node_types.into_iter().collect::<Vec<_>>().join(", ")
                    ),
                ));
            }
        }

        if let Some(edges) = edges {
            for (index, edge) in edges.iter().enumerate() {
                let Some(edge_obj) = edge.as_object() else {
                    errors.push(make_error_item(
                        format!("workflow.edges[{index}]"),
                        "type_object",
                        format!("workflow.edges[{index}] must be an object"),
                    ));
                    continue;
                };
                let from = edge_obj
                    .get("from")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .unwrap_or("");
                if from.is_empty() {
                    errors.push(make_error_item(
                        format!("workflow.edges[{index}].from"),
                        "required",
                        format!("workflow.edges[{index}].from is required"),
                    ));
                }
                let to = edge_obj
                    .get("to")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .unwrap_or("");
                if to.is_empty() {
                    errors.push(make_error_item(
                        format!("workflow.edges[{index}].to"),
                        "required",
                        format!("workflow.edges[{index}].to is required"),
                    ));
                }
            }
        }
    }

    Ok(WorkflowContractV1Resp {
        ok: true,
        operator: "workflow_contract_v1".to_string(),
        status: if errors.is_empty() {
            "done".to_string()
        } else {
            "invalid".to_string()
        },
        schema_version: WORKFLOW_CONTRACT_SCHEMA_VERSION.to_string(),
        graph_contract: WORKFLOW_GRAPH_CONTRACT_AUTHORITY.to_string(),
        error_item_contract: NODE_CONFIG_VALIDATION_ERROR_CONTRACT_AUTHORITY.to_string(),
        validation_scope,
        valid: errors.is_empty(),
        normalized_workflow_definition: normalized,
        error_items: errors,
        notes,
        node_type_inventory: validation_inventory(assets),
        operator_resolutions,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workflow_contract_validation_reports_unknown_node_types() {
        let out = run_workflow_contract_v1(WorkflowContractV1Req {
            workflow_definition: json!({
                "workflow_id": "wf_unknown",
                "version": "1.0.0",
                "nodes": [{ "id": "n1", "type": "unknown_future_node" }],
                "edges": [],
            }),
            allow_version_migration: Some(false),
            require_non_empty_nodes: Some(true),
            validation_scope: Some("run".to_string()),
        })
        .expect("validation response");

        assert!(out.ok);
        assert!(!out.valid);
        assert_eq!(out.status, "invalid");
        assert!(out.error_items.iter().any(|item| {
            item.get("path").and_then(Value::as_str) == Some("workflow.nodes")
                && item.get("code").and_then(Value::as_str) == Some("unknown_node_type")
        }));
    }

    #[test]
    fn workflow_contract_validation_migrates_missing_version_only_when_allowed() {
        let out = run_workflow_contract_v1(WorkflowContractV1Req {
            workflow_definition: json!({
                "workflow_id": "wf_migrate",
                "nodes": [{ "id": "n1", "type": "ingest_files" }],
                "edges": [],
            }),
            allow_version_migration: Some(true),
            require_non_empty_nodes: Some(false),
            validation_scope: Some("authoring".to_string()),
        })
        .expect("validation response");

        assert!(out.valid);
        assert_eq!(
            out.normalized_workflow_definition
                .get("version")
                .and_then(Value::as_str),
            Some(DEFAULT_WORKFLOW_VERSION)
        );
        assert!(!out.notes.is_empty());
    }
}
