use crate::api_types::{CompiledFilter, FilterOp};
use regex::Regex;
use serde_json::{Map, Value, json};
use std::collections::HashMap;

#[path = "rules/basics.rs"]
mod basics;
pub(crate) use basics::{
    as_array_str, as_bool, cast_value, is_missing, rule_get, value_to_f64, value_to_i64,
    value_to_string,
};

#[path = "rules/expr.rs"]
mod expr;
pub(crate) use expr::{apply_expression_fields, apply_string_and_date_ops};

#[path = "rules/filters.rs"]
mod filters;
pub(crate) use filters::{compile_filters, filter_match_compiled};

#[path = "rules/ordering.rs"]
mod ordering;
pub(crate) use ordering::{compare_rows, dedup_key, value_to_string_or_null};
