export const NODE_CONFIG_CONTRACT_SET_SCHEMA_VERSION = "node_config_contracts.v1";
export const NODE_CONFIG_CONTRACT_SET_AUTHORITY = "contracts/desktop/node_config_contracts.v1.json";
export const NODE_CONFIG_CONTRACT_TYPES = Object.freeze([
  "aggregate_rows_v2",
  "aggregate_rows_v3",
  "aggregate_rows_v4",
  "ai_audit",
  "ai_refine",
  "ai_strategy_v1",
  "clean_md",
  "constraint_solver_v1",
  "ds_refine",
  "ingest_files",
  "join_rows_v2",
  "join_rows_v3",
  "join_rows_v4",
  "lineage_v2",
  "lineage_v3",
  "load_rows_v2",
  "load_rows_v3",
  "manual_review",
  "office_slot_fill_v1",
  "optimizer_v1",
  "parquet_io_v2",
  "plugin_registry_v1",
  "quality_check_v2",
  "quality_check_v3",
  "quality_check_v4",
  "rule_simulator_v1",
  "sql_chart_v1",
  "transform_rows_v3",
  "udf_wasm_v2",
  "window_rows_v1"
]);
export const NODE_CONFIG_CONTRACT_QUALITY_BY_TYPE = Object.freeze({
  "ingest_files": "typed",
  "clean_md": "typed",
  "manual_review": "typed",
  "ai_refine": "typed",
  "ds_refine": "typed",
  "ai_strategy_v1": "nested_shape_constrained",
  "ai_audit": "typed",
  "load_rows_v2": "enum_constrained",
  "load_rows_v3": "nested_shape_constrained",
  "transform_rows_v3": "nested_shape_constrained",
  "join_rows_v2": "enum_constrained",
  "quality_check_v2": "nested_shape_constrained",
  "quality_check_v3": "nested_shape_constrained",
  "quality_check_v4": "nested_shape_constrained",
  "aggregate_rows_v2": "nested_shape_constrained",
  "lineage_v2": "nested_shape_constrained",
  "lineage_v3": "nested_shape_constrained",
  "rule_simulator_v1": "nested_shape_constrained",
  "constraint_solver_v1": "nested_shape_constrained",
  "sql_chart_v1": "typed",
  "office_slot_fill_v1": "nested_shape_constrained",
  "join_rows_v3": "enum_constrained",
  "join_rows_v4": "enum_constrained",
  "aggregate_rows_v3": "nested_shape_constrained",
  "aggregate_rows_v4": "nested_shape_constrained",
  "window_rows_v1": "nested_shape_constrained",
  "plugin_registry_v1": "nested_shape_constrained",
  "optimizer_v1": "nested_shape_constrained",
  "parquet_io_v2": "nested_shape_constrained",
  "udf_wasm_v2": "nested_shape_constrained"
});
export const NODE_CONFIG_CONTRACTS_BY_TYPE = Object.freeze({
  "ingest_files": {
    "type": "ingest_files",
    "quality": "typed",
    "validators": [
      {
        "kind": "object",
        "path": "input_map"
      }
    ]
  },
  "clean_md": {
    "type": "clean_md",
    "quality": "typed",
    "validators": [
      {
        "kind": "boolean",
        "path": "export_canonical_bundle"
      },
      {
        "kind": "string",
        "path": "canonical_title"
      }
    ]
  },
  "manual_review": {
    "type": "manual_review",
    "quality": "typed",
    "validators": [
      {
        "kind": "string_non_empty",
        "path": "review_key"
      },
      {
        "kind": "boolean",
        "path": "default_approve"
      },
      {
        "kind": "string",
        "path": "default_reviewer"
      },
      {
        "kind": "string",
        "path": "default_comment"
      }
    ]
  },
  "ai_refine": {
    "type": "ai_refine",
    "quality": "typed",
    "validators": [
      {
        "kind": "boolean",
        "path": "reuse_existing"
      },
      {
        "kind": "boolean",
        "path": "allow_ai_on_data"
      },
      {
        "kind": "string",
        "path": "provider_name"
      },
      {
        "kind": "string",
        "path": "ai_endpoint"
      },
      {
        "kind": "string",
        "path": "ai_api_key"
      },
      {
        "kind": "string",
        "path": "ai_model"
      }
    ]
  },
  "ds_refine": {
    "type": "ds_refine",
    "quality": "typed",
    "validators": [
      {
        "kind": "boolean",
        "path": "reuse_existing"
      },
      {
        "kind": "boolean",
        "path": "allow_ai_on_data"
      },
      {
        "kind": "string",
        "path": "provider_name"
      },
      {
        "kind": "string",
        "path": "ai_endpoint"
      },
      {
        "kind": "string",
        "path": "ai_api_key"
      },
      {
        "kind": "string",
        "path": "ai_model"
      }
    ]
  },
  "ai_strategy_v1": {
    "type": "ai_strategy_v1",
    "quality": "nested_shape_constrained",
    "validators": [
      {
        "kind": "ai_providers",
        "path": "providers"
      },
      {
        "kind": "boolean",
        "path": "allow_ai_on_data"
      }
    ]
  },
  "ai_audit": {
    "type": "ai_audit",
    "quality": "typed",
    "validators": [
      {
        "kind": "boolean",
        "path": "numeric_lock"
      },
      {
        "kind": "boolean",
        "path": "citation_required"
      },
      {
        "kind": "boolean",
        "path": "recalc_verify"
      },
      {
        "kind": "integer_min",
        "path": "max_new_numbers",
        "min": 0
      },
      {
        "kind": "integer_min",
        "path": "max_metric_delta",
        "min": 0
      }
    ]
  },
  "load_rows_v2": {
    "type": "load_rows_v2",
    "quality": "enum_constrained",
    "validators": [
      {
        "kind": "enum",
        "path": "source_type",
        "allowed": [
          "jsonl",
          "csv",
          "sqlite",
          "sqlserver",
          "parquet",
          "txt",
          "pdf",
          "docx",
          "xlsx",
          "image"
        ]
      },
      {
        "kind": "string",
        "path": "source"
      },
      {
        "kind": "string_non_empty",
        "path": "query"
      },
      {
        "kind": "integer_min",
        "path": "limit",
        "min": 1
      }
    ]
  },
  "load_rows_v3": {
    "type": "load_rows_v3",
    "quality": "nested_shape_constrained",
    "validators": [
      {
        "kind": "enum",
        "path": "source_type",
        "allowed": [
          "jsonl",
          "csv",
          "sqlite",
          "sqlserver",
          "parquet",
          "txt",
          "pdf",
          "docx",
          "xlsx",
          "image"
        ]
      },
      {
        "kind": "string",
        "path": "source"
      },
      {
        "kind": "string_non_empty",
        "path": "query"
      },
      {
        "kind": "integer_min",
        "path": "limit",
        "min": 1
      },
      {
        "kind": "integer_min",
        "path": "max_retries",
        "min": 0
      },
      {
        "kind": "integer_min",
        "path": "retry_backoff_ms",
        "min": 0
      },
      {
        "kind": "string_non_empty",
        "path": "resume_token"
      },
      {
        "kind": "json_object",
        "path": "connector_options"
      }
    ]
  },
  "transform_rows_v3": {
    "type": "transform_rows_v3",
    "quality": "nested_shape_constrained",
    "validators": [
      {
        "kind": "array",
        "path": "rows"
      },
      {
        "kind": "rules_object",
        "path": "rules"
      },
      {
        "kind": "computed_fields",
        "path": "computed_fields_v3"
      }
    ]
  },
  "join_rows_v2": {
    "type": "join_rows_v2",
    "quality": "enum_constrained",
    "validators": [
      {
        "kind": "array",
        "path": "left_rows"
      },
      {
        "kind": "array",
        "path": "right_rows"
      },
      {
        "kind": "string_array_non_empty",
        "path": "left_on"
      },
      {
        "kind": "string_array_non_empty",
        "path": "right_on"
      },
      {
        "kind": "enum",
        "path": "join_type",
        "allowed": [
          "inner",
          "left",
          "right",
          "full",
          "semi",
          "anti"
        ]
      }
    ]
  },
  "quality_check_v2": {
    "type": "quality_check_v2",
    "quality": "nested_shape_constrained",
    "validators": [
      {
        "kind": "array",
        "path": "rows"
      },
      {
        "kind": "rules_object",
        "path": "rules"
      }
    ]
  },
  "quality_check_v3": {
    "type": "quality_check_v3",
    "quality": "nested_shape_constrained",
    "validators": [
      {
        "kind": "array",
        "path": "rows"
      },
      {
        "kind": "rules_object",
        "path": "rules"
      }
    ]
  },
  "quality_check_v4": {
    "type": "quality_check_v4",
    "quality": "nested_shape_constrained",
    "validators": [
      {
        "kind": "array",
        "path": "rows"
      },
      {
        "kind": "rules_object",
        "path": "rules"
      },
      {
        "kind": "string",
        "path": "rules_dsl"
      }
    ]
  },
  "aggregate_rows_v2": {
    "type": "aggregate_rows_v2",
    "quality": "nested_shape_constrained",
    "validators": [
      {
        "kind": "array",
        "path": "rows"
      },
      {
        "kind": "string_array_non_empty",
        "path": "group_by"
      },
      {
        "kind": "aggregate_defs",
        "path": "aggregates"
      }
    ]
  },
  "lineage_v2": {
    "type": "lineage_v2",
    "quality": "nested_shape_constrained",
    "validators": [
      {
        "kind": "rules_object",
        "path": "rules"
      },
      {
        "kind": "computed_fields",
        "path": "computed_fields_v3"
      }
    ]
  },
  "lineage_v3": {
    "type": "lineage_v3",
    "quality": "nested_shape_constrained",
    "validators": [
      {
        "kind": "rules_object",
        "path": "rules"
      },
      {
        "kind": "computed_fields",
        "path": "computed_fields_v3"
      },
      {
        "kind": "workflow_steps",
        "path": "workflow_steps"
      },
      {
        "kind": "array",
        "path": "rows"
      }
    ]
  },
  "rule_simulator_v1": {
    "type": "rule_simulator_v1",
    "quality": "nested_shape_constrained",
    "validators": [
      {
        "kind": "array",
        "path": "rows"
      },
      {
        "kind": "rules_object",
        "path": "rules"
      },
      {
        "kind": "rules_object",
        "path": "candidate_rules"
      }
    ]
  },
  "constraint_solver_v1": {
    "type": "constraint_solver_v1",
    "quality": "nested_shape_constrained",
    "validators": [
      {
        "kind": "array",
        "path": "rows"
      },
      {
        "kind": "constraint_defs",
        "path": "constraints"
      }
    ]
  },
  "sql_chart_v1": {
    "type": "sql_chart_v1",
    "quality": "typed",
    "validators": [
      {
        "kind": "array",
        "path": "rows"
      },
      {
        "kind": "string_non_empty",
        "path": "chart_type"
      },
      {
        "kind": "string_non_empty",
        "path": "category_field"
      },
      {
        "kind": "string_non_empty",
        "path": "value_field"
      },
      {
        "kind": "string_non_empty",
        "path": "series_field"
      },
      {
        "kind": "integer_min",
        "path": "top_n",
        "min": 1
      }
    ]
  },
  "office_slot_fill_v1": {
    "type": "office_slot_fill_v1",
    "quality": "nested_shape_constrained",
    "validators": [
      {
        "kind": "enum",
        "path": "template_kind",
        "allowed": [
          "docx",
          "pptx",
          "xlsx"
        ]
      },
      {
        "kind": "string_non_empty",
        "path": "template_version"
      },
      {
        "kind": "string_array_non_empty",
        "path": "required_slots"
      },
      {
        "kind": "slot_bindings",
        "path": "slots"
      },
      {
        "kind": "string_non_empty",
        "path": "chart_source_node"
      }
    ]
  },
  "join_rows_v3": {
    "type": "join_rows_v3",
    "quality": "enum_constrained",
    "validators": [
      {
        "kind": "array",
        "path": "left_rows"
      },
      {
        "kind": "array",
        "path": "right_rows"
      },
      {
        "kind": "string_array_non_empty",
        "path": "left_on"
      },
      {
        "kind": "string_array_non_empty",
        "path": "right_on"
      },
      {
        "kind": "enum",
        "path": "join_type",
        "allowed": [
          "inner",
          "left",
          "right",
          "full",
          "semi",
          "anti"
        ]
      },
      {
        "kind": "enum",
        "path": "strategy",
        "allowed": [
          "auto",
          "hash",
          "sort_merge"
        ]
      },
      {
        "kind": "integer_min",
        "path": "chunk_size",
        "min": 1
      }
    ]
  },
  "join_rows_v4": {
    "type": "join_rows_v4",
    "quality": "enum_constrained",
    "validators": [
      {
        "kind": "array",
        "path": "left_rows"
      },
      {
        "kind": "array",
        "path": "right_rows"
      },
      {
        "kind": "string_array_non_empty",
        "path": "left_on"
      },
      {
        "kind": "string_array_non_empty",
        "path": "right_on"
      },
      {
        "kind": "enum",
        "path": "join_type",
        "allowed": [
          "inner",
          "left",
          "right",
          "full",
          "semi",
          "anti"
        ]
      },
      {
        "kind": "enum",
        "path": "strategy",
        "allowed": [
          "auto",
          "hash",
          "sort_merge"
        ]
      },
      {
        "kind": "integer_min",
        "path": "chunk_size",
        "min": 1
      },
      {
        "kind": "boolean",
        "path": "enable_bloom"
      }
    ]
  },
  "aggregate_rows_v3": {
    "type": "aggregate_rows_v3",
    "quality": "nested_shape_constrained",
    "validators": [
      {
        "kind": "array",
        "path": "rows"
      },
      {
        "kind": "string_array_non_empty",
        "path": "group_by"
      },
      {
        "kind": "aggregate_defs",
        "path": "aggregates"
      },
      {
        "kind": "integer_min",
        "path": "approx_sample_size",
        "min": 1
      }
    ]
  },
  "aggregate_rows_v4": {
    "type": "aggregate_rows_v4",
    "quality": "nested_shape_constrained",
    "validators": [
      {
        "kind": "array",
        "path": "rows"
      },
      {
        "kind": "string_array_non_empty",
        "path": "group_by"
      },
      {
        "kind": "aggregate_defs",
        "path": "aggregates"
      },
      {
        "kind": "integer_min",
        "path": "approx_sample_size",
        "min": 1
      },
      {
        "kind": "boolean",
        "path": "verify_exact"
      },
      {
        "kind": "integer_min",
        "path": "parallel_workers",
        "min": 1
      }
    ]
  },
  "window_rows_v1": {
    "type": "window_rows_v1",
    "quality": "nested_shape_constrained",
    "validators": [
      {
        "kind": "array",
        "path": "rows"
      },
      {
        "kind": "string_array_non_empty",
        "path": "partition_by"
      },
      {
        "kind": "string_non_empty",
        "path": "order_by"
      },
      {
        "kind": "window_functions",
        "path": "functions"
      }
    ]
  },
  "plugin_registry_v1": {
    "type": "plugin_registry_v1",
    "quality": "nested_shape_constrained",
    "validators": [
      {
        "kind": "enum",
        "path": "op",
        "allowed": [
          "list",
          "get",
          "register",
          "upsert",
          "delete",
          "unregister"
        ]
      },
      {
        "kind": "string",
        "path": "plugin"
      },
      {
        "kind": "manifest_object",
        "path": "manifest"
      },
      {
        "kind": "conditional_required_non_empty",
        "path": "manifest.command",
        "when_path": "op",
        "one_of": [
          "register",
          "upsert"
        ]
      }
    ]
  },
  "optimizer_v1": {
    "type": "optimizer_v1",
    "quality": "nested_shape_constrained",
    "validators": [
      {
        "kind": "row_objects",
        "path": "rows"
      },
      {
        "kind": "integer_min",
        "path": "row_count_hint",
        "min": 0
      },
      {
        "kind": "boolean",
        "path": "prefer_arrow"
      },
      {
        "kind": "json_object",
        "path": "join_hint"
      },
      {
        "kind": "json_object",
        "path": "aggregate_hint"
      }
    ]
  },
  "parquet_io_v2": {
    "type": "parquet_io_v2",
    "quality": "nested_shape_constrained",
    "validators": [
      {
        "kind": "enum",
        "path": "op",
        "allowed": [
          "write",
          "save",
          "read",
          "load",
          "inspect",
          "inspect_schema",
          "merge_small"
        ]
      },
      {
        "kind": "string",
        "path": "path"
      },
      {
        "kind": "row_objects",
        "path": "rows"
      },
      {
        "kind": "enum",
        "path": "parquet_mode",
        "allowed": [
          "typed",
          "payload"
        ]
      },
      {
        "kind": "integer_min",
        "path": "limit",
        "min": 1
      },
      {
        "kind": "string_array_non_empty",
        "path": "columns"
      },
      {
        "kind": "string_non_empty",
        "path": "predicate_field"
      },
      {
        "kind": "json_compatible",
        "path": "predicate_eq"
      },
      {
        "kind": "string_array_non_empty",
        "path": "partition_by"
      },
      {
        "kind": "enum",
        "path": "compression",
        "allowed": [
          "snappy",
          "gzip",
          "zstd",
          "none",
          "uncompressed"
        ]
      },
      {
        "kind": "boolean",
        "path": "recursive"
      },
      {
        "kind": "enum",
        "path": "schema_mode",
        "allowed": [
          "additive",
          "strict",
          "widen"
        ]
      },
      {
        "kind": "paired_required",
        "path": "predicate_field",
        "paired_path": "predicate_eq",
        "message": "predicate_field/predicate_eq must be provided together for read/load"
      }
    ]
  },
  "udf_wasm_v2": {
    "type": "udf_wasm_v2",
    "quality": "nested_shape_constrained",
    "validators": [
      {
        "kind": "row_objects",
        "path": "rows"
      },
      {
        "kind": "string",
        "path": "field"
      },
      {
        "kind": "string",
        "path": "output_field"
      },
      {
        "kind": "string",
        "path": "op"
      },
      {
        "kind": "string",
        "path": "wasm_base64"
      },
      {
        "kind": "integer_min",
        "path": "max_output_bytes",
        "min": 1
      },
      {
        "kind": "string_non_empty",
        "path": "signed_token"
      },
      {
        "kind": "string_array_non_empty",
        "path": "allowed_ops"
      },
      {
        "kind": "op_in_allowed_ops",
        "path": "op",
        "allowed_path": "allowed_ops"
      }
    ]
  }
});
