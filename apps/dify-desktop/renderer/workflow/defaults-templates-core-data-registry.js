export const NODE_CONFIG_TEMPLATES_CORE_DATA_REGISTRY = {
  schema_registry_v1_infer: {
    name: "default_schema",
    version: "v1",
    rows: [],
  },
  schema_registry_v1_get: {
    name: "default_schema",
    version: "v1",
  },
  schema_registry_v1_register: {
    name: "default_schema",
    version: "v1",
    schema: {},
  },
  schema_registry_v2_check_compat: {
    name: "default_schema",
    from_version: "v1",
    to_version: "v2",
    mode: "backward",
  },
  schema_registry_v2_suggest_migration: {
    name: "default_schema",
    from_version: "v1",
    to_version: "v2",
  },
  udf_wasm_v1: {
    rows: [],
    field: "",
    output_field: "",
    op: "identity",
  },
  time_series_v1: {
    rows: [],
    time_field: "month",
    value_field: "value",
    group_by: [],
    window: 3,
  },
  stats_v1: {
    rows: [],
    x_field: "x",
    y_field: "y",
  },
  entity_linking_v1: {
    rows: [],
    field: "entity",
    id_field: "entity_id",
  },
  table_reconstruct_v1: {
    lines: [],
    delimiter: "\\s{2,}|\\t",
  },
  feature_store_v1_upsert: {
    key_field: "id",
    rows: [],
  },
  feature_store_v1_get: {
    key: "",
  },
};
