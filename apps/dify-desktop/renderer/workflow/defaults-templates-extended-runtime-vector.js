export const NODE_CONFIG_TEMPLATES_EXTENDED_RUNTIME_VECTOR = {
  vector_index_v2_build: {
    shard: "default",
    rows: [],
    id_field: "id",
    text_field: "text",
    metadata_fields: [],
    replace: false,
  },
  vector_index_v2_search: {
    query: "",
    top_k: 5,
    shard: "",
    filter_eq: {},
    rerank_meta_field: "",
    rerank_meta_weight: 0,
  },
  vector_index_v2_eval: {
    run_id: "",
    shard: "",
    top_k: 5,
    cases: [],
  },
};
