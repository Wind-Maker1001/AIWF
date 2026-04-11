const CLEANING_SPEC_V2_SCHEMA_VERSION = "cleaning_spec.v2";
const QUALITY_GATE_KEYS = [
  "max_invalid_rows",
  "max_filtered_rows",
  "min_output_rows",
  "max_invalid_ratio",
  "max_required_missing_ratio",
  "max_duplicate_rows_removed",
  "allow_empty_output",
  "numeric_parse_rate_min",
  "date_parse_rate_min",
  "duplicate_key_ratio_max",
  "blank_row_ratio_max",
];

function clone(value) {
  return JSON.parse(JSON.stringify(value));
}

function asObject(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? clone(value) : {};
}

function asArray(value) {
  return Array.isArray(value) ? clone(value) : [];
}

function asStringArray(value) {
  return asArray(value).map((item) => String(item || "").trim()).filter(Boolean);
}

function pickQualityGates(source = {}) {
  const out = {};
  QUALITY_GATE_KEYS.forEach((key) => {
    if (source[key] !== undefined) out[key] = source[key];
  });
  return out;
}

function normalizeCleaningSpec(raw) {
  const source = raw && typeof raw === "object" && !Array.isArray(raw) && raw.cleaning_spec_v2 && typeof raw.cleaning_spec_v2 === "object"
    ? raw.cleaning_spec_v2
    : raw;
  if (!source || typeof source !== "object" || Array.isArray(source)) return null;
  if (String(source.schema_version || "").trim() !== CLEANING_SPEC_V2_SCHEMA_VERSION) return null;
  return {
    schema_version: CLEANING_SPEC_V2_SCHEMA_VERSION,
    ingest: asObject(source.ingest),
    schema: asObject(source.schema),
    transform: asObject(source.transform),
    quality: asObject(source.quality),
    artifacts: asObject(source.artifacts),
    audit: asObject(source.audit),
  };
}

function compileLegacyRulesToCleaningSpec(rules, options = {}) {
  const source = asObject(rules);
  return {
    schema_version: CLEANING_SPEC_V2_SCHEMA_VERSION,
    ingest: asObject(options.ingest),
    schema: {
      canonical_profile: String(source.canonical_profile || "").trim().toLowerCase(),
      defaults: asObject(source.default_values),
      unique_keys: asStringArray(source.deduplicate_by),
      header_aliases: {},
      auto_normalize_headers: false,
      header_normalizer: "none",
    },
    transform: {
      rename_map: asObject(source.rename_map),
      casts: asObject(source.casts),
      required_fields: asStringArray(source.required_fields),
      default_values: asObject(source.default_values),
      include_fields: asArray(source.include_fields),
      exclude_fields: asArray(source.exclude_fields),
      filters: asArray(source.filters),
      deduplicate_by: asStringArray(source.deduplicate_by),
      deduplicate_keep: String(source.deduplicate_keep || "last").trim().toLowerCase() || "last",
      sort_by: asArray(source.sort_by),
      aggregate: asObject(source.aggregate),
      null_values: asStringArray(source.null_values),
      trim_strings: source.trim_strings !== false,
      computed_fields: asObject(source.computed_fields),
      string_ops: asArray(source.string_ops),
      date_ops: asArray(source.date_ops),
      field_ops: asArray(source.field_ops),
    },
    quality: {
      required_fields: asStringArray(source.required_fields),
      gates: pickQualityGates(source),
      image_rules: asObject(source.image_rules),
      xlsx_rules: asObject(source.xlsx_rules),
    },
    artifacts: asObject(options.artifacts),
    audit: {
      enabled: true,
      sample_limit: 5,
      lineage: true,
      warnings: ["compiled from legacy rules"],
    },
  };
}

function deriveLegacyRulesFromCleaningSpec(spec) {
  const normalized = normalizeCleaningSpec(spec);
  if (!normalized) return {};
  const transform = asObject(normalized.transform);
  const quality = asObject(normalized.quality);
  const gates = asObject(quality.gates);
  const rules = {
    rename_map: asObject(transform.rename_map),
    casts: asObject(transform.casts),
    required_fields: asStringArray(transform.required_fields),
    default_values: asObject(transform.default_values),
    include_fields: asArray(transform.include_fields),
    exclude_fields: asArray(transform.exclude_fields),
    filters: asArray(transform.filters),
    deduplicate_by: asStringArray(transform.deduplicate_by),
    deduplicate_keep: String(transform.deduplicate_keep || "last").trim().toLowerCase() || "last",
    sort_by: asArray(transform.sort_by),
    aggregate: asObject(transform.aggregate),
    null_values: asStringArray(transform.null_values),
    trim_strings: transform.trim_strings !== false,
    computed_fields: asObject(transform.computed_fields),
    string_ops: asArray(transform.string_ops),
    date_ops: asArray(transform.date_ops),
    field_ops: asArray(transform.field_ops),
  };
  if ((!Array.isArray(rules.required_fields) || rules.required_fields.length === 0) && Array.isArray(quality.required_fields)) {
    rules.required_fields = asStringArray(quality.required_fields);
  }
  Object.assign(rules, gates);
  const canonicalProfile = String(normalized.schema?.canonical_profile || "").trim().toLowerCase();
  if (canonicalProfile) rules.canonical_profile = canonicalProfile;
  return rules;
}

function mergeCleaningSpec(base, override) {
  const left = normalizeCleaningSpec(base) || normalizeCleaningSpec({ schema_version: CLEANING_SPEC_V2_SCHEMA_VERSION, ingest: {}, schema: {}, transform: {}, quality: {}, artifacts: {}, audit: {} });
  const right = normalizeCleaningSpec(override);
  if (!right) return left;
  return {
    schema_version: CLEANING_SPEC_V2_SCHEMA_VERSION,
    ingest: { ...asObject(left.ingest), ...asObject(right.ingest) },
    schema: { ...asObject(left.schema), ...asObject(right.schema) },
    transform: { ...asObject(left.transform), ...asObject(right.transform) },
    quality: { ...asObject(left.quality), ...asObject(right.quality) },
    artifacts: { ...asObject(left.artifacts), ...asObject(right.artifacts) },
    audit: { ...asObject(left.audit), ...asObject(right.audit) },
  };
}

function applyCleaningSpecToParams(params = {}, spec) {
  const normalized = normalizeCleaningSpec(spec);
  if (!normalized) return { ...(params || {}) };
  const next = { ...(params || {}) };
  next.cleaning_spec_v2 = mergeCleaningSpec(next.cleaning_spec_v2, normalized);

  const templateQualityRules = {
    ...asObject(normalized.quality?.gates),
  };
  const requiredFields = asStringArray(normalized.quality?.required_fields);
  if (requiredFields.length > 0) templateQualityRules.required_fields = requiredFields;
  next.quality_rules = { ...templateQualityRules, ...asObject(next.quality_rules) };
  next.image_rules = { ...asObject(normalized.quality?.image_rules), ...asObject(next.image_rules) };
  next.xlsx_rules = { ...asObject(normalized.quality?.xlsx_rules), ...asObject(next.xlsx_rules) };
  next.sheet_profiles = { ...asObject(normalized.ingest?.sheet_profiles), ...asObject(next.sheet_profiles) };
  if (!String(next.canonical_profile || "").trim()) {
    next.canonical_profile = String(normalized.schema?.canonical_profile || "").trim().toLowerCase();
  }
  if (next.blank_output_expected === undefined && normalized.quality?.gates?.blank_output_expected !== undefined) {
    next.blank_output_expected = Boolean(normalized.quality.gates.blank_output_expected);
  }
  if (!String(next.template_expected_profile || "").trim()) {
    next.template_expected_profile = String(normalized.schema?.template_expected_profile || normalized.schema?.canonical_profile || "").trim().toLowerCase();
  }
  return next;
}

function normalizeCleaningTemplatePayload(raw) {
  const source = raw && typeof raw === "object" && !Array.isArray(raw) ? clone(raw) : {};
  const paramsSchema = asObject(source.params_schema);
  const cleaningSpec = normalizeCleaningSpec(source) || normalizeCleaningSpec(source.cleaning_spec_v2);
  if (cleaningSpec) {
    return {
      template_format: "cleaning_spec_v2",
      cleaning_spec_v2: cleaningSpec,
      rules: deriveLegacyRulesFromCleaningSpec(cleaningSpec),
      params_schema: paramsSchema,
    };
  }
  if (source.rules && typeof source.rules === "object" && !Array.isArray(source.rules)) {
    const compiled = compileLegacyRulesToCleaningSpec(source.rules);
    return {
      template_format: "legacy_rules",
      cleaning_spec_v2: compiled,
      rules: asObject(source.rules),
      params_schema: paramsSchema,
    };
  }
  return null;
}

module.exports = {
  CLEANING_SPEC_V2_SCHEMA_VERSION,
  applyCleaningSpecToParams,
  compileLegacyRulesToCleaningSpec,
  deriveLegacyRulesFromCleaningSpec,
  mergeCleaningSpec,
  normalizeCleaningSpec,
  normalizeCleaningTemplatePayload,
};
