const RUST_OPERATOR_PALETTE_SECTIONS = Object.freeze([
  Object.freeze({ id: "data_access", label: "数据接入", domains: Object.freeze(["storage", "schema"]) }),
  Object.freeze({ id: "data_processing", label: "数据处理", domains: Object.freeze(["transform", "join", "analytics", "execution"]) }),
  Object.freeze({ id: "workflow_analysis", label: "流程分析", domains: Object.freeze(["workflow", "analysis"]) }),
  Object.freeze({ id: "intelligence", label: "智能索引", domains: Object.freeze(["intelligence"]) }),
  Object.freeze({ id: "governance", label: "治理平台", domains: Object.freeze(["governance", "platform"]) }),
  Object.freeze({ id: "extensibility", label: "扩展能力", domains: Object.freeze(["integration"]) }),
]);

const RUST_OPERATOR_PALETTE_PINNED_ORDER = Object.freeze([
  "load_rows_v2",
  "load_rows_v3",
  "transform_rows_v3",
  "join_rows_v2",
  "join_rows_v3",
  "join_rows_v4",
  "aggregate_rows_v2",
  "aggregate_rows_v3",
  "aggregate_rows_v4",
  "quality_check_v2",
  "quality_check_v3",
  "quality_check_v4",
  "lineage_v2",
  "lineage_v3",
  "parquet_io_v2",
  "udf_wasm_v2",
  "vector_index_v2_build",
  "vector_index_v2_search",
  "plugin_registry_v1",
  "plugin_operator_v1",
]);

function uniqueSorted(values) {
  return Array.from(new Set(values.map((value) => String(value || "").trim()).filter(Boolean))).sort();
}

function buildRustOperatorPalettePolicy(metadataByType = {}, presentations = {}) {
  const errors = [];
  const desktopTypes = uniqueSorted(Object.keys(metadataByType));
  const presentationTypes = uniqueSorted(Object.keys(presentations));

  const sectionDomainMap = new Map();
  const invalidSections = [];
  for (const section of RUST_OPERATOR_PALETTE_SECTIONS) {
    const id = String(section?.id || "").trim();
    const label = String(section?.label || "").trim();
    const domains = uniqueSorted(Array.isArray(section?.domains) ? section.domains : []);
    if (!id || !label || domains.length === 0) {
      invalidSections.push(id || "(missing-id)");
      continue;
    }
    for (const domain of domains) {
      if (sectionDomainMap.has(domain)) {
        const existing = sectionDomainMap.get(domain);
        errors.push(`rust operator palette section domain duplicated: ${domain} (${existing.id}, ${id})`);
        continue;
      }
      sectionDomainMap.set(domain, { id, label });
    }
  }

  const pinnedIndex = new Map();
  const duplicatePinnedOperators = [];
  RUST_OPERATOR_PALETTE_PINNED_ORDER.forEach((operator, index) => {
    const normalized = String(operator || "").trim();
    if (!normalized) return;
    if (pinnedIndex.has(normalized)) {
      duplicatePinnedOperators.push(normalized);
      return;
    }
    pinnedIndex.set(normalized, index);
  });

  const missingPaletteSectionDomains = [];
  const missingPresentationOperators = [];
  const invalidPresentationEntries = [];

  const entries = desktopTypes.map((type) => {
    const metadata = metadataByType[type] || {};
    const presentation = presentations[type];
    const domain = String(metadata.domain || "").trim();
    const section = sectionDomainMap.get(domain);
    if (!section) {
      missingPaletteSectionDomains.push(domain || `(missing-domain:${type})`);
    }
    if (!presentation) {
      missingPresentationOperators.push(type);
    } else if (!String(presentation.name || "").trim() || !String(presentation.desc || "").trim()) {
      invalidPresentationEntries.push(type);
    }
    return {
      type,
      name: String(presentation?.name || "").trim(),
      desc: String(presentation?.desc || "").trim(),
      group: section ? section.label : "",
      policy_section: section ? section.id : "",
      policy_source: "rust_manifest",
      domain,
      pinned_rank: pinnedIndex.has(type) ? pinnedIndex.get(type) : Number.MAX_SAFE_INTEGER,
    };
  });

  const stalePresentationOperators = presentationTypes.filter((type) => !desktopTypes.includes(type));
  const stalePinnedOperators = [...pinnedIndex.keys()].filter((type) => !desktopTypes.includes(type));

  if (invalidSections.length > 0) {
    errors.push(`rust operator palette sections invalid: ${invalidSections.join(", ")}`);
  }
  if (duplicatePinnedOperators.length > 0) {
    errors.push(`rust operator palette pinned order duplicated: ${uniqueSorted(duplicatePinnedOperators).join(", ")}`);
  }
  if (missingPaletteSectionDomains.length > 0) {
    errors.push(`rust operator palette policy missing domains: ${uniqueSorted(missingPaletteSectionDomains).join(", ")}`);
  }
  if (missingPresentationOperators.length > 0) {
    errors.push(`rust operator presentations missing desktop-exposable operators: ${uniqueSorted(missingPresentationOperators).join(", ")}`);
  }
  if (invalidPresentationEntries.length > 0) {
    errors.push(`rust operator presentations invalid entries: ${uniqueSorted(invalidPresentationEntries).join(", ")}`);
  }
  if (stalePresentationOperators.length > 0) {
    errors.push(`rust operator presentations have stale operators: ${stalePresentationOperators.join(", ")}`);
  }
  if (stalePinnedOperators.length > 0) {
    errors.push(`rust operator palette pinned order has stale operators: ${stalePinnedOperators.join(", ")}`);
  }

  const sectionOrder = new Map(RUST_OPERATOR_PALETTE_SECTIONS.map((section, index) => [section.id, index]));
  entries.sort((left, right) => {
    const sectionDelta = (sectionOrder.get(left.policy_section) ?? Number.MAX_SAFE_INTEGER)
      - (sectionOrder.get(right.policy_section) ?? Number.MAX_SAFE_INTEGER);
    if (sectionDelta !== 0) return sectionDelta;
    if (left.pinned_rank !== right.pinned_rank) return left.pinned_rank - right.pinned_rank;
    const nameDelta = left.name.localeCompare(right.name, "zh-Hans-CN");
    if (nameDelta !== 0) return nameDelta;
    return left.type.localeCompare(right.type);
  });

  return {
    ok: errors.length === 0,
    entries: entries.map(({ domain: _domain, pinned_rank: _pinnedRank, ...entry }) => Object.freeze(entry)),
    errors,
    details: {
      invalidSections: uniqueSorted(invalidSections),
      missingPaletteSectionDomains: uniqueSorted(missingPaletteSectionDomains),
      duplicatePinnedOperators: uniqueSorted(duplicatePinnedOperators),
      stalePinnedOperators,
      missingPresentationOperators: uniqueSorted(missingPresentationOperators),
      stalePresentationOperators,
      invalidPresentationEntries: uniqueSorted(invalidPresentationEntries),
    },
  };
}

export {
  RUST_OPERATOR_PALETTE_SECTIONS,
  RUST_OPERATOR_PALETTE_PINNED_ORDER,
  buildRustOperatorPalettePolicy,
};
