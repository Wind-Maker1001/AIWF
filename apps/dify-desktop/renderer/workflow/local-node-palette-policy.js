const LOCAL_NODE_TYPES = Object.freeze([
  "ingest_files",
  "clean_md",
  "compute_rust",
  "manual_review",
  "sql_chart_v1",
  "office_slot_fill_v1",
  "ai_strategy_v1",
  "ds_refine",
  "ai_refine",
  "ai_audit",
  "md_output",
]);

const LOCAL_NODE_SECTIONS = Object.freeze([
  Object.freeze({
    id: "local_foundation",
    label: "基础编排",
    types: Object.freeze(["ingest_files", "clean_md", "compute_rust"]),
  }),
  Object.freeze({
    id: "local_ai",
    label: "AI 编排",
    types: Object.freeze(["ai_strategy_v1", "ds_refine", "ai_refine"]),
  }),
  Object.freeze({
    id: "local_governance",
    label: "治理与审核",
    types: Object.freeze(["manual_review", "ai_audit"]),
  }),
  Object.freeze({
    id: "local_output",
    label: "输出编排",
    types: Object.freeze(["sql_chart_v1", "office_slot_fill_v1", "md_output"]),
  }),
]);

const LOCAL_NODE_PINNED_ORDER = Object.freeze([
  "ingest_files",
  "clean_md",
  "ds_refine",
  "ai_refine",
  "ai_audit",
  "manual_review",
  "compute_rust",
  "sql_chart_v1",
  "office_slot_fill_v1",
  "md_output",
  "ai_strategy_v1",
]);

function uniqueSorted(values) {
  return Array.from(new Set(values.map((value) => String(value || "").trim()).filter(Boolean))).sort();
}

function buildLocalNodePalettePolicy(presentations = {}) {
  const errors = [];
  const sectionTypeMap = new Map();
  const invalidSections = [];

  for (const section of LOCAL_NODE_SECTIONS) {
    const id = String(section?.id || "").trim();
    const label = String(section?.label || "").trim();
    const types = uniqueSorted(Array.isArray(section?.types) ? section.types : []);
    if (!id || !label || types.length === 0) {
      invalidSections.push(id || "(missing-id)");
      continue;
    }
    for (const type of types) {
      if (sectionTypeMap.has(type)) {
        const existing = sectionTypeMap.get(type);
        errors.push(`local node palette section type duplicated: ${type} (${existing.id}, ${id})`);
        continue;
      }
      sectionTypeMap.set(type, { id, label });
    }
  }

  const pinnedIndex = new Map();
  const duplicatePinnedTypes = [];
  LOCAL_NODE_PINNED_ORDER.forEach((type, index) => {
    const normalized = String(type || "").trim();
    if (!normalized) return;
    if (pinnedIndex.has(normalized)) {
      duplicatePinnedTypes.push(normalized);
      return;
    }
    pinnedIndex.set(normalized, index);
  });

  const missingSectionTypes = LOCAL_NODE_TYPES.filter((type) => !sectionTypeMap.has(type));
  const missingPresentationTypes = LOCAL_NODE_TYPES.filter((type) => !Object.prototype.hasOwnProperty.call(presentations, type));
  const stalePresentationTypes = Object.keys(presentations).filter((type) => !LOCAL_NODE_TYPES.includes(type));
  const invalidPresentationEntries = Object.keys(presentations).filter((type) => {
    const item = presentations[type];
    return !item
      || typeof item !== "object"
      || Array.isArray(item)
      || !String(item.name || "").trim()
      || !String(item.desc || "").trim();
  });
  const stalePinnedTypes = [...pinnedIndex.keys()].filter((type) => !LOCAL_NODE_TYPES.includes(type));

  if (invalidSections.length > 0) {
    errors.push(`local node palette sections invalid: ${invalidSections.join(", ")}`);
  }
  if (missingSectionTypes.length > 0) {
    errors.push(`local node palette policy missing node types: ${missingSectionTypes.join(", ")}`);
  }
  if (missingPresentationTypes.length > 0) {
    errors.push(`local node presentations missing node types: ${missingPresentationTypes.join(", ")}`);
  }
  if (stalePresentationTypes.length > 0) {
    errors.push(`local node presentations have stale node types: ${stalePresentationTypes.join(", ")}`);
  }
  if (invalidPresentationEntries.length > 0) {
    errors.push(`local node presentations invalid entries: ${invalidPresentationEntries.join(", ")}`);
  }
  if (duplicatePinnedTypes.length > 0) {
    errors.push(`local node palette pinned order duplicated: ${uniqueSorted(duplicatePinnedTypes).join(", ")}`);
  }
  if (stalePinnedTypes.length > 0) {
    errors.push(`local node palette pinned order has stale node types: ${stalePinnedTypes.join(", ")}`);
  }

  const sectionOrder = new Map(LOCAL_NODE_SECTIONS.map((section, index) => [section.id, index]));
  const entries = LOCAL_NODE_TYPES.map((type) => {
    const section = sectionTypeMap.get(type);
    const presentation = presentations[type] || {};
    return {
      type,
      name: String(presentation.name || "").trim(),
      desc: String(presentation.desc || "").trim(),
      group: section ? section.label : "",
      policy_section: section ? section.id : "",
      policy_source: "local_policy",
      pinned_rank: pinnedIndex.has(type) ? pinnedIndex.get(type) : Number.MAX_SAFE_INTEGER,
    };
  }).sort((left, right) => {
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
    entries: entries.map(({ pinned_rank: _pinnedRank, ...entry }) => Object.freeze(entry)),
    errors,
    details: {
      invalidSections: uniqueSorted(invalidSections),
      missingSectionTypes: uniqueSorted(missingSectionTypes),
      missingPresentationTypes: uniqueSorted(missingPresentationTypes),
      stalePresentationTypes: uniqueSorted(stalePresentationTypes),
      invalidPresentationEntries: uniqueSorted(invalidPresentationEntries),
      duplicatePinnedTypes: uniqueSorted(duplicatePinnedTypes),
      stalePinnedTypes: uniqueSorted(stalePinnedTypes),
    },
  };
}

export {
  LOCAL_NODE_TYPES,
  LOCAL_NODE_SECTIONS,
  LOCAL_NODE_PINNED_ORDER,
  buildLocalNodePalettePolicy,
};
