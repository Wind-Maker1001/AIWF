function normalizeGroupLabel(value) {
  const label = String(value || "").trim();
  return label || "未分组";
}

function findNodeCatalogEntry(nodeCatalog = [], type) {
  const normalized = String(type || "").trim().toLowerCase();
  return (Array.isArray(nodeCatalog) ? nodeCatalog : []).find((item) => (
    String(item?.type || "").trim().toLowerCase() === normalized
  )) || null;
}

function formatNodePolicyHint(nodeCatalog = [], rawType, fallbackLabel = "") {
  const entry = findNodeCatalogEntry(nodeCatalog, rawType);
  const label = String(entry?.name || fallbackLabel || rawType || "").trim();
  const groupLabel = normalizeGroupLabel(entry?.group);
  const policySection = String(entry?.policy_section || "").trim();
  const policySource = String(entry?.policy_source || "").trim();
  const suffix = policySection
    ? ` · ${groupLabel} [${policySection}]`
    : (groupLabel && groupLabel !== "未分组" ? ` · ${groupLabel}` : "");
  const sourceSuffix = policySource ? ` · ${policySource}` : "";
  return {
    label,
    statusLabel: `${label}${suffix}${sourceSuffix}`,
    groupLabel,
    policySection,
    policySource,
  };
}

function getNodePolicyInfo(nodeCatalog = [], node) {
  const entry = findNodeCatalogEntry(nodeCatalog, node?.type);
  if (!entry) return null;
  const group = String(entry.group || "").trim();
  const policySection = String(entry.policy_section || "").trim();
  const policySource = String(entry.policy_source || "").trim();
  if (!group && !policySection && !policySource) {
    return null;
  }
  return {
    group,
    policySection,
    policySource,
  };
}

function describePolicyOwnership(info = {}) {
  const source = String(info?.policySource || "").trim().toLowerCase();
  if (source === "rust_manifest") {
    return {
      owner: "Rust 平台",
      identityRule: "节点 type / identity 不可在前端改写",
      boundary: "前端可调整展示与使用方式，但不拥有 operator identity / metadata 真相。",
    };
  }
  if (source === "local_policy") {
    return {
      owner: "前端 authoring",
      identityRule: "节点 type 由前端 policy 管理，可随本地 authoring 演进",
      boundary: "前端可直接调整本地节点展示与交互，但不应伪装成 Rust 平台能力节点。",
    };
  }
  return {
    owner: "-",
    identityRule: "未声明来源时禁止改写节点 identity",
    boundary: "未声明来源，禁止扩张行为边界。",
  };
}

export {
  normalizeGroupLabel,
  findNodeCatalogEntry,
  formatNodePolicyHint,
  getNodePolicyInfo,
  describePolicyOwnership,
};
