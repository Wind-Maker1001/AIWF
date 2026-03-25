import { LOCAL_NODE_TYPES } from "./local-node-palette-policy.js";
import {
  describePolicyOwnership,
  findNodeCatalogEntry,
  formatNodePolicyHint,
  normalizeGroupLabel,
} from "./node-policy-support.js";

function createWorkflowPaletteUi(els, deps = {}) {
  const {
    setStatus = () => {},
    nodeCatalog = [],
    defaultNodeConfigFn = () => ({}),
    createNode = () => "",
    selectNodeIds = () => {},
    renderAll = () => {},
    computeDropPosition = () => ({ x: 60, y: 60 }),
    createElement = (tag) => document.createElement(tag),
  } = deps;

  const simpleNodeTypes = new Set(
    (Array.isArray(LOCAL_NODE_TYPES) ? LOCAL_NODE_TYPES : []).map((type) => (
      String(type || "").trim().toLowerCase()
    ))
  );

  function buildPaletteGroups(list) {
    const groups = [];
    const groupIndexByKey = new Map();
    list.forEach((item) => {
      const groupLabel = normalizeGroupLabel(item?.group);
      const groupKey = `${String(item?.policy_section || "").trim()}::${groupLabel}`;
      if (!groupIndexByKey.has(groupKey)) {
        groupIndexByKey.set(groupKey, groups.length);
        groups.push({ key: groupKey, label: groupLabel, items: [] });
      }
      groups[groupIndexByKey.get(groupKey)].items.push(item);
    });
    return groups;
  }

  function decoratePaletteItem(item, node) {
    item.className = "palette-item";
    item.draggable = true;
    item.dataset.nodeType = String(node?.type || "");
    item.dataset.paletteGroup = normalizeGroupLabel(node?.group);
    item.dataset.policySection = String(node?.policy_section || "").trim();
  }

  function resolveNodeCreateSpec(rawType) {
    const normalized = String(rawType || "").trim().toLowerCase();
    if (normalized === "ds_refine") {
      return {
        nodeType: "ai_refine",
        label: "DS提炼",
        config: {
          ...defaultNodeConfigFn("ai_refine"),
          reuse_existing: false,
          provider_name: "DeepSeek",
          ai_endpoint: String(els.aiEndpoint?.value || "").trim() || "https://api.deepseek.com/v1/chat/completions",
          ai_api_key: String(els.aiKey?.value || "").trim(),
          ai_model: String(els.aiModel?.value || "").trim() || "deepseek-chat",
        },
      };
    }
    return {
      nodeType: String(rawType || "").trim(),
      label: String(rawType || "").trim(),
      config: null,
    };
  }

  function createNodeFromSpec(spec, x, y, statusText) {
    let id = "";
    try {
      id = createNode(spec.nodeType, x, y, spec.config || undefined);
    } catch (error) {
      const message = String(error?.message || error || "");
      if (error?.code === "workflow_node_type_unregistered" || /unregistered node types/i.test(message)) {
        setStatus(`节点类型未注册: ${spec.nodeType}`, false);
        return;
      }
      throw error;
    }
    selectNodeIds([id]);
    renderAll();
    setStatus(statusText, true);
  }

  function setNodeTypeAuthoringState(entry, raw) {
    const normalized = String(raw || "").trim();
    const state = !normalized ? "empty" : (entry ? "registered" : "unknown");
    if (els.btnAdd) els.btnAdd.disabled = state !== "registered";
    if (!els.nodeType) return;
    if (els.nodeType.dataset) els.nodeType.dataset.policyState = state;
    els.nodeType.title = state === "registered"
      ? `registered node type: ${String(entry?.type || normalized)}`
      : (state === "unknown" ? `unregistered node type: ${normalized}` : "enter node type");
    if (typeof els.nodeType.setAttribute === "function") {
      els.nodeType.setAttribute("aria-invalid", state === "unknown" ? "true" : "false");
    } else {
      els.nodeType.ariaInvalid = state === "unknown" ? "true" : "false";
    }
  }

  function buildNodeTypePolicyRows(hint) {
    const ownership = describePolicyOwnership({
      policySource: hint.policySource,
      policySection: hint.policySection,
      group: hint.groupLabel,
    });
    return [
      { label: "节点", value: hint.label },
      { label: "分组", value: hint.groupLabel || "-" },
      { label: "策略段", value: hint.policySection || "-" },
      { label: "来源", value: hint.policySource || "-" },
      { label: "所有权", value: ownership.owner },
      { label: "Identity", value: ownership.identityRule },
      { label: "编辑边界", value: ownership.boundary },
    ];
  }

  function renderNodeTypePolicyHint() {
    const raw = String(els.nodeType?.value || "").trim();
    const entry = raw ? findNodeCatalogEntry(nodeCatalog, raw) : null;
    setNodeTypeAuthoringState(entry, raw);
    if (!els.nodeTypePolicyHint) return;
    if (!raw) {
      els.nodeTypePolicyHint.textContent = "节点类型策略: 待输入";
      els.nodeTypePolicyHint.innerHTML = '<div class="cfg-help">节点类型策略: 待输入</div>';
      return;
    }
    if (!entry) {
      els.nodeTypePolicyHint.textContent = `节点类型策略: 未注册 (${raw})`;
      els.nodeTypePolicyHint.innerHTML = `<div class="cfg-help" style="color:#b42318">节点类型策略: 未注册 (${raw})，已禁止添加</div>`;
      return;
    }
    const hint = formatNodePolicyHint(nodeCatalog, raw, raw);
    const rows = buildNodeTypePolicyRows(hint);
    els.nodeTypePolicyHint.textContent = `节点类型策略: ${rows.map((row) => `${row.label} ${row.value}`).join(" 路 ")}`;
    els.nodeTypePolicyHint.innerHTML = [
      '<div class="node-policy-card">',
      '<div class="cfg-help">节点类型策略</div>',
      ...rows.map((row) => (
        `<div class="node-policy-row"><strong>${row.label}</strong><span>${row.value}</span></div>`
      )),
      "</div>",
    ].join("");
  }

  function handleAddNode() {
    const raw = String(els.nodeType?.value || "").trim();
    if (!raw) {
      setStatus("节点类型不能为空", false);
      return;
    }
    const spec = resolveNodeCreateSpec(raw);
    const hint = formatNodePolicyHint(nodeCatalog, raw, spec.label || spec.nodeType);
    createNodeFromSpec(spec, 60, 60, `已添加节点: ${hint.statusLabel}`);
  }

  function renderPalette() {
    if (!els.palette) {
      renderNodeTypePolicyHint();
      return;
    }
    els.palette.innerHTML = "";
    const mode = String(els.paletteMode?.value || "simple").trim().toLowerCase();
    const kw = String(els.paletteSearch?.value || "").trim().toLowerCase();
    const list = nodeCatalog.filter((node) => {
      const normalizedType = String(node?.type || "").trim().toLowerCase();
      if (mode === "simple" && !simpleNodeTypes.has(normalizedType)) return false;
      if (!kw) return true;
      const haystack = `${String(node?.name || "")} ${String(node?.type || "")} ${String(node?.desc || "")}`.toLowerCase();
      return haystack.includes(kw);
    });
    buildPaletteGroups(list).forEach((group) => {
      const groupWrap = createElement("div");
      const groupTitle = createElement("div");
      const groupItems = createElement("div");
      groupWrap.className = "palette-group";
      groupWrap.dataset.paletteGroup = group.label;
      groupTitle.className = "palette-group-title";
      groupTitle.textContent = group.label;
      groupItems.className = "palette-group-items";
      groupWrap.appendChild(groupTitle);
      groupWrap.appendChild(groupItems);
      group.items.forEach((node) => {
        const item = createElement("div");
        const titleWrap = createElement("div");
        const title = createElement("strong");
        const type = createElement("div");
        const meta = createElement("div");
        const desc = createElement("div");
        decoratePaletteItem(item, node);
        title.textContent = String(node?.name || "");
        titleWrap.appendChild(title);
        type.style.fontSize = "12px";
        type.style.color = "#4f6378";
        type.textContent = String(node?.type || "");
        meta.style.fontSize = "11px";
        meta.style.color = "#7b8da0";
        meta.style.marginTop = "2px";
        const policySection = String(node?.policy_section || "").trim();
        const policySource = String(node?.policy_source || "").trim();
        meta.textContent = [policySection || normalizeGroupLabel(node?.group), policySource].filter(Boolean).join(" 路 ");
        desc.style.fontSize = "12px";
        desc.style.color = "#6b7f94";
        desc.style.marginTop = "3px";
        desc.textContent = String(node?.desc || "");
        item.appendChild(titleWrap);
        item.appendChild(type);
        item.appendChild(meta);
        item.appendChild(desc);
        groupItems.appendChild(item);
      });
      els.palette.appendChild(groupWrap);
    });
    Array.from(els.palette.querySelectorAll(".palette-item")).forEach((item) => {
      item.addEventListener("click", () => {
        if (els.nodeType) els.nodeType.value = String(item.dataset.nodeType || "ingest_files");
        const hint = formatNodePolicyHint(nodeCatalog, item.dataset.nodeType, item.dataset.nodeType);
        renderNodeTypePolicyHint();
        setStatus(`已选择节点: ${hint.statusLabel}`, true);
      });
      item.addEventListener("dragstart", (evt) => {
        const nodeType = String(item.dataset.nodeType || "");
        evt.dataTransfer.setData("text/plain", nodeType);
        evt.dataTransfer.effectAllowed = "copy";
      });
    });
    renderNodeTypePolicyHint();
  }

  function handleCanvasDragOver(evt) {
    evt.preventDefault();
    evt.dataTransfer.dropEffect = "copy";
  }

  function handleCanvasDrop(evt) {
    evt.preventDefault();
    const raw = String(evt.dataTransfer.getData("text/plain") || "").trim();
    if (!raw) return;
    const spec = resolveNodeCreateSpec(raw);
    const pos = computeDropPosition(evt);
    const hint = formatNodePolicyHint(nodeCatalog, raw, spec.label || spec.nodeType);
    createNodeFromSpec(spec, pos.x, pos.y, `已拖入节点: ${hint.statusLabel}`);
  }

  return {
    handleAddNode,
    renderPalette,
    renderNodeTypePolicyHint,
    handleCanvasDragOver,
    handleCanvasDrop,
  };
}

export { createWorkflowPaletteUi };
