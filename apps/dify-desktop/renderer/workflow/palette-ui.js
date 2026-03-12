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

  const simpleNodeTypes = new Set([
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
  ]);

  function resolveNodeCreateSpec(rawType) {
    const t = String(rawType || "").trim().toLowerCase();
    if (t === "ds_refine") {
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
    return { nodeType: rawType, label: String(rawType || ""), config: null };
  }

  function createNodeFromSpec(spec, x, y, statusText) {
    const id = createNode(spec.nodeType, x, y, spec.config || undefined);
    selectNodeIds([id]);
    renderAll();
    setStatus(statusText, true);
  }

  function handleAddNode() {
    const raw = String(els.nodeType?.value || "").trim();
    if (!raw) {
      setStatus("节点类型不能为空", false);
      return;
    }
    const spec = resolveNodeCreateSpec(raw);
    createNodeFromSpec(spec, 60, 60, `已添加节点: ${spec.label || spec.nodeType}`);
  }

  function renderPalette() {
    if (!els.palette) return;
    els.palette.innerHTML = "";
    const mode = String(els.paletteMode?.value || "simple").trim().toLowerCase();
    const kw = String(els.paletteSearch?.value || "").trim().toLowerCase();
    const list = nodeCatalog.filter((n) => {
      const t = String(n?.type || "").trim().toLowerCase();
      if (mode === "simple" && !simpleNodeTypes.has(t)) return false;
      if (!kw) return true;
      const hay = `${String(n?.name || "")} ${String(n?.type || "")} ${String(n?.desc || "")}`.toLowerCase();
      return hay.includes(kw);
    });
    list.forEach((n) => {
      const item = createElement("div");
      const titleWrap = createElement("div");
      const title = createElement("strong");
      const type = createElement("div");
      const desc = createElement("div");
      item.className = "palette-item";
      item.draggable = true;
      item.dataset.nodeType = String(n?.type || "");
      title.textContent = String(n?.name || "");
      titleWrap.appendChild(title);
      type.style.fontSize = "12px";
      type.style.color = "#4f6378";
      type.textContent = String(n?.type || "");
      desc.style.fontSize = "12px";
      desc.style.color = "#6b7f94";
      desc.style.marginTop = "3px";
      desc.textContent = String(n?.desc || "");
      item.appendChild(titleWrap);
      item.appendChild(type);
      item.appendChild(desc);
      els.palette.appendChild(item);
    });
    Array.from(els.palette.querySelectorAll(".palette-item")).forEach((item) => {
      item.addEventListener("click", () => {
        if (els.nodeType) els.nodeType.value = String(item.dataset.nodeType || "ingest_files");
      });
      item.addEventListener("dragstart", (evt) => {
        const t = String(item.dataset.nodeType || "");
        evt.dataTransfer.setData("text/plain", t);
        evt.dataTransfer.effectAllowed = "copy";
      });
    });
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
    createNodeFromSpec(spec, pos.x, pos.y, `已拖入节点: ${spec.label || spec.nodeType}`);
  }

  return {
    handleAddNode,
    renderPalette,
    handleCanvasDragOver,
    handleCanvasDrop,
  };
}

export { createWorkflowPaletteUi };
