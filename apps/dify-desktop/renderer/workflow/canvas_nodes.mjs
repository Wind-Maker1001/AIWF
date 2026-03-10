function isPrimaryPointer(evt) {
  const pointerType = String(evt?.pointerType || "");
  if (pointerType === "mouse") return evt.button === 0;
  return evt.isPrimary !== false;
}

function renderNodesLayer(ctx, nodeW, nodeH, createEl) {
  ctx.nodesLayer.innerHTML = "";
  ctx.inputPortByNodeId.clear();
  ctx.portByNodeId.clear();
  ctx.nodeElById.clear();
  ctx.visibleNodeIds.clear();
  const nodeCount = Array.isArray(ctx.store?.state?.graph?.nodes) ? ctx.store.state.graph.nodes.length : 0;
  const enableCull = nodeCount > 200;
  const vr = enableCull && ctx.getVisibleDisplayRect ? ctx.getVisibleDisplayRect(ctx.cullMargin || 0) : null;

  for (const node of ctx.store.state.graph.nodes) {
    const p = ctx.worldToDisplay(Number(node.x || 0), Number(node.y || 0));
    const w = nodeW * ctx.zoom;
    const h = nodeH * ctx.zoom;
    if (vr) {
      const hidden = (p.x + w < vr.x) || (p.y + h < vr.y) || (p.x > vr.x + vr.w) || (p.y > vr.y + vr.h);
      if (hidden) continue;
    }
    ctx.visibleNodeIds.add(String(node.id || ""));

    const card = createEl("div", "node");
    card.style.left = `${p.x}px`;
    card.style.top = `${p.y}px`;
    card.style.transform = `scale(${ctx.zoom})`;
    card.style.transformOrigin = "top left";
    card.dataset.id = node.id;
    if (ctx.isSelected(node.id)) card.classList.add("selected");

    const hd = createEl("div", "node-hd");
    const ttl = createEl("strong", "", `${ctx.catalogName(node.type)} (${node.id})`);
    const actions = createEl("div", "node-actions");
    const delBtn = createEl("button", "mini-btn del", "删除");
    actions.append(delBtn);
    hd.append(ttl, actions);

    const bd = createEl("div", "node-bd");
    bd.textContent = node.type;

    const inPort = createEl("div", "port in");
    inPort.dataset.nodeId = node.id;
    inPort.title = "输入端口";

    const outPort = createEl("div", "port out");
    outPort.dataset.nodeId = node.id;
    outPort.title = "输出端口（拖到目标输入端口）";

    card.append(hd, bd, inPort, outPort);
    ctx.nodesLayer.append(card);
    ctx.inputPortByNodeId.set(node.id, inPort);
    ctx.portByNodeId.set(node.id, { inPort, outPort });
    ctx.nodeElById.set(node.id, card);

    delBtn.addEventListener("click", (evt) => {
      evt.stopPropagation();
      ctx.invalidateEdgesForNodes([node.id]);
      ctx.store.removeNode(node.id);
      ctx.selectedIds.delete(node.id);
      ctx.onChange();
    });
    delBtn.addEventListener("pointerdown", (evt) => {
      evt.stopPropagation();
    });

    card.addEventListener("pointerdown", (evt) => {
      if (!isPrimaryPointer(evt)) return;
      if (evt.ctrlKey || evt.metaKey || evt.shiftKey) ctx.toggleSelection(node.id);
      else if (!ctx.isSelected(node.id)) ctx.selectOne(node.id);
      ctx.requestRender(false);
    });

    outPort.addEventListener("pointerdown", (evt) => {
      if (!isPrimaryPointer(evt)) return;
      ctx.onLinkStart(evt, node.id);
    });

    hd.addEventListener("pointerdown", (evt) => {
      if (!isPrimaryPointer(evt)) return;
      if (evt.target && evt.target.closest && evt.target.closest(".node-actions")) return;
      if (evt.ctrlKey || evt.metaKey || evt.shiftKey) {
        evt.preventDefault();
        evt.stopPropagation();
        ctx.toggleSelection(node.id);
        ctx.requestRender(false);
        return;
      }
      ctx.onDragStart(evt, node.id);
    });
  }
}

function updateNodePositionsFastLayer(ctx, ids = []) {
  for (const id of ids || []) {
    const n = ctx.store.state.graph.nodes.find((x) => x.id === id);
    const card = ctx.nodeElById.get(id);
    if (!n || !card) continue;
    const p = ctx.worldToDisplay(Number(n.x || 0), Number(n.y || 0));
    card.style.left = `${p.x}px`;
    card.style.top = `${p.y}px`;
    card.style.transform = `scale(${ctx.zoom})`;
    card.style.transformOrigin = "top left";
    if (ctx.isSelected(id)) card.classList.add("selected");
    else card.classList.remove("selected");
  }
}

export {
  renderNodesLayer,
  updateNodePositionsFastLayer,
};
