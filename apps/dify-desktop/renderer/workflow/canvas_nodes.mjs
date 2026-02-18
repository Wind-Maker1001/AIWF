function renderNodesLayer(ctx, nodeW, nodeH, createEl) {
  ctx.nodesLayer.innerHTML = "";
  ctx.inputPortByNodeId.clear();
  ctx.portByNodeId.clear();
  ctx.nodeElById.clear();

  for (const node of ctx.store.state.graph.nodes) {
    const card = createEl("div", "node");
    const p = ctx.worldToDisplay(Number(node.x || 0), Number(node.y || 0));
    card.style.left = `${p.x}px`;
    card.style.top = `${p.y}px`;
    card.style.transform = `scale(${ctx.zoom})`;
    card.style.transformOrigin = "top left";
    card.dataset.id = node.id;
    if (ctx.isSelected(node.id)) card.classList.add("selected");

    const hd = createEl("div", "node-hd");
    const ttl = createEl("strong", "", `${ctx.catalogName(node.type)} (${node.id})`);
    const actions = createEl("div", "node-actions");
    const delBtn = createEl("button", "mini del", "删除");
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
    delBtn.addEventListener("mousedown", (evt) => {
      // Prevent header drag handler from hijacking delete interaction.
      evt.stopPropagation();
    });

    card.addEventListener("mousedown", (evt) => {
      if (evt.button !== 0) return;
      if (evt.ctrlKey || evt.metaKey || evt.shiftKey) ctx.toggleSelection(node.id);
      else if (!ctx.isSelected(node.id)) ctx.selectOne(node.id);
      ctx.requestRender(false);
    });

    outPort.addEventListener("mousedown", (evt) => ctx.onLinkStart(evt, node.id));
    hd.addEventListener("mousedown", (evt) => {
      if (evt.target && evt.target.closest && evt.target.closest(".node-actions")) return;
      if (evt.ctrlKey || evt.metaKey || evt.shiftKey) {
        if (evt.button !== 0) return;
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
