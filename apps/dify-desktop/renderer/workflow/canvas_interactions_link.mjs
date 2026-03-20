import { storePointer } from './canvas_interactions_touch.mjs';

function onLinkStart(ctx, evt, fromId) {
  evt.preventDefault();
  evt.stopPropagation();
  ctx.lastClient = { x: evt.clientX, y: evt.clientY };
  if (Number.isFinite(evt.pointerId)) storePointer(ctx, evt);
  ctx.linking = {
    from: fromId,
    pointerId: Number.isFinite(evt.pointerId) ? evt.pointerId : null,
  };
  markLinkTargets(ctx);
  ctx.requestRender(false);
}

function linkErrorMessage(reason) {
  if (reason === "self") return "不能连接到自身节点";
  if (reason === "duplicate") return "连线已存在";
  if (reason === "cycle") return "不允许形成环路（仅支持 DAG）";
  return "连线失败";
}

function resolveLinkTarget(evt, documentLike = document) {
  const direct = evt.target && evt.target.closest ? evt.target.closest(".port.in") : null;
  let toId = direct ? String(direct.dataset.nodeId || "") : "";

  if (!toId) {
    const hit = documentLike.elementFromPoint(evt.clientX, evt.clientY);
    const hitPort = hit && hit.closest ? hit.closest(".port.in") : null;
    toId = hitPort ? String(hitPort.dataset.nodeId || "") : "";
  }
  return toId;
}

function finishLinkByEvent(ctx, evt, documentLike = document) {
  if (!ctx.linking) return;

  const toId = resolveLinkTarget(evt, documentLike);
  if (toId && toId !== ctx.linking.from) {
    if (ctx.store.hasEdge(ctx.linking.from, toId)) {
      ctx.store.unlink(ctx.linking.from, toId);
      ctx.onChange();
      ctx.onWarn(`已取消连线: ${ctx.linking.from} -> ${toId}`);
    } else {
      const res = ctx.store.linkToFrom(ctx.linking.from, toId);
      if (res && res.ok) ctx.onChange();
      else ctx.onWarn(linkErrorMessage(res?.reason));
    }
  }

  ctx.linking = null;
  clearLinkTargets(ctx);
  ctx.requestRender(false);
}

function markLinkTargets(ctx) {
  for (const [id, port] of ctx.inputPortByNodeId.entries()) {
    if (!ctx.linking || id === ctx.linking.from) continue;
    port.classList.add("target");
  }
}

function clearLinkTargets(ctx) {
  for (const port of ctx.inputPortByNodeId.values()) {
    port.classList.remove("target");
  }
}

export {
  clearLinkTargets,
  finishLinkByEvent,
  linkErrorMessage,
  markLinkTargets,
  onLinkStart,
  resolveLinkTarget,
};
