import {
  beginTouchPan,
  beginTouchPinch,
  dropPointer,
  isPrimaryPointer,
  isTouchLike,
  jumpMinimap,
  maybeRefreshTouchGesture,
  snap,
  storePointer,
  updateTouchPan,
  updateTouchPinch,
} from './canvas_interactions_touch.mjs';
import {
  clearLinkTargets,
  finishLinkByEvent,
  linkErrorMessage,
  markLinkTargets,
  onLinkStart,
} from './canvas_interactions_link.mjs';
import {
  onDragEnd,
  onDragMove,
  onDragStart,
  onMarqueeEnd,
  onMarqueeMove,
  renderGuides,
  renderMarquee,
} from './canvas_interactions_drag.mjs';
import {
  bindMinimapEvents,
  renderMinimap,
} from './canvas_interactions_minimap.mjs';

function bindCanvasEvents(ctx) {
  window.addEventListener("pointermove", (evt) => {
    ctx.lastClient = { x: evt.clientX, y: evt.clientY };
    storePointer(ctx, evt);

    if (ctx.touchPinch && updateTouchPinch(ctx)) return;
    if (ctx.touchPan && updateTouchPan(ctx, evt)) return;
    if (ctx.drag && (!Number.isFinite(ctx.drag.pointerId) || ctx.drag.pointerId === evt.pointerId)) {
      ctx.onDragMove(evt);
      return;
    }
    if (ctx.marquee && (!Number.isFinite(ctx.marquee.pointerId) || ctx.marquee.pointerId === evt.pointerId)) {
      ctx.onMarqueeMove(evt);
      return;
    }
    if (ctx.linking && (!Number.isFinite(ctx.linking.pointerId) || ctx.linking.pointerId === evt.pointerId)) {
      ctx.requestRender(false);
    }
  });

  const finishPointer = (evt) => {
    if (ctx.drag && (!Number.isFinite(ctx.drag.pointerId) || ctx.drag.pointerId === evt.pointerId)) ctx.onDragEnd();
    if (ctx.marquee && (!Number.isFinite(ctx.marquee.pointerId) || ctx.marquee.pointerId === evt.pointerId)) ctx.onMarqueeEnd(evt);
    if (ctx.linking && (!Number.isFinite(ctx.linking.pointerId) || ctx.linking.pointerId === evt.pointerId)) ctx.finishLinkByEvent(evt);
    if (ctx.touchPan && ctx.touchPan.pointerId === evt.pointerId) ctx.touchPan = null;
    if (ctx.touchPinch && Array.isArray(ctx.touchPinch.pointerIds) && ctx.touchPinch.pointerIds.includes(evt.pointerId)) ctx.touchPinch = null;
    dropPointer(ctx, evt);
    maybeRefreshTouchGesture(ctx);
  };

  window.addEventListener("pointerup", finishPointer);
  window.addEventListener("pointercancel", finishPointer);

  ctx.canvasWrap.addEventListener("scroll", () => {
    if (ctx.linking) ctx.requestRender(false);
    ctx.requestMinimap();
  });

  ctx.canvasSurface.addEventListener("pointerdown", (evt) => {
    ctx.lastClient = { x: evt.clientX, y: evt.clientY };
    storePointer(ctx, evt);

    const target = evt.target;
    if (target.closest && (target.closest(".node") || target.closest(".port") || target.closest(".edge-hit") || target.closest(".minimap-wrap"))) {
      maybeRefreshTouchGesture(ctx);
      return;
    }

    if (ctx.onEdgeSelect) ctx.onEdgeSelect(null);
    if (isTouchLike(evt)) {
      ctx.clearSelection();
      beginTouchPan(ctx, {
        pointerId: evt.pointerId,
        clientX: evt.clientX,
        clientY: evt.clientY,
      });
      ctx.requestRender(false);
      return;
    }

    if (!isPrimaryPointer(evt)) return;
    const s = ctx.clientToSurface(evt.clientX, evt.clientY);
    ctx.marquee = { x0: s.x, y0: s.y, x1: s.x, y1: s.y, pointerId: evt.pointerId };
    ctx.clearSelection();
    ctx.requestRender(false);
  });
}

export {
  bindCanvasEvents,
  bindMinimapEvents,
  renderGuides,
  renderMarquee,
  onMarqueeMove,
  onMarqueeEnd,
  renderMinimap,
  onDragStart,
  onDragMove,
  onDragEnd,
  onLinkStart,
  linkErrorMessage,
  finishLinkByEvent,
  markLinkTargets,
  clearLinkTargets,
};
