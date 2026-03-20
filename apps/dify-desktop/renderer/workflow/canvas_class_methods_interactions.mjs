import {
  onDragStart as onDragStartImpl,
  onDragMove as onDragMoveImpl,
  onDragEnd as onDragEndImpl,
  onLinkStart as onLinkStartImpl,
  linkErrorMessage as linkErrorMessageImpl,
  finishLinkByEvent as finishLinkByEventImpl,
  markLinkTargets as markLinkTargetsImpl,
  clearLinkTargets as clearLinkTargetsImpl,
} from './canvas_interactions.mjs';
import {
  clearLinkTargets as wrapClearLinkTargets,
  finishLinkByEvent as wrapFinishLinkByEvent,
  linkErrorMessage as wrapLinkErrorMessage,
  markLinkTargets as wrapMarkLinkTargets,
  onDragEnd as wrapOnDragEnd,
  onDragMove as wrapOnDragMove,
  onDragStart as wrapOnDragStart,
  onLinkStart as wrapOnLinkStart,
} from './canvas_wrappers.mjs';
import { defineMethod } from './canvas_class_methods_support.mjs';

function installWorkflowCanvasInteractionMethods(WorkflowCanvas) {
  const prototype = WorkflowCanvas.prototype;

  defineMethod(prototype, 'onDragStart', function onDragStart(evt, nodeId) {
    wrapOnDragStart(this, evt, nodeId, onDragStartImpl);
  });

  defineMethod(prototype, 'onDragMove', function onDragMove(evt) {
    wrapOnDragMove(this, evt, onDragMoveImpl);
  });

  defineMethod(prototype, 'onDragEnd', function onDragEnd() {
    wrapOnDragEnd(this, onDragEndImpl);
  });

  defineMethod(prototype, 'onLinkStart', function onLinkStart(evt, fromId) {
    wrapOnLinkStart(this, evt, fromId, onLinkStartImpl);
  });

  defineMethod(prototype, 'linkErrorMessage', function linkErrorMessage(reason) {
    return wrapLinkErrorMessage(reason, linkErrorMessageImpl);
  });

  defineMethod(prototype, 'finishLinkByEvent', function finishLinkByEvent(evt) {
    wrapFinishLinkByEvent(this, evt, finishLinkByEventImpl);
  });

  defineMethod(prototype, 'markLinkTargets', function markLinkTargets() {
    wrapMarkLinkTargets(this, markLinkTargetsImpl);
  });

  defineMethod(prototype, 'clearLinkTargets', function clearLinkTargets() {
    wrapClearLinkTargets(this, clearLinkTargetsImpl);
  });
}

export { installWorkflowCanvasInteractionMethods };
