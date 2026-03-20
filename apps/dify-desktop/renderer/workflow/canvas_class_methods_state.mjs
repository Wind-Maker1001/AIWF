import {
  catalogName as stateCatalogName,
  clearSelection as stateClearSelection,
  getSelectedIds as stateGetSelectedIds,
  isSelected as stateIsSelected,
  selectOne as stateSelectOne,
  setArrangePolicy as stateSetArrangePolicy,
  setSelectedIds as stateSetSelectedIds,
  toggleSelection as stateToggleSelection,
} from './canvas_state.mjs';
import { defineMethod } from './canvas_class_methods_support.mjs';

function installWorkflowCanvasStateMethods(WorkflowCanvas) {
  const prototype = WorkflowCanvas.prototype;

  defineMethod(prototype, 'catalogName', function catalogName(type) {
    return stateCatalogName(this, type);
  });

  defineMethod(prototype, 'clearSelection', function clearSelection() {
    stateClearSelection(this);
  });

  defineMethod(prototype, 'selectOne', function selectOne(id) {
    stateSelectOne(this, id);
  });

  defineMethod(prototype, 'toggleSelection', function toggleSelection(id) {
    stateToggleSelection(this, id);
  });

  defineMethod(prototype, 'isSelected', function isSelected(id) {
    return stateIsSelected(this, id);
  });

  defineMethod(prototype, 'getSelectedIds', function getSelectedIds() {
    return stateGetSelectedIds(this);
  });

  defineMethod(prototype, 'setSelectedIds', function setSelectedIds(ids = []) {
    stateSetSelectedIds(this, ids);
  });

  defineMethod(prototype, 'setArrangePolicy', function setArrangePolicy(policy = {}) {
    stateSetArrangePolicy(this, policy);
  });
}

export { installWorkflowCanvasStateMethods };
