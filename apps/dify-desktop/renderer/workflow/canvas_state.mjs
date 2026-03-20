function catalogName(canvas, type) {
  const item = canvas.nodeCatalog.find((row) => row.type === type);
  return item ? item.name : type;
}

function clearSelection(canvas) {
  const changed = canvas.selectedIds.size > 0;
  canvas.selectedIds.clear();
  if (changed) canvas.onSelectionChange(canvas.getSelectedIds());
}

function selectOne(canvas, id) {
  const previous = canvas.getSelectedIds().join("|");
  canvas.selectedIds.clear();
  if (id) canvas.selectedIds.add(id);
  if (previous !== canvas.getSelectedIds().join("|")) canvas.onSelectionChange(canvas.getSelectedIds());
}

function toggleSelection(canvas, id) {
  if (!id) return;
  const previous = canvas.getSelectedIds().join("|");
  if (canvas.selectedIds.has(id)) canvas.selectedIds.delete(id);
  else canvas.selectedIds.add(id);
  if (previous !== canvas.getSelectedIds().join("|")) canvas.onSelectionChange(canvas.getSelectedIds());
}

function isSelected(canvas, id) {
  return canvas.selectedIds.has(id);
}

function getSelectedIds(canvas) {
  return Array.from(canvas.selectedIds);
}

function setSelectedIds(canvas, ids = []) {
  const previous = canvas.getSelectedIds().join("|");
  canvas.selectedIds.clear();
  for (const id of ids || []) {
    const nextId = String(id || "");
    if (nextId) canvas.selectedIds.add(nextId);
  }
  if (previous !== canvas.getSelectedIds().join("|")) canvas.onSelectionChange(canvas.getSelectedIds());
}

function setArrangePolicy(canvas, policy = {}) {
  canvas.arrangePolicy = {
    ...canvas.arrangePolicy,
    ...(policy || {}),
  };
}

export {
  catalogName,
  clearSelection,
  getSelectedIds,
  isSelected,
  selectOne,
  setArrangePolicy,
  setSelectedIds,
  toggleSelection,
};
