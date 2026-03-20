function cfgNode(node) {
  return node?.config && typeof node.config === "object" ? { ...node.config } : {};
}

function nextMapKey(map, prefix) {
  let index = 1;
  while (Object.prototype.hasOwnProperty.call(map, `${prefix}_${index}`)) index += 1;
  return `${prefix}_${index}`;
}

function createUpdatedNodeMapConfig(node, kind) {
  const cfg = cfgNode(node);
  const isInput = kind === "input";
  const mapKey = isInput ? "input_map" : "output_map";
  const prefix = isInput ? "target" : "alias";
  const defaultValue = isInput ? "$prev.ok" : "ok";
  const map = cfg[mapKey] && typeof cfg[mapKey] === "object" && !Array.isArray(cfg[mapKey])
    ? { ...cfg[mapKey] }
    : {};
  map[nextMapKey(map, prefix)] = defaultValue;
  cfg[mapKey] = map;
  return cfg;
}

function unlinkSelectedGraphEdges(edges = [], selectedIds = []) {
  const selected = new Set(Array.isArray(selectedIds) ? selectedIds : []);
  const sourceEdges = Array.isArray(edges) ? edges : [];
  const nextEdges = sourceEdges.filter((edge) => !(selected.has(edge.from) && selected.has(edge.to)));
  return {
    edges: nextEdges,
    removed: sourceEdges.length - nextEdges.length,
  };
}

export {
  cfgNode,
  createUpdatedNodeMapConfig,
  nextMapKey,
  unlinkSelectedGraphEdges,
};
