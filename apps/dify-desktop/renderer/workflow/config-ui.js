import { defaultNodeConfig } from "./defaults.js";
import { createWorkflowConfigEdgeSupport } from "./config-ui-edge.js";
import { createWorkflowConfigMapSupport } from "./config-ui-maps.js";
import {
  buildNodeConfigFormSchema,
  parseFieldValue,
  parseNodeConfigFormElement,
  parseNodeConfigTextValue,
  prettyJson,
  renderNodeConfigFormInto,
  toFieldDisplayValue,
} from "./config-ui-node-form.js";
import { createWorkflowConfigNodeEditorSupport } from "./config-ui-node-editor.js";

function createWorkflowConfigUi(els, deps = {}) {
  const {
    store,
    canvas,
    nodeCatalog = [],
    nodeFormSchemas = {},
    edgeHintsByNodeType = {},
    setStatus = () => {},
    renderAll = () => {},
    refreshOfflineBoundaryHint = () => {},
    getSelectedEdge = () => null,
    setSelectedEdge = () => {},
    getCfgViewMode = () => "form",
    setCfgViewMode = () => {},
  } = deps;

  function singleSelectedNode() {
    const ids = canvas.getSelectedIds();
    if (!Array.isArray(ids) || ids.length !== 1) return null;
    return store.getNode(ids[0]);
  }

  function parseNodeConfigText() {
    return parseNodeConfigTextValue(els.nodeConfig?.value || "");
  }

  const edgeSupport = createWorkflowConfigEdgeSupport(els, {
    store,
    edgeHintsByNodeType,
    getSelectedEdge,
    setSelectedEdge,
  });

  function formSchemaForNodeType(type) {
    return nodeFormSchemas[String(type || "")] || [];
  }

  function fullNodeSchemaForType(type) {
    return buildNodeConfigFormSchema(type, nodeFormSchemas);
  }

  function setCfgMode(mode) {
    const nextMode = mode === "json" ? "json" : "form";
    setCfgViewMode(nextMode);
    const isJson = getCfgViewMode() === "json";
    els.nodeConfig.style.display = isJson ? "block" : "none";
    els.nodeConfigForm.style.display = isJson ? "none" : "block";
    els.btnCfgJson.className = `btn ${isJson ? "" : "secondary"}`.trim();
    els.btnCfgForm.className = `btn ${isJson ? "secondary" : ""}`.trim();
  }

  function renderNodeConfigForm(node, cfg) {
    renderNodeConfigFormInto(els.nodeConfigForm, node, cfg, fullNodeSchemaForType(node?.type));
  }

  function parseNodeConfigForm(node, baseCfg = {}) {
    return parseNodeConfigFormElement(
      els.nodeConfigForm,
      baseCfg,
      fullNodeSchemaForType(node?.type),
      parseFieldValue
    );
  }

  const mapSupport = createWorkflowConfigMapSupport(els, {
    store,
    singleSelectedNode,
    prettyJson,
  });

  const nodeEditorSupport = createWorkflowConfigNodeEditorSupport(els, {
    canvas,
    nodeCatalog,
    singleSelectedNode,
    prettyJson,
    renderNodeConfigForm,
    renderIoMapEditor: mapSupport.renderIoMapEditor,
    defaultNodeConfig,
  });

  return {
    singleSelectedNode,
    prettyJson,
    parseNodeConfigText,
    parseEdgeWhenText: edgeSupport.parseEdgeWhenText,
    parseLooseJsonValue: edgeSupport.parseLooseJsonValue,
    setEdgeWhenBuilderVisibility: edgeSupport.setEdgeWhenBuilderVisibility,
    edgeWhenFromBuilder: edgeSupport.edgeWhenFromBuilder,
    applyEdgeWhenToBuilder: edgeSupport.applyEdgeWhenToBuilder,
    syncEdgeTextFromBuilder: edgeSupport.syncEdgeTextFromBuilder,
    rebuildEdgeHints: edgeSupport.rebuildEdgeHints,
    renderEdgeConfigEditor: edgeSupport.renderEdgeConfigEditor,
    formSchemaForNodeType,
    setCfgMode,
    toFieldDisplayValue,
    parseFieldValue,
    renderNodeConfigForm,
    parseNodeConfigForm,
    readMapTableRows: mapSupport.readMapTableRows,
    renderMapTableRows: mapSupport.renderMapTableRows,
    syncIoMapFromTables: mapSupport.syncIoMapFromTables,
    renderIoMapEditor: mapSupport.renderIoMapEditor,
    renderNodeConfigEditor: nodeEditorSupport.renderNodeConfigEditor,
  };
}

export { createWorkflowConfigUi };
