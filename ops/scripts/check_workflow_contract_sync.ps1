param(
  [string]$RepoRoot = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }

if (-not $RepoRoot) {
  $RepoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
}
if (-not (Get-Command node -ErrorAction SilentlyContinue)) {
  throw "node not found in PATH"
}

$nodeScript = @'
const fs = require("fs");
const path = require("path");
const { pathToFileURL } = require("url");

function fail(message) {
  console.error(message);
  process.exit(1);
}

(async () => {
  const repoRoot = process.argv[2];
  const schemaPath = path.join(repoRoot, "contracts", "workflow", "workflow.schema.json");
  const defaultsPath = path.join(repoRoot, "apps", "dify-desktop", "renderer", "workflow", "defaults.js");
  const storeSupportPath = path.join(repoRoot, "apps", "dify-desktop", "renderer", "workflow", "store-support.js");
  const runPayloadPath = path.join(repoRoot, "apps", "dify-desktop", "renderer", "workflow", "run-payload-support.js");
  const workflowGraphPath = path.join(repoRoot, "apps", "dify-desktop", "workflow_graph.js");
  const paletteUiPath = path.join(repoRoot, "apps", "dify-desktop", "renderer", "workflow", "palette-ui.js");
  const preflightUiPath = path.join(repoRoot, "apps", "dify-desktop", "renderer", "workflow", "preflight-ui.js");

  const schema = JSON.parse(fs.readFileSync(schemaPath, "utf8"));
  const required = Array.isArray(schema.required) ? schema.required : [];
  for (const key of ["workflow_id", "version", "nodes", "edges"]) {
    if (!required.includes(key)) fail(`workflow schema missing required top-level field: ${key}`);
  }

  const defaults = await import(pathToFileURL(defaultsPath).href);
  const storeSupport = await import(pathToFileURL(storeSupportPath).href);
  const runPayloadSupport = await import(pathToFileURL(runPayloadPath).href);
  const paletteUi = await import(pathToFileURL(paletteUiPath).href);
  const preflightUi = await import(pathToFileURL(preflightUiPath).href);
  const workflowGraph = require(workflowGraphPath);

  const defaultGraph = defaults.defaultWorkflowGraph();
  const missingDefault = required.filter((field) => !(field in defaultGraph));
  if (missingDefault.length > 0) {
    fail(`desktop default workflow graph missing fields: ${missingDefault.join(", ")}`);
  }
  if (!String(defaultGraph.version || "").trim()) {
    fail("desktop default workflow graph has empty version");
  }

  const imported = storeSupport.normalizeImportedGraphWithContract({
    workflow_id: "wf_import_gate",
    nodes: [
      { id: "n1", type: "ingest_files" },
      { id: "n2", type: "md_output" },
    ],
    edges: [{ from: "n1", to: "n2" }],
  });
  if (!String(imported?.graph?.version || "").trim()) {
    fail("normalizeImportedGraphWithContract did not preserve or fill workflow.version");
  }
  if (!imported?.contract?.migrated) {
    fail("normalizeImportedGraphWithContract did not record missing-version migration");
  }

  let importRejectedUnknownType = false;
  try {
    storeSupport.normalizeImportedGraphWithContract({
      workflow_id: "wf_import_unknown_gate",
      version: "1.0.0",
      nodes: [{ id: "n1", type: "unknown_future_node" }],
      edges: [],
    });
  } catch (error) {
    importRejectedUnknownType = /unregistered node types in import/i.test(String(error?.message || error));
  }
  if (!importRejectedUnknownType) {
    fail("normalizeImportedGraphWithContract did not reject unregistered node types");
  }

  const payload = runPayloadSupport.buildBaseRunPayload({}, defaultGraph, 42);
  if (payload.workflow_id !== defaultGraph.workflow_id) {
    fail("run payload workflow_id drifted from graph.workflow_id");
  }
  if (payload.workflow_version !== defaultGraph.version) {
    fail("run payload workflow_version drifted from graph.version");
  }
  if (payload?.workflow?.version !== defaultGraph.version) {
    fail("run payload nested workflow.version drifted from graph.version");
  }

  let payloadRejectedUnknownType = false;
  try {
    runPayloadSupport.buildBaseRunPayload({}, {
      workflow_id: "wf_payload_unknown_gate",
      version: "1.0.0",
      nodes: [{ id: "n1", type: "unknown_future_node" }],
      edges: [],
    }, 42);
  } catch (error) {
    payloadRejectedUnknownType = /unregistered node types in run_payload/i.test(String(error?.message || error));
  }
  if (!payloadRejectedUnknownType) {
    fail("buildBaseRunPayload did not reject unregistered node types");
  }

  const nodeType = {
    value: "unknown_future_node",
    dataset: {},
    title: "",
    ariaInvalid: "",
    setAttribute(name, value) {
      if (name === "aria-invalid") this.ariaInvalid = value;
      else this[name] = value;
    },
  };
  const nodeTypePolicyHint = { textContent: "", innerHTML: "" };
  const btnAdd = { disabled: false };
  const palette = paletteUi.createWorkflowPaletteUi({
    nodeType,
    nodeTypePolicyHint,
    btnAdd,
  }, {
    nodeCatalog: [
      { type: "ai_refine", name: "AI 提炼", group: "AI 编排", policy_section: "local_ai", policy_source: "local_policy" },
    ],
  });
  palette.renderNodeTypePolicyHint();
  const authoringRejectedUnknownType = btnAdd.disabled === true
    && nodeType.dataset.policyState === "unknown"
    && nodeType.ariaInvalid === "true"
    && String(nodeTypePolicyHint.textContent || "").includes("unknown_future_node");
  if (!authoringRejectedUnknownType) {
    fail("palette authoring surface did not gate unknown node types");
  }

  function createElement(tag) {
    return {
      tag,
      className: "",
      textContent: "",
      innerHTML: "",
      style: {},
      children: [],
      append(...nodes) {
        this.children.push(...nodes);
      },
      appendChild(node) {
        this.children.push(node);
      },
    };
  }

  const preflightRows = {
    innerHTML: "",
    children: [],
    appendChild(node) {
      this.children.push(node);
    },
  };
  const preflight = preflightUi.createWorkflowPreflightUi({
    preflightSummary: { textContent: "", style: {} },
    preflightRisk: { textContent: "", style: {} },
    preflightRows,
  }, {
    createElement,
  });
  preflight.renderPreflightReport({
    ok: false,
    issues: [
      {
        level: "error",
        kind: "unknown_node_type",
        node_id: "n1",
        message: "workflow contains unregistered node types: unknown_future_node",
      },
    ],
  });
  const firstRow = Array.isArray(preflightRows.children) ? preflightRows.children[0] : null;
  const preflightUnknownTypeGuided = !!firstRow
    && Array.isArray(firstRow.children)
    && firstRow.children.length === 4
    && String(firstRow.children[1]?.textContent || "").includes("unknown_node_type")
    && String(firstRow.children[2]?.textContent || "").includes("Rust manifest / local node policy")
    && Array.isArray(firstRow.children[3]?.children)
    && firstRow.children[3].children.length === 1;
  if (!preflightUnknownTypeGuided) {
    fail("preflight ui did not render explicit unknown node type guidance");
  }

  const normalized = workflowGraph.normalizeWorkflow({
    workflow: {
      workflow_id: "wf_main_gate",
      nodes: [{ id: "n1", type: "ingest_files" }],
      edges: [],
    },
  });
  if (!String(normalized?.graph?.version || "").trim()) {
    fail("main workflow normalization did not fill workflow.version");
  }
  if (!normalized?.contract?.migrated) {
    fail("main workflow normalization did not record missing-version migration");
  }

  const invalid = workflowGraph.validateGraph({
    workflow_id: "wf_invalid_gate",
    nodes: [{ id: "n1", type: "ingest_files" }],
    edges: [],
  });
  if (invalid.ok) {
    fail("workflow graph validation accepted a graph without top-level version");
  }

  console.log(JSON.stringify({
    required,
    defaultVersion: defaultGraph.version,
    importMigrated: imported.contract.migrated,
    importRejectedUnknownType,
    payloadRejectedUnknownType,
    authoringRejectedUnknownType,
    preflightUnknownTypeGuided,
    normalizedVersion: normalized.graph.version,
  }));
})().catch((error) => {
  console.error(error && error.stack ? error.stack : String(error));
  process.exit(1);
});
'@

$nodeScript | node - $RepoRoot
if ($LASTEXITCODE -ne 0) {
  throw "workflow contract sync checks failed"
}

Ok "workflow contract sync check passed"
