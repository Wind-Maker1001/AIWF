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
  const runPayloadSupportPath = path.join(repoRoot, "apps", "dify-desktop", "renderer", "workflow", "run-payload-support.js");
  const paletteUiPath = path.join(repoRoot, "apps", "dify-desktop", "renderer", "workflow", "palette-ui.js");
  const preflightUiPath = path.join(repoRoot, "apps", "dify-desktop", "renderer", "workflow", "preflight-ui.js");
  const validationServicePath = path.join(repoRoot, "apps", "dify-desktop", "workflow_validation_service.js");
  const workflowIpcStorePath = path.join(repoRoot, "apps", "dify-desktop", "workflow_ipc_store.js");
  const workflowIpcRunPath = path.join(repoRoot, "apps", "dify-desktop", "workflow_ipc_run.js");
  const workflowIpcQueueAppsPath = path.join(repoRoot, "apps", "dify-desktop", "workflow_ipc_queue_apps.js");
  const workflowEnginePath = path.join(repoRoot, "apps", "dify-desktop", "workflow_engine.js");
  const preflightControllerUiPath = path.join(repoRoot, "apps", "dify-desktop", "renderer", "workflow", "preflight-controller-ui.js");
  const flowIoUiPath = path.join(repoRoot, "apps", "dify-desktop", "renderer", "workflow", "flow-io-ui.js");
  const runPayloadUiPath = path.join(repoRoot, "apps", "dify-desktop", "renderer", "workflow", "run-payload-ui.js");

  const schema = JSON.parse(fs.readFileSync(schemaPath, "utf8"));
  const required = Array.isArray(schema.required) ? schema.required : [];
  for (const key of ["workflow_id", "version", "nodes", "edges"]) {
    if (!required.includes(key)) fail(`workflow schema missing required top-level field: ${key}`);
  }

  const defaults = await import(pathToFileURL(defaultsPath).href);
  const storeSupport = await import(pathToFileURL(storeSupportPath).href);
  const runPayloadSupport = await import(pathToFileURL(runPayloadSupportPath).href);
  const paletteUi = await import(pathToFileURL(paletteUiPath).href);
  const preflightUi = await import(pathToFileURL(preflightUiPath).href);
  const { createWorkflowValidationSupport } = require(validationServicePath);

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

  const payload = runPayloadSupport.buildBaseRunPayload({}, {
    workflow_id: "wf_payload_unknown_gate",
    version: "1.0.0",
    nodes: [{ id: "n1", type: "unknown_future_node" }],
    edges: [],
  }, 42);
  const runPayloadDefersUnknownType = payload?.workflow?.nodes?.[0]?.type === "unknown_future_node";
  if (!runPayloadDefersUnknownType) {
    fail("buildBaseRunPayload no longer preserves unknown node types for authoritative runtime validation");
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
      { type: "ai_refine", name: "AI", group: "AI", policy_section: "local_ai", policy_source: "local_policy" },
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
        contract_boundary: "node_catalog_truth",
        resolution_hint: "replace node type or sync Rust manifest / local node policy",
        action_text: "定位节点",
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

  const validationSupport = createWorkflowValidationSupport({
    fetchImpl: async () => {
      throw new Error("connect ECONNREFUSED");
    },
  });
  let rustUnavailableFailsClosed = false;
  try {
    await validationSupport.validateWorkflowDefinitionAuthoritatively({
      workflowDefinition: {
        workflow_id: "wf_unavailable",
        version: "1.0.0",
        nodes: [{ id: "n1", type: "ingest_files" }],
        edges: [],
      },
      validationScope: "run",
    });
  } catch (error) {
    rustUnavailableFailsClosed = /workflow validation unavailable/i.test(String(error?.message || error));
  }
  if (!rustUnavailableFailsClosed) {
    fail("workflow validation service no longer fails closed when Rust is unavailable");
  }

  const workflowIpcStoreText = fs.readFileSync(workflowIpcStorePath, "utf8");
  const workflowIpcRunText = fs.readFileSync(workflowIpcRunPath, "utf8");
  const workflowIpcQueueAppsText = fs.readFileSync(workflowIpcQueueAppsPath, "utf8");
  const workflowEngineText = fs.readFileSync(workflowEnginePath, "utf8");
  const preflightControllerUiText = fs.readFileSync(preflightControllerUiPath, "utf8");
  const flowIoUiText = fs.readFileSync(flowIoUiPath, "utf8");
  const runPayloadUiText = fs.readFileSync(runPayloadUiPath, "utf8");

  const importRejectedUnknownType =
    /validateWorkflowDefinitionAuthoritatively/.test(workflowIpcStoreText)
    && !/assertWorkflowContract\(graph/.test(workflowIpcStoreText);
  if (!importRejectedUnknownType) {
    fail("workflow_ipc_store.js no longer routes import/save authority through Rust validation");
  }

  const payloadRejectedUnknownType =
    /validateWorkflowDefinitionAuthoritatively/.test(workflowIpcRunText)
    && /validateWorkflowDefinitionAuthoritatively/.test(workflowIpcQueueAppsText);
  if (!payloadRejectedUnknownType) {
    fail("run authoritative paths no longer route workflow validation through Rust");
  }

  const engineUsesRustValidation =
    /createWorkflowValidationSupport/.test(workflowEngineText)
    && /validateWorkflowDefinitionAuthoritatively/.test(workflowEngineText);
  if (!engineUsesRustValidation) {
    fail("workflow_engine.js no longer routes local engine validation through Rust");
  }

  const preflightUsesRustWorkflowValidation =
    /\/operators\/workflow_contract_v1\/validate/.test(preflightControllerUiText);
  if (!preflightUsesRustWorkflowValidation) {
    fail("preflight controller no longer calls Rust workflow contract validation");
  }

  const flowIoAvoidsLocalAssert = !/assertWorkflowContract/.test(flowIoUiText);
  if (!flowIoAvoidsLocalAssert) {
    fail("flow-io-ui still performs local authoritative workflow contract assert");
  }

  const runPayloadAvoidsLocalAssert = !/assertWorkflowContract/.test(runPayloadUiText);
  if (!runPayloadAvoidsLocalAssert) {
    fail("run-payload-ui still performs local authoritative workflow contract assert");
  }

  console.log(JSON.stringify({
    required,
    defaultVersion: defaultGraph.version,
    importMigrated: imported.contract.migrated,
    importRejectedUnknownType,
    payloadRejectedUnknownType,
    authoringRejectedUnknownType,
    preflightUnknownTypeGuided,
    normalizedVersion: imported.graph.version,
    runPayloadDefersUnknownType,
    engineUsesRustValidation,
    preflightUsesRustWorkflowValidation,
    flowIoAvoidsLocalAssert,
    runPayloadAvoidsLocalAssert,
    rustUnavailableFailsClosed,
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
