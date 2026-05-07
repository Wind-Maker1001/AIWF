#!/usr/bin/env node

const fs = require("node:fs");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

const REPO_ROOT = path.resolve(__dirname, "..", "..");
const DEFAULT_ACCEL_URL = "http://127.0.0.1:18082";

function usage() {
  return [
    "Usage:",
    "  node ops/scripts/migrate_workflow_version_import_fallback.js --kind workflow_json|template_pack|local_template_storage --input <path> --output <path> [--overwrite] [--rust-endpoint <url>]",
  ].join("\n");
}

function parseArgs(argv) {
  const args = {
    kind: "",
    input: "",
    output: "",
    overwrite: false,
    rustEndpoint: "",
  };
  for (let index = 0; index < argv.length; index += 1) {
    const token = String(argv[index] || "").trim();
    if (token === "--overwrite") {
      args.overwrite = true;
      continue;
    }
    const next = String(argv[index + 1] || "").trim();
    if (!next) {
      throw new Error(`missing value for ${token}\n${usage()}`);
    }
    if (token === "--kind") {
      args.kind = next;
      index += 1;
      continue;
    }
    if (token === "--input") {
      args.input = next;
      index += 1;
      continue;
    }
    if (token === "--output") {
      args.output = next;
      index += 1;
      continue;
    }
    if (token === "--rust-endpoint") {
      args.rustEndpoint = next;
      index += 1;
      continue;
    }
    throw new Error(`unknown argument: ${token}\n${usage()}`);
  }
  if (!["workflow_json", "template_pack", "local_template_storage"].includes(args.kind)) {
    throw new Error(`--kind must be one of workflow_json, template_pack, local_template_storage\n${usage()}`);
  }
  if (!args.input) {
    throw new Error(`--input is required\n${usage()}`);
  }
  if (!args.output) {
    throw new Error(`--output is required\n${usage()}`);
  }
  return args;
}

function clone(value) {
  return JSON.parse(JSON.stringify(value));
}

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

function ensureWritableOutput(filePath, overwrite) {
  if (fs.existsSync(filePath) && !overwrite) {
    throw new Error(`output already exists: ${filePath}. Re-run with --overwrite to replace it.`);
  }
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
}

function stripTemplatePackMetadata(normalizedPack) {
  return {
    schema_version: String(normalizedPack?.schema_version || ""),
    id: String(normalizedPack?.id || ""),
    name: String(normalizedPack?.name || ""),
    version: String(normalizedPack?.version || ""),
    source: String(normalizedPack?.source || ""),
    templates: Array.isArray(normalizedPack?.templates) ? normalizedPack.templates.map((item) => clone(item)) : [],
    created_at: String(normalizedPack?.created_at || ""),
  };
}

function resolveTemplateCandidate(source = {}) {
  if (source.workflow_definition && typeof source.workflow_definition === "object") {
    return clone(source.workflow_definition);
  }
  if (source.graph && typeof source.graph === "object") {
    return clone(source.graph);
  }
  return null;
}

async function loadStoreSupport() {
  return import(pathToFileURL(path.join(REPO_ROOT, "apps", "dify-desktop", "renderer", "workflow", "store-support.js")).href);
}

async function loadTemplateStorageContract() {
  return import(pathToFileURL(path.join(REPO_ROOT, "apps", "dify-desktop", "renderer", "workflow", "template-storage-contract.js")).href);
}

function loadTemplatePackContract() {
  return require(path.join(REPO_ROOT, "apps", "dify-desktop", "workflow_template_pack_contract.js"));
}

function loadWorkflowValidationSupport() {
  return require(path.join(REPO_ROOT, "apps", "dify-desktop", "workflow_validation_service.js"));
}

async function migrateWorkflowDefinition(rawWorkflow, storeSupport) {
  const migrated = storeSupport.normalizeImportedGraphWithContract(rawWorkflow, {
    allowVersionMigration: true,
  });
  const errors = Array.isArray(migrated?.contract?.errors) ? migrated.contract.errors : [];
  if (errors.length > 0) {
    throw new Error(`workflow migration failed: ${errors.join("; ")}`);
  }
  return migrated.graph;
}

async function validateCanonicalWorkflow(validationSupport, workflowDefinition, options = {}) {
  const validated = await validationSupport.validateWorkflowDefinitionAuthoritatively({
    workflowDefinition,
    rustEndpoint: options.rustEndpoint || "",
    allowVersionMigration: false,
    requireNonEmptyNodes: options.requireNonEmptyNodes === true,
    validationScope: String(options.validationScope || "authoring"),
  });
  return validated?.normalized_workflow_definition && typeof validated.normalized_workflow_definition === "object"
    ? validated.normalized_workflow_definition
    : workflowDefinition;
}

async function migrateWorkflowJson(raw, deps, args) {
  const migrated = await migrateWorkflowDefinition(raw, deps.storeSupport);
  return await validateCanonicalWorkflow(deps.validationSupport, migrated, {
    rustEndpoint: args.rustEndpoint,
    requireNonEmptyNodes: false,
    validationScope: "authoring",
  });
}

async function migrateTemplatePack(raw, deps, args) {
  const source = raw && typeof raw === "object" ? clone(raw) : {};
  const templates = Array.isArray(source.templates) ? source.templates : [];
  source.templates = templates.map((template) => {
    const out = template && typeof template === "object" ? { ...template } : {};
    const candidate = resolveTemplateCandidate(out);
    if (candidate) {
      out.workflow_definition = candidate;
    }
    delete out.graph;
    return out;
  });
  for (const template of source.templates) {
    if (!template.workflow_definition || typeof template.workflow_definition !== "object") {
      continue;
    }
    template.workflow_definition = await migrateWorkflowDefinition(template.workflow_definition, deps.storeSupport);
  }

  const normalized = deps.templatePackContract.normalizeTemplatePackArtifact(source, {
    allowVersionMigration: true,
    source: "migration_tool",
    allowLegacyGraphAlias: false,
  });

  normalized.templates = await Promise.all(
    (Array.isArray(normalized.templates) ? normalized.templates : []).map(async (template) => ({
      ...template,
      workflow_definition: await validateCanonicalWorkflow(
        deps.validationSupport,
        template.workflow_definition || {},
        {
          rustEndpoint: args.rustEndpoint,
          requireNonEmptyNodes: true,
          validationScope: "authoring",
        },
      ),
    })),
  );

  return stripTemplatePackMetadata(normalized);
}

async function migrateLocalTemplateStorage(raw, deps, args) {
  const source = Array.isArray(raw) ? raw.map((item) => clone(item)) : clone(raw && typeof raw === "object" ? raw : {});
  const items = Array.isArray(source)
    ? source
    : (Array.isArray(source.items) ? source.items : []);
  const migratedItems = await Promise.all(items.map(async (item) => {
    const out = item && typeof item === "object" ? { ...item } : {};
    const candidate = resolveTemplateCandidate(out);
    if (candidate) {
      out.workflow_definition = await migrateWorkflowDefinition(candidate, deps.storeSupport);
    }
    delete out.graph;
    return out;
  }));
  const normalized = deps.templateStorageContract.normalizeLocalTemplateStorage(
    Array.isArray(source)
      ? migratedItems
      : { ...source, items: migratedItems },
    {
      allowStorageSchemaMigration: true,
      allowEntrySchemaMigration: true,
      allowLegacyGraphAlias: false,
    },
  );
  normalized.items = await Promise.all(
    (Array.isArray(normalized.items) ? normalized.items : []).map(async (item) => ({
      ...item,
      workflow_definition: await validateCanonicalWorkflow(
        deps.validationSupport,
        item.workflow_definition || {},
        {
          rustEndpoint: args.rustEndpoint,
          requireNonEmptyNodes: true,
          validationScope: "authoring",
        },
      ),
    })),
  );
  return {
    schema_version: String(normalized.schema_version || ""),
    items: Array.isArray(normalized.items) ? normalized.items.map((item) => clone(item)) : [],
  };
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const inputPath = path.resolve(args.input);
  const outputPath = path.resolve(args.output);
  const rustEndpoint = String(args.rustEndpoint || process.env.AIWF_ACCEL_URL || DEFAULT_ACCEL_URL).trim();

  if (!fs.existsSync(inputPath)) {
    throw new Error(`input file not found: ${inputPath}`);
  }
  ensureWritableOutput(outputPath, args.overwrite);

  const [
    storeSupport,
    templateStorageContract,
  ] = await Promise.all([
    loadStoreSupport(),
    loadTemplateStorageContract(),
  ]);
  const templatePackContract = loadTemplatePackContract();
  const { createWorkflowValidationSupport } = loadWorkflowValidationSupport();
  const validationSupport = createWorkflowValidationSupport();
  const raw = readJson(inputPath);
  const deps = {
    storeSupport,
    templatePackContract,
    templateStorageContract,
    validationSupport,
  };

  let output;
  if (args.kind === "workflow_json") {
    output = await migrateWorkflowJson(raw, deps, { rustEndpoint });
  } else if (args.kind === "template_pack") {
    output = await migrateTemplatePack(raw, deps, { rustEndpoint });
  } else {
    output = await migrateLocalTemplateStorage(raw, deps, { rustEndpoint });
  }

  fs.writeFileSync(outputPath, `${JSON.stringify(output, null, 2)}\n`, "utf8");
  console.log(JSON.stringify({
    ok: true,
    kind: args.kind,
    input: inputPath,
    output: outputPath,
    rust_endpoint: rustEndpoint,
  }));
}

main().catch((error) => {
  console.error(String(error?.stack || error || "workflow version import migration failed"));
  process.exit(1);
});
