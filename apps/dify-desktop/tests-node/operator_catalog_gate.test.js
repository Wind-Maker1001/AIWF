const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { spawnSync } = require("node:child_process");
const support = require("../../../ops/scripts/operator_manifest_support.js");
const { pathToFileURL } = require("node:url");

function parseJsonLine(output) {
  const lines = String(output || "")
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);
  for (let index = lines.length - 1; index >= 0; index -= 1) {
    const line = lines[index];
    if (!line.startsWith("{") || !line.endsWith("}")) {
      continue;
    }
    return JSON.parse(line);
  }
  return null;
}

test("operator catalog sync gate passes", () => {
  const repoRoot = path.resolve(__dirname, "../../..");
  const script = path.resolve(repoRoot, "ops/scripts/check_operator_catalog_sync.ps1");
  const result = spawnSync("powershell", ["-ExecutionPolicy", "Bypass", "-File", script], {
    cwd: repoRoot,
    encoding: "utf8",
  });

  assert.equal(result.status, 0, `${result.stdout}\n${result.stderr}`);
  const payload = parseJsonLine(`${result.stdout}\n${result.stderr}`);
  assert.ok(payload, `expected structured JSON payload in output:\n${result.stdout}\n${result.stderr}`);
  assert.equal(payload.status, "passed");
  assert.match(payload.manifestPath, /operators_manifest\.v1\.json/i);
  assert.match(payload.schemaPath, /operators_manifest\.schema\.json/i);
  assert.match(payload.desktopModulePath, /rust_operator_manifest\.generated\.js/i);
  assert.match(payload.rendererModulePath, /renderer[\\\/]workflow[\\\/]rust_operator_manifest\.generated\.js/i);
  assert.ok(payload.manifestOperatorCount > 0);
  assert.ok(payload.publishedCount > 0);
  assert.ok(payload.workflowCount > 0);
  assert.ok(payload.desktopExposableCount > 0);
  assert.ok(payload.desktopModuleCount > 0);
  assert.ok(payload.rustMappedCount > 0);
  assert.ok(payload.defaultsCatalogCount > 0);
  assert.ok(payload.builtinOperatorCount > 0);
  assert.ok(payload.drift);
  assert.deepEqual(payload.drift.manifestMissingOperators, []);
  assert.deepEqual(payload.drift.manifestStaleOperators, []);
  assert.deepEqual(payload.drift.manifestMetadataDrift, []);
  assert.deepEqual(payload.drift.desktopModuleMissingOperators, []);
  assert.deepEqual(payload.drift.desktopModuleStaleOperators, []);
  assert.deepEqual(payload.drift.desktopModuleMetadataDrift, []);
  assert.deepEqual(payload.drift.missingPresentationOperators, []);
  assert.deepEqual(payload.drift.stalePresentationOperators, []);
  assert.deepEqual(payload.drift.invalidPresentationEntries, []);
  assert.deepEqual(payload.drift.missingPublishedInCatalog, []);
  assert.deepEqual(payload.drift.missingPublishedInRouting, []);
  assert.deepEqual(payload.drift.missingDesktopExposableInCatalog, []);
  assert.deepEqual(payload.drift.missingDesktopExposableInRouting, []);
});

test("operator manifest export writes a machine-readable Rust authority file", () => {
  const repoRoot = path.resolve(__dirname, "../../..");
  const script = path.resolve(repoRoot, "ops/scripts/export_operator_manifest.ps1");
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "aiwf-operator-manifest-"));
  const outputPath = path.join(tempDir, "operators_manifest.v1.json");
  const desktopModulePath = path.join(tempDir, "rust_operator_manifest.generated.js");
  const rendererModulePath = path.join(tempDir, "rust_operator_manifest.generated.mjs");
  const result = spawnSync("powershell", [
    "-ExecutionPolicy",
    "Bypass",
    "-File",
    script,
    "-OutputPath",
    outputPath,
    "-DesktopModulePath",
    desktopModulePath,
    "-RendererModulePath",
    rendererModulePath,
  ], {
    cwd: repoRoot,
    encoding: "utf8",
  });

  assert.equal(result.status, 0, `${result.stdout}\n${result.stderr}`);
  const payload = JSON.parse(fs.readFileSync(outputPath, "utf8"));
  const validation = support.validateOperatorManifest(payload);
  assert.equal(validation.ok, true, validation.errors.join("\n"));
  assert.equal(payload.manifest_version, "v1");
  assert.equal(payload.authority, "rust_operator_catalog");
  assert.ok(Array.isArray(payload.operators));
  assert.ok(payload.operators.length > 0);
  assert.ok(payload.summary.known_operator_count >= payload.summary.published_operator_count);
  assert.ok(payload.summary.workflow_exposable_count >= payload.summary.desktop_exposable_count);
  const desktopModule = require(desktopModulePath);
  assert.ok(Array.isArray(desktopModule.DESKTOP_RUST_OPERATOR_TYPES));
  assert.ok(desktopModule.DESKTOP_RUST_OPERATOR_TYPES.length > 0);
  assert.equal(desktopModule.DESKTOP_RUST_OPERATOR_TYPES.length, payload.summary.desktop_exposable_count);
  return import(pathToFileURL(rendererModulePath).href).then((rendererModule) => {
    assert.ok(Array.isArray(rendererModule.DESKTOP_RUST_OPERATOR_TYPES));
    assert.equal(rendererModule.DESKTOP_RUST_OPERATOR_TYPES.length, payload.summary.desktop_exposable_count);
    assert.deepEqual(rendererModule.DESKTOP_RUST_OPERATOR_TYPES, desktopModule.DESKTOP_RUST_OPERATOR_TYPES);
    assert.equal(rendererModule.DESKTOP_RUST_OPERATOR_TYPES.includes("postprocess_rows_v1"), true);
    assert.equal(rendererModule.DESKTOP_RUST_OPERATOR_METADATA.postprocess_rows_v1.palette_hidden, true);
    const stdoutPayload = parseJsonLine(`${result.stdout}\n${result.stderr}`);
    assert.ok(stdoutPayload, `expected structured JSON payload in output:\n${result.stdout}\n${result.stderr}`);
    assert.match(stdoutPayload.schemaPath, /operators_manifest\.schema\.json/i);
    assert.match(stdoutPayload.desktopModulePath, /rust_operator_manifest\.generated\.js/i);
    assert.match(stdoutPayload.rendererModulePath, /rust_operator_manifest\.generated\.(js|mjs)/i);
  });
});

test("operator catalog sync gate emits structured failure details", () => {
  const repoRoot = path.resolve(__dirname, "../../..");
  const script = path.resolve(repoRoot, "ops/scripts/check_operator_catalog_sync.ps1");
  const result = spawnSync("powershell", [
    "-ExecutionPolicy",
    "Bypass",
    "-File",
    script,
    "-RequiredPublishedOperators",
    "__impossible_operator__",
  ], {
    cwd: repoRoot,
    encoding: "utf8",
  });

  assert.notEqual(result.status, 0, "forcing a missing required published operator should fail");
  const payload = parseJsonLine(`${result.stdout}\n${result.stderr}`);
  assert.ok(payload, `expected structured JSON payload in output:\n${result.stdout}\n${result.stderr}`);
  assert.equal(payload.status, "failed");
  assert.deepEqual(payload.requiredPublishedOperators, ["__impossible_operator__"]);
  assert.deepEqual(payload.drift.requiredPublishedMissing, ["__impossible_operator__"]);
  assert.match(payload.issues.join("\n"), /required published rust operators missing from catalog truth/i);
});
