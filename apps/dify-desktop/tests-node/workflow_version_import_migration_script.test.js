const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const http = require("node:http");
const { spawn } = require("node:child_process");

const repoRoot = path.resolve(__dirname, "../../..");
const scriptPath = path.join(repoRoot, "ops", "scripts", "migrate_workflow_version_import_fallback.js");
const fixtureRoot = path.join(__dirname, "fixtures", "workflow_version_import_normalization");

function createValidationServer(t) {
  const requests = [];
  const server = http.createServer((req, res) => {
    if (req.method !== "POST" || req.url !== "/operators/workflow_contract_v1/validate") {
      res.statusCode = 404;
      res.end("not found");
      return;
    }
    const chunks = [];
    req.on("data", (chunk) => chunks.push(chunk));
    req.on("end", () => {
      const payload = JSON.parse(Buffer.concat(chunks).toString("utf8") || "{}");
      requests.push(payload);
      const workflowDefinition = payload.workflow_definition && typeof payload.workflow_definition === "object"
        ? payload.workflow_definition
        : {};
      if (!String(workflowDefinition.version || "").trim()) {
        res.statusCode = 422;
        res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({
          ok: true,
          valid: false,
          error: "workflow.version is required",
          error_code: "workflow_graph_invalid",
          error_items: [{ path: "workflow.version", code: "required", message: "workflow.version is required" }],
          normalized_workflow_definition: workflowDefinition,
        }));
        return;
      }
      res.statusCode = 200;
      res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify({
        ok: true,
        valid: true,
        normalized_workflow_definition: workflowDefinition,
        notes: [],
      }));
    });
  });
  return new Promise((resolve, reject) => {
    server.listen(0, "127.0.0.1", () => {
      t.after(() => server.close());
      const address = server.address();
      resolve({
        endpoint: `http://127.0.0.1:${address.port}`,
        requests,
      });
    });
    server.on("error", reject);
  });
}

function runMigration(kind, inputPath, outputPath, rustEndpoint, extraArgs = []) {
  return new Promise((resolve, reject) => {
    const child = spawn("node", [
      scriptPath,
      "--kind",
      kind,
      "--input",
      inputPath,
      "--output",
      outputPath,
      "--rust-endpoint",
      rustEndpoint,
      ...extraArgs,
    ], {
      cwd: repoRoot,
      stdio: ["ignore", "pipe", "pipe"],
    });

    let stdout = "";
    let stderr = "";
    child.stdout.on("data", (chunk) => {
      stdout += chunk.toString("utf8");
    });
    child.stderr.on("data", (chunk) => {
      stderr += chunk.toString("utf8");
    });
    child.on("error", reject);
    child.on("close", (code) => {
      resolve({ status: code, stdout, stderr });
    });
  });
}

test("workflow version migration script canonicalizes unversioned workflow json", async (t) => {
  const { endpoint, requests } = await createValidationServer(t);
  const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "aiwf-workflow-version-migrate-"));
  const inputPath = path.join(tempRoot, "workflow_input.json");
  const outputPath = path.join(tempRoot, "workflow_output.json");
  fs.copyFileSync(path.join(fixtureRoot, "unversioned_workflow.json"), inputPath);

  const result = await runMigration("workflow_json", inputPath, outputPath, endpoint);

  assert.equal(result.status, 0, `${result.stdout}\n${result.stderr}`);
  const output = JSON.parse(fs.readFileSync(outputPath, "utf8"));
  assert.equal(output.workflow_id, "wf_fixture_unversioned");
  assert.equal(output.version, "1.0.0");
  assert.equal(Array.isArray(output.nodes), true);
  assert.equal(requests.length, 1);
  assert.equal(requests[0].workflow_definition.version, "1.0.0");

  const overwriteBlocked = await runMigration("workflow_json", inputPath, outputPath, endpoint);
  assert.notEqual(overwriteBlocked.status, 0);
  assert.match(`${overwriteBlocked.stdout}\n${overwriteBlocked.stderr}`, /output already exists/i);
});

test("workflow version migration script canonicalizes legacy template packs with graph alias", async (t) => {
  const { endpoint, requests } = await createValidationServer(t);
  const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "aiwf-template-pack-migrate-"));
  const inputPath = path.join(tempRoot, "template_pack_input.json");
  const outputPath = path.join(tempRoot, "template_pack_output.json");
  fs.copyFileSync(path.join(fixtureRoot, "template_pack_legacy_graph.json"), inputPath);

  const result = await runMigration("template_pack", inputPath, outputPath, endpoint);

  assert.equal(result.status, 0, `${result.stdout}\n${result.stderr}`);
  const output = JSON.parse(fs.readFileSync(outputPath, "utf8"));
  assert.equal(output.schema_version, "template_pack_artifact.v1");
  assert.equal(output.templates.length, 1);
  assert.equal(output.templates[0].workflow_definition.version, "1.0.0");
  assert.equal(Object.prototype.hasOwnProperty.call(output.templates[0], "graph"), false);
  assert.equal(requests.length, 1);
  assert.equal(requests[0].workflow_definition.version, "1.0.0");
});

test("workflow version migration script canonicalizes legacy local template storage with graph alias", async (t) => {
  const { endpoint, requests } = await createValidationServer(t);
  const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "aiwf-local-template-migrate-"));
  const inputPath = path.join(tempRoot, "local_template_input.json");
  const outputPath = path.join(tempRoot, "local_template_output.json");
  fs.copyFileSync(path.join(fixtureRoot, "local_template_storage_legacy_graph.json"), inputPath);

  const result = await runMigration("local_template_storage", inputPath, outputPath, endpoint);

  assert.equal(result.status, 0, `${result.stdout}\n${result.stderr}`);
  const output = JSON.parse(fs.readFileSync(outputPath, "utf8"));
  assert.equal(output.schema_version, "local_template_storage.v1");
  assert.equal(output.items.length, 1);
  assert.equal(output.items[0].workflow_definition.version, "1.0.0");
  assert.equal(Object.prototype.hasOwnProperty.call(output.items[0], "graph"), false);
  assert.equal(requests.length, 1);
  assert.equal(requests[0].workflow_definition.version, "1.0.0");
});
