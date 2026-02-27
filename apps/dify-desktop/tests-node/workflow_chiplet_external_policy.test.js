const test = require("node:test");
const assert = require("node:assert/strict");
const crypto = require("crypto");
const {
  parsePluginCapabilityAllowlist,
  buildPluginSignatureBase,
  verifyPluginSignature,
  normalizeCapabilities,
  findBlockedCapabilities,
} = require("../workflow_chiplets/domains/external_policy");

test("external policy parses capability allowlist from config and env", () => {
  const prev = process.env.AIWF_CHIPLET_CAPABILITY_ALLOWLIST;
  process.env.AIWF_CHIPLET_CAPABILITY_ALLOWLIST = "cap.env.a;cap.env.b";
  try {
    const out = parsePluginCapabilityAllowlist({
      chiplet_plugin_capability_allowlist: ["cap.cfg.a", "cap.env.a"],
    });
    assert.deepEqual(out, ["cap.cfg.a", "cap.env.a", "cap.env.b"]);
  } finally {
    if (typeof prev === "undefined") delete process.env.AIWF_CHIPLET_CAPABILITY_ALLOWLIST;
    else process.env.AIWF_CHIPLET_CAPABILITY_ALLOWLIST = prev;
  }
});

test("external policy verifies plugin signature with hmac secret", () => {
  const manifest = {
    name: "demo",
    version: "1.0.0",
    api_version: "v1",
  };
  const entrySource = "module.exports={register(){}};";
  const base = buildPluginSignatureBase(manifest, entrySource);
  manifest.signature_hmac_sha256 = crypto.createHmac("sha256", "unit-secret").update(base, "utf8").digest("hex");
  const out = verifyPluginSignature(manifest, entrySource, "unit-secret");
  assert.deepEqual(out, { ok: true, skipped: false });
});

test("external policy normalizes capabilities and reports blocked ones", () => {
  const caps = normalizeCapabilities({ capabilities: ["file.read", " net.none ", ""] });
  assert.deepEqual(caps, ["file.read", "net.none"]);
  const blocked = findBlockedCapabilities(caps, ["file.read"]);
  assert.deepEqual(blocked, ["net.none"]);
});
