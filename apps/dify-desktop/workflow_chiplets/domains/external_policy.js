const crypto = require("crypto");

function parsePluginCapabilityAllowlist(config = {}) {
  const fromCfg = Array.isArray(config?.chiplet_plugin_capability_allowlist)
    ? config.chiplet_plugin_capability_allowlist
    : [];
  const fromEnv = String(process.env.AIWF_CHIPLET_CAPABILITY_ALLOWLIST || "")
    .split(/[;,]/)
    .map((x) => String(x || "").trim())
    .filter(Boolean);
  const all = [...fromCfg, ...fromEnv]
    .map((x) => String(x || "").trim())
    .filter(Boolean);
  return Array.from(new Set(all));
}

function sha256Text(s) {
  return crypto.createHash("sha256").update(String(s || ""), "utf8").digest("hex");
}

function buildPluginSignatureBase(manifest, entrySource) {
  const name = String(manifest?.name || "").trim();
  const version = String(manifest?.version || "").trim();
  const apiVersion = String(manifest?.api_version || "").trim();
  const entryHash = sha256Text(entrySource || "");
  return `${name}\n${version}\n${apiVersion}\n${entryHash}`;
}

function verifyPluginSignature(manifest, entrySource, signingSecret) {
  const sig = String(manifest?.signature_hmac_sha256 || "").trim().toLowerCase();
  if (!signingSecret) return { ok: true, skipped: true };
  if (!sig) return { ok: false, error: "plugin signature required when signing secret is set" };
  const base = buildPluginSignatureBase(manifest, entrySource);
  const expected = crypto.createHmac("sha256", String(signingSecret)).update(base, "utf8").digest("hex");
  if (sig !== expected) return { ok: false, error: "plugin signature mismatch" };
  return { ok: true, skipped: false };
}

function normalizeCapabilities(manifest) {
  return Array.isArray(manifest?.capabilities)
    ? manifest.capabilities.map((x) => String(x || "").trim()).filter(Boolean)
    : [];
}

function findBlockedCapabilities(capabilities = [], allowlist = []) {
  if (!Array.isArray(allowlist) || allowlist.length === 0) return [];
  return capabilities.filter((c) => !allowlist.includes(c));
}

module.exports = {
  parsePluginCapabilityAllowlist,
  buildPluginSignatureBase,
  verifyPluginSignature,
  normalizeCapabilities,
  findBlockedCapabilities,
};
