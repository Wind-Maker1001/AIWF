function versionListRequestPayload(limit = 120) {
  return { limit };
}

function versionComparePayload(versionA, versionB) {
  return {
    version_a: String(versionA || "").trim(),
    version_b: String(versionB || "").trim(),
  };
}

function cacheStatsStatusText(ok, error) {
  return ok ? "缓存已清空" : `清空缓存失败: ${error || "unknown"}`;
}

export {
  cacheStatsStatusText,
  versionComparePayload,
  versionListRequestPayload,
};
