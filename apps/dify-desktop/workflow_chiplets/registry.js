class WorkflowChipletRegistry {
  constructor() {
    this._map = new Map();
  }

  normalizeOptions(type, chiplet) {
    const t = String(type || "").trim();
    const priority = Number(chiplet?.priority);
    const timeoutMs = Number(chiplet?.timeout_ms);
    const retries = Number(chiplet?.retries);
    const threshold = Number(chiplet?.circuit?.failure_threshold);
    const cooldownMs = Number(chiplet?.circuit?.cooldown_ms);
    return {
      id: String(chiplet?.id || `chiplet.${t}.v1`),
      priority: Number.isFinite(priority) ? priority : 100,
      timeout_ms: Number.isFinite(timeoutMs) && timeoutMs > 0 ? timeoutMs : 180000,
      retries: Number.isFinite(retries) && retries >= 0 ? Math.floor(retries) : 0,
      circuit: {
        enabled: chiplet?.circuit?.enabled !== false,
        failure_threshold: Number.isFinite(threshold) && threshold > 0 ? Math.floor(threshold) : 3,
        cooldown_ms: Number.isFinite(cooldownMs) && cooldownMs > 0 ? Math.floor(cooldownMs) : 30000,
      },
    };
  }

  register(type, chiplet) {
    const t = String(type || "").trim();
    if (!t) throw new Error("chiplet type is required");
    if (!chiplet || typeof chiplet.run !== "function") {
      throw new Error(`chiplet ${t} must provide run(ctx, node, envelope)`);
    }
    const normalized = {
      ...chiplet,
      ...this.normalizeOptions(t, chiplet),
    };
    this._map.set(t, normalized);
    return this;
  }

  resolve(type) {
    return this._map.get(String(type || "").trim()) || null;
  }

  has(type) {
    return this._map.has(String(type || "").trim());
  }

  list() {
    return Array.from(this._map.keys()).sort();
  }
}

module.exports = {
  WorkflowChipletRegistry,
};
