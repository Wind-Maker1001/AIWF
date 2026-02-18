const path = require("path");
const { fork } = require("child_process");

function nowMs() {
  return Date.now();
}

class WorkerSlot {
  constructor(task) {
    this.task = String(task || "");
    this.workerPath = path.join(__dirname, "isolated_worker_process.js");
    this.child = null;
    this.busy = false;
    this.pending = null;
    this.closed = false;
    this.spawn();
  }

  spawn() {
    this.child = fork(this.workerPath, [], {
      stdio: ["ignore", "ignore", "ignore", "ipc"],
      windowsHide: true,
    });
    this.child.on("message", (msg) => this.handleMessage(msg));
    this.child.on("error", (e) => this.handleCrash(e));
    this.child.on("exit", (code) => this.handleCrash(new Error(`chiplet worker exited: ${code}`)));
  }

  handleMessage(msg) {
    if (!msg || typeof msg !== "object") return;
    if (!this.pending) return;
    const id = String(msg.id || "");
    if (!id || id !== this.pending.id) return;
    const p = this.pending;
    this.pending = null;
    this.busy = false;
    clearTimeout(p.timer);
    if (msg.type === "ok") p.resolve(msg.data || {});
    else p.reject(new Error(String(msg.error || "chiplet worker error")));
  }

  handleCrash(err) {
    if (this.closed) return;
    if (this.pending) {
      const p = this.pending;
      this.pending = null;
      this.busy = false;
      clearTimeout(p.timer);
      p.reject(err instanceof Error ? err : new Error(String(err || "chiplet worker crashed")));
    }
    this.recreate();
  }

  recreate() {
    if (this.closed) return;
    try { this.child?.removeAllListeners(); } catch {}
    this.child = null;
    this.spawn();
  }

  async run(payload = {}, timeoutMs = 120000) {
    if (this.busy || !this.child) throw new Error("worker slot unavailable");
    const id = `${this.task}_${nowMs()}_${Math.random().toString(36).slice(2, 8)}`;
    this.busy = true;
    return await new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        if (!this.pending || this.pending.id !== id) return;
        this.pending = null;
        this.busy = false;
        reject(new Error(`chiplet worker timeout: ${this.task}`));
        this.recreate();
      }, Math.max(1000, Number(timeoutMs) || 120000));
      this.pending = { id, resolve, reject, timer };
      try {
        this.child.send({ type: "run", id, task: this.task, payload });
      } catch (e) {
        clearTimeout(timer);
        this.pending = null;
        this.busy = false;
        reject(e);
        this.recreate();
      }
    });
  }

  shutdown() {
    this.closed = true;
    if (this.pending) {
      clearTimeout(this.pending.timer);
      this.pending.reject(new Error("chiplet worker shutting down"));
      this.pending = null;
    }
    try { this.child?.kill("SIGKILL"); } catch {}
    this.busy = false;
    this.child = null;
  }
}

class TaskWorkerPool {
  constructor(task, size) {
    this.task = String(task || "");
    const n = Number(size);
    this.size = Number.isFinite(n) && n > 0 ? Math.floor(n) : 1;
    this.slots = Array.from({ length: this.size }, () => new WorkerSlot(this.task));
    this.queue = [];
  }

  pickIdleSlot() {
    return this.slots.find((s) => !s.busy) || null;
  }

  pump() {
    while (this.queue.length > 0) {
      const slot = this.pickIdleSlot();
      if (!slot) break;
      const item = this.queue.shift();
      slot.run(item.payload, item.timeoutMs).then(item.resolve).catch(item.reject).finally(() => this.pump());
    }
  }

  async run(payload = {}, timeoutMs = 120000) {
    const slot = this.pickIdleSlot();
    if (slot) {
      try {
        return await slot.run(payload, timeoutMs);
      } finally {
        this.pump();
      }
    }
    return await new Promise((resolve, reject) => {
      this.queue.push({ payload, timeoutMs, resolve, reject });
    });
  }

  shutdown() {
    while (this.queue.length) {
      const item = this.queue.shift();
      item.reject(new Error("chiplet worker pool shutting down"));
    }
    this.slots.forEach((s) => s.shutdown());
  }

  stats() {
    const busy = this.slots.reduce((acc, s) => acc + (s.busy ? 1 : 0), 0);
    return {
      task: this.task,
      size: this.size,
      busy,
      idle: Math.max(0, this.size - busy),
      queued: this.queue.length,
    };
  }
}

const POOLS = new Map();

function poolSizeForTask(task) {
  const t = String(task || "").toLowerCase();
  const key = t === "compute_rust"
    ? "AIWF_CHIPLET_COMPUTE_POOL_SIZE"
    : t === "ai_refine"
      ? "AIWF_CHIPLET_AI_POOL_SIZE"
      : "AIWF_CHIPLET_POOL_SIZE";
  const n = Number(process.env[key] || process.env.AIWF_CHIPLET_POOL_SIZE || "1");
  if (Number.isFinite(n) && n >= 1 && n <= 8) return Math.floor(n);
  return 1;
}

function getPool(task) {
  const key = String(task || "");
  if (!POOLS.has(key)) POOLS.set(key, new TaskWorkerPool(key, poolSizeForTask(key)));
  return POOLS.get(key);
}

async function runIsolatedTask(task, payload = {}, timeoutMs = 120000) {
  const p = getPool(task);
  return await p.run(payload, timeoutMs);
}

function shutdownAllPools() {
  for (const p of POOLS.values()) p.shutdown();
  POOLS.clear();
}

function getPoolStats() {
  const pools = Array.from(POOLS.values()).map((p) => p.stats());
  const totals = pools.reduce((acc, p) => {
    acc.size += p.size;
    acc.busy += p.busy;
    acc.idle += p.idle;
    acc.queued += p.queued;
    return acc;
  }, { size: 0, busy: 0, idle: 0, queued: 0 });
  return { pools, totals };
}

process.on("exit", shutdownAllPools);
process.on("SIGINT", shutdownAllPools);
process.on("SIGTERM", shutdownAllPools);

module.exports = { runIsolatedTask, shutdownAllPools, getPoolStats };
