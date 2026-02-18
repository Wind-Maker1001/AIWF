from __future__ import annotations

import os
from typing import Any, Dict

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse


app = FastAPI(title="AIWF Dify Console", version="1.0.0")


def _default_env_file() -> str:
    here = os.path.abspath(os.path.dirname(__file__))
    return os.path.normpath(os.path.join(here, "..", "..", "ops", "config", "dev.env"))


def _import_dotenv(path: str) -> None:
    if not os.path.isfile(path):
        return
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s or s.startswith("#") or "=" not in s:
                continue
            k, v = s.split("=", 1)
            k = k.strip()
            v = v.strip().strip('"').strip("'")
            if k and k not in os.environ:
                os.environ[k] = v


def _base_url() -> str:
    return str(os.getenv("AIWF_BASE_URL") or "http://127.0.0.1:18080").rstrip("/")


def _api_headers() -> Dict[str, str]:
    out = {"Content-Type": "application/json"}
    api_key = str(os.getenv("AIWF_API_KEY") or "").strip()
    if api_key:
        out["X-API-Key"] = api_key
    return out


@app.on_event("startup")
def on_startup() -> None:
    env_file = os.getenv("AIWF_ENV_FILE") or _default_env_file()
    _import_dotenv(env_file)


@app.get("/health")
def health() -> Dict[str, Any]:
    return {"ok": True, "service": "dify-console", "base_url": _base_url()}


@app.get("/api/base_health")
def base_health() -> Dict[str, Any]:
    base = _base_url()
    headers = _api_headers()
    result: Dict[str, Any] = {"ok": True, "base_url": base}
    try:
        with httpx.Client(timeout=8.0) as client:
            result["actuator"] = client.get(f"{base}/actuator/health", headers=headers).json()
            result["dify_bridge"] = client.get(f"{base}/api/v1/integrations/dify/health", headers=headers).json()
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"base health check failed: {e}")
    return result


@app.post("/api/run_cleaning")
async def run_cleaning(request: Request) -> JSONResponse:
    body = await request.json()
    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="request body must be object")

    base = _base_url()
    headers = _api_headers()
    url = f"{base}/api/v1/integrations/dify/run_cleaning"
    try:
        async with httpx.AsyncClient(timeout=180.0) as client:
            resp = await client.post(url, json=body, headers=headers)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"call base failed: {e}")

    if resp.status_code >= 400:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    return JSONResponse(resp.json())


def _index_html() -> str:
    return """<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>AIWF 作业助手</title>
  <style>
    :root {
      --bg1: #eef6ff;
      --bg2: #fff7ee;
      --panel: #ffffff;
      --line: #d7e0eb;
      --text: #1f2d3d;
      --muted: #667788;
      --brand: #0b5ea8;
      --brand2: #0b7d67;
      --ok: #087443;
      --bad: #b42318;
      --shadow: 0 14px 38px rgba(21, 38, 59, .10);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      background:
        radial-gradient(1200px 500px at 2% -20%, #dbeaff 0, transparent 62%),
        radial-gradient(1200px 500px at 96% -24%, #ffe8d8 0, transparent 62%),
        linear-gradient(180deg, var(--bg1), var(--bg2));
      color: var(--text);
      font-family: "Segoe UI", "Microsoft YaHei", sans-serif;
      min-height: 100vh;
    }
    .shell { max-width: 1240px; margin: 0 auto; padding: 22px; display: grid; gap: 14px; }
    .hero {
      border-radius: 18px;
      background: linear-gradient(120deg, #103660, #0b5ea8 46%, #0b7d67);
      color: #f4f9ff;
      padding: 22px 24px;
      box-shadow: var(--shadow);
    }
    .hero h1 { margin: 0; font-size: 30px; }
    .hero p { margin: 8px 0 0; opacity: .93; }
    .steps {
      margin-top: 10px;
      display: inline-flex;
      gap: 8px;
      font-size: 12px;
      background: rgba(255,255,255,.15);
      border-radius: 999px;
      padding: 6px 10px;
    }
    .layout { display: grid; grid-template-columns: 410px 1fr; gap: 14px; align-items: start; }
    @media (max-width: 1080px) { .layout { grid-template-columns: 1fr; } }
    .card { background: var(--panel); border: 1px solid var(--line); border-radius: 16px; box-shadow: var(--shadow); }
    .hd { padding: 13px 16px 10px; border-bottom: 1px solid #e9eff7; font-weight: 700; }
    .bd { padding: 14px 16px; }
    .tip {
      font-size: 12px;
      color: var(--muted);
      margin-bottom: 8px;
      background: #f3f8fe;
      border: 1px dashed #cfe0f2;
      border-radius: 10px;
      padding: 8px;
    }
    .field { margin-bottom: 10px; }
    .field label { display: block; font-size: 12px; color: var(--muted); margin-bottom: 4px; }
    input, select, textarea {
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 10px;
      padding: 9px 10px;
      font-size: 14px;
      color: var(--text);
      background: #fff;
      font-family: inherit;
    }
    textarea { min-height: 108px; resize: vertical; }
    .btns { display: flex; gap: 8px; flex-wrap: wrap; margin-top: 8px; }
    button {
      border: 0;
      border-radius: 10px;
      padding: 10px 14px;
      font-size: 14px;
      color: #fff;
      cursor: pointer;
      background: linear-gradient(120deg, var(--brand), #2a79ca);
      box-shadow: 0 8px 18px rgba(11, 94, 168, .24);
      transition: transform .12s ease, opacity .12s ease;
    }
    button:hover { transform: translateY(-1px); }
    button.secondary {
      background: linear-gradient(120deg, #44566b, #5d748f);
      box-shadow: 0 8px 18px rgba(68, 86, 107, .24);
    }
    button:disabled { opacity: .65; cursor: not-allowed; transform: none; }
    .status { margin-top: 9px; font-size: 13px; min-height: 18px; color: var(--muted); }
    .status.ok { color: var(--ok); }
    .status.bad { color: var(--bad); }
    details {
      margin-top: 8px;
      border: 1px solid #dfe8f2;
      border-radius: 10px;
      background: #fbfdff;
      padding: 8px 10px;
    }
    summary { cursor: pointer; color: #34506d; font-size: 13px; font-weight: 600; }
    .metrics { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 10px; }
    @media (max-width: 860px) { .metrics { grid-template-columns: repeat(2, minmax(0, 1fr)); } }
    .metric { border: 1px solid #dfe7f2; border-radius: 12px; padding: 10px; background: linear-gradient(180deg, #fefefe, #f4f9ff); }
    .metric .k { color: var(--muted); font-size: 12px; }
    .metric .v { margin-top: 2px; font-size: 20px; font-weight: 700; }
    .tbl {
      width: 100%; border-collapse: collapse; border: 1px solid #e4ecf5;
      border-radius: 12px; overflow: hidden; font-size: 13px;
    }
    .tbl th, .tbl td { border-bottom: 1px solid #eef2f7; padding: 8px 9px; text-align: left; }
    .tbl th { background: #f4f8fd; color: #4d6277; font-weight: 700; }
    .tbl tr:last-child td { border-bottom: 0; }
    .path-cell {
      max-width: 560px;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    pre {
      margin: 0;
      min-height: 250px;
      border-radius: 12px;
      border: 1px solid #1f2d3d;
      background: #0f1720;
      color: #d9e6f6;
      padding: 12px;
      font-size: 12px;
      line-height: 1.45;
      white-space: pre-wrap;
      word-break: break-word;
      overflow: auto;
    }
  </style>
</head>
<body>
  <div class="shell">
    <section class="hero">
      <h1>AIWF 作业助手</h1>
      <p>双击打开就能用。按下面三步操作：填题目 -> 点运行 -> 下载结果。</p>
      <div class="steps"><span>步骤 1: 填信息</span><span>步骤 2: 运行</span><span>步骤 3: 看结果</span></div>
    </section>

    <section class="layout">
      <article class="card">
        <div class="hd">一步一步填写</div>
        <div class="bd">
          <div class="tip">不懂技术参数也能用：只填“题目”和“数据文件路径”即可。</div>

          <div class="field">
            <label>这次作业/辩论的标题</label>
            <input id="reportTitle" placeholder="例如：AI 对就业影响的正反证据整理" />
          </div>

          <div class="field">
            <label>数据文件路径（可选）</label>
            <input id="inputCsvPath" placeholder="例如：C:\\数据\\input.csv" />
          </div>

          <div class="field">
            <label>输出语言</label>
            <select id="officeLang"><option value="zh" selected>中文</option><option value="en">English</option></select>
          </div>

          <div class="field">
            <label>成品风格</label>
            <select id="officeTheme"><option value="debate" selected>辩论风</option><option value="academic">学术风</option><option value="professional">商务风</option></select>
          </div>

          <div class="btns">
            <button id="btnRun">开始生成作业成品</button>
            <button id="btnHealth" class="secondary">检查系统是否正常</button>
          </div>
          <div id="status" class="status"></div>

          <details>
            <summary>高级设置（懂的人再改）</summary>
            <div class="field" style="margin-top:8px;"><label>owner</label><input id="owner" value="dify" /></div>
            <div class="field"><label>actor</label><input id="actor" value="dify" /></div>
            <div class="field"><label>ruleset_version</label><input id="ruleset" value="v1" /></div>
            <div class="field"><label>office_max_rows（默认 5000）</label><input id="officeMaxRows" placeholder="5000" /></div>
            <div class="field"><label>extra params JSON</label><textarea id="extraParams">{}</textarea></div>
          </details>
        </div>
      </article>

      <article style="display:grid; gap:14px;">
        <div class="card">
          <div class="hd">运行结果一眼看懂</div>
          <div class="bd">
            <div class="metrics">
              <div class="metric"><div class="k">任务编号</div><div class="v" id="mJobId">-</div></div>
              <div class="metric"><div class="k">是否成功</div><div class="v" id="mRunOk">-</div></div>
              <div class="metric"><div class="k">耗时(秒)</div><div class="v" id="mSec">-</div></div>
              <div class="metric"><div class="k">成品数量</div><div class="v" id="mArts">0</div></div>
            </div>
          </div>
        </div>

        <div class="card">
          <div class="hd">成品文件（xlsx/docx/pptx）</div>
          <div class="bd">
            <table class="tbl">
              <thead><tr><th>文件ID</th><th>类型</th><th>路径</th></tr></thead>
              <tbody id="artifactRows"><tr><td colspan="3" style="color:#75869a;">还没有结果，先点“开始生成作业成品”</td></tr></tbody>
            </table>
          </div>
        </div>

        <div class="card">
          <div class="hd">详细日志（给技术同学）</div>
          <div class="bd"><pre id="result">{}</pre></div>
        </div>
      </article>
    </section>
  </div>

  <script>
    const statusEl = document.getElementById("status");
    const resultEl = document.getElementById("result");
    const btnRun = document.getElementById("btnRun");
    const btnHealth = document.getElementById("btnHealth");
    const mJobId = document.getElementById("mJobId");
    const mRunOk = document.getElementById("mRunOk");
    const mSec = document.getElementById("mSec");
    const mArts = document.getElementById("mArts");
    const artifactRows = document.getElementById("artifactRows");

    const setStatus = (msg, ok=true) => {
      statusEl.className = "status " + (ok ? "ok" : "bad");
      statusEl.textContent = msg;
    };
    const showJson = (obj) => { resultEl.textContent = JSON.stringify(obj, null, 2); };

    const escHtml = (v) => String(v ?? "")
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#39;");

    const renderArtifacts = (arr) => {
      if (!Array.isArray(arr) || arr.length === 0) {
        artifactRows.innerHTML = '<tr><td colspan="3" style="color:#75869a;">还没有结果，先点“开始生成作业成品”</td></tr>';
        return;
      }
      artifactRows.innerHTML = arr.map(a => {
        const id = escHtml(a.artifact_id || "-");
        const kind = escHtml(a.kind || "-");
        const path = escHtml(a.path || "-");
        return `<tr><td>${id}</td><td>${kind}</td><td class="path-cell" title="${path}">${path}</td></tr>`;
      }).join("");
    };

    const updateMetrics = (payload) => {
      mJobId.textContent = payload?.job_id || "-";
      mRunOk.textContent = payload?.run?.ok ? "成功" : "失败";
      mSec.textContent = payload?.run?.seconds ?? "-";
      const ac = Array.isArray(payload?.artifacts) ? payload.artifacts.length : 0;
      mArts.textContent = String(ac);
      renderArtifacts(payload?.artifacts || []);
    };

    const lockButtons = (locked) => {
      btnRun.disabled = locked;
      btnHealth.disabled = locked;
    };

    btnHealth.addEventListener("click", async () => {
      lockButtons(true);
      setStatus("正在检查系统...");
      try {
        const r = await fetch("/api/base_health");
        const j = await r.json();
        showJson(j);
        setStatus("系统正常，可以开始", true);
      } catch (e) {
        setStatus("系统检查失败: " + e, false);
      } finally {
        lockButtons(false);
      }
    });

    btnRun.addEventListener("click", async () => {
      lockButtons(true);
      setStatus("正在生成中，请稍等...");
      let extra = {};
      try {
        extra = JSON.parse(document.getElementById("extraParams").value || "{}");
      } catch {
        setStatus("高级设置里的 JSON 格式不对", false);
        lockButtons(false);
        return;
      }

      const params = Object.assign({}, extra, {
        office_lang: document.getElementById("officeLang").value,
        office_theme: document.getElementById("officeTheme").value
      });

      const inputCsvPath = document.getElementById("inputCsvPath").value.trim();
      const reportTitle = document.getElementById("reportTitle").value.trim();
      const officeMaxRows = document.getElementById("officeMaxRows").value.trim();
      if (inputCsvPath) params.input_csv_path = inputCsvPath;
      if (reportTitle) params.report_title = reportTitle;
      if (officeMaxRows) params.office_max_rows = Number(officeMaxRows);

      const body = {
        owner: document.getElementById("owner").value || "dify",
        actor: document.getElementById("actor").value || "dify",
        ruleset_version: document.getElementById("ruleset").value || "v1",
        params
      };

      try {
        const r = await fetch("/api/run_cleaning", {
          method: "POST",
          headers: {"Content-Type": "application/json"},
          body: JSON.stringify(body)
        });
        const j = await r.json();
        showJson(j);
        updateMetrics(j || {});
        if (r.ok && j.ok) {
          setStatus("已完成，任务编号：" + j.job_id, true);
        } else {
          setStatus("生成失败，请看下方详细日志", false);
        }
      } catch (e) {
        setStatus("请求失败: " + e, false);
      } finally {
        lockButtons(false);
      }
    });
  </script>
</body>
</html>
"""
@app.get("/", response_class=HTMLResponse)
def index() -> str:
    return _index_html()


