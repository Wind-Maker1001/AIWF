import {
  loadWorkflowStatusMessage,
  saveWorkflowName,
  stringifyWorkflowGraph,
} from "./flow-io-support.js";
import {
  assertWorkflowContract,
  combineWorkflowMigrationReports,
} from "./workflow-contract.js";

function createWorkflowFlowIoUi(els, deps = {}) {
  const {
    setStatus = () => {},
    graphPayload = () => ({}),
    refreshVersions = async () => {},
    migrateLoadedWorkflowGraph = (graph) => ({ migrated: false, graph, notes: [] }),
    applyLoadedWorkflowGraph = () => {},
    getLoadedWorkflowName = () => "",
    renderMigrationReport = () => {},
  } = deps;

  function exportJson() {
    const json = stringifyWorkflowGraph(graphPayload());
    if (els.log) els.log.textContent = json;
    setStatus("宸插鍑烘祦绋?JSON 鍒板彸渚ф棩蹇楀尯", true);
  }

  async function saveFlow() {
    try {
      const graph = graphPayload();
      assertWorkflowContract(graph, { requireNonEmptyNodes: true });
      const name = saveWorkflowName(els.workflowName?.value || "");
      const out = await window.aiwfDesktop.saveWorkflow(graph, name);
      if (out?.ok) {
        setStatus(`娴佺▼宸蹭繚瀛? ${out.path}`, true);
        await refreshVersions();
      } else if (!out?.canceled) {
        setStatus(`淇濆瓨澶辫触: ${out?.error || "unknown"}`, false);
      }
    } catch (e) {
      setStatus(`淇濆瓨澶辫触: ${e}`, false);
    }
  }

  async function loadFlow() {
    try {
      const out = await window.aiwfDesktop.loadWorkflow();
      if (!out?.ok) {
        if (!out?.canceled) setStatus(`鍔犺浇澶辫触: ${out?.error || "unknown"}`, false);
        return;
      }
      const migrated = migrateLoadedWorkflowGraph(out.graph || {});
      const applied = applyLoadedWorkflowGraph(migrated.graph || {});
      const migrationReport = combineWorkflowMigrationReports(migrated, applied?.contract);
      if (els.workflowName) els.workflowName.value = getLoadedWorkflowName() || "Custom Workflow";
      renderMigrationReport(migrationReport);
      setStatus(loadWorkflowStatusMessage(out.path, migrationReport), true);
    } catch (e) {
      setStatus(`鍔犺浇澶辫触: ${e}`, false);
    }
  }

  return {
    exportJson,
    saveFlow,
    loadFlow,
  };
}

export { createWorkflowFlowIoUi };
