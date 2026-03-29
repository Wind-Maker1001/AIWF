import {
  loadWorkflowStatusMessage,
  saveWorkflowName,
  stringifyWorkflowGraph,
} from "./flow-io-support.js";
import {
  combineWorkflowMigrationReports,
  formatWorkflowContractError,
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
    setStatus("已导出流程 JSON 到右侧日志区", true);
  }

  async function saveFlow() {
    try {
      const graph = graphPayload();
      const name = saveWorkflowName(els.workflowName?.value || "");
      const out = await window.aiwfDesktop.saveWorkflow(graph, name);
      if (out?.ok) {
        setStatus(`流程已保存: ${out.path}`, true);
        await refreshVersions();
        return;
      }
      if (out?.saved_local && out?.path) {
        setStatus(`流程已保存到本地: ${out.path} | 版本记录失败: ${formatWorkflowContractError(out)}`, true);
        return;
      }
      if (!out?.canceled) {
        setStatus(`保存失败: ${formatWorkflowContractError(out)}`, false);
      }
    } catch (error) {
      setStatus(`保存失败: ${formatWorkflowContractError(error)}`, false);
    }
  }

  async function loadFlow() {
    try {
      const out = await window.aiwfDesktop.loadWorkflow();
      if (!out?.ok) {
        if (!out?.canceled) setStatus(`加载失败: ${formatWorkflowContractError(out)}`, false);
        return;
      }
      const migrated = migrateLoadedWorkflowGraph(out.graph || {});
      const applied = applyLoadedWorkflowGraph(migrated.graph || {});
      const migrationReport = combineWorkflowMigrationReports(migrated, applied?.contract);
      if (els.workflowName) els.workflowName.value = getLoadedWorkflowName() || "Custom Workflow";
      renderMigrationReport(migrationReport);
      setStatus(loadWorkflowStatusMessage(out.path, migrationReport), true);
    } catch (error) {
      setStatus(`加载失败: ${formatWorkflowContractError(error)}`, false);
    }
  }

  return {
    exportJson,
    saveFlow,
    loadFlow,
  };
}

export { createWorkflowFlowIoUi };
