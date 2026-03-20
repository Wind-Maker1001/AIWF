import {
  buildBaseRunPayload,
  mergeRunPayload,
} from "./run-payload-support.js";

function createWorkflowRunPayloadUi(els, deps = {}) {
  const {
    store,
    sandboxDedupWindowSec = () => 600,
  } = deps;

  function graphPayload() {
    store.setWorkflowName(els.workflowName.value);
    return store.exportGraph();
  }

  function runPayload(extra = {}) {
    const graph = graphPayload();
    const base = buildBaseRunPayload(els, graph, sandboxDedupWindowSec());
    return mergeRunPayload(base, extra);
  }

  return {
    graphPayload,
    runPayload,
  };
}

export { createWorkflowRunPayloadUi };
