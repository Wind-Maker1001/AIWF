import { createWorkflowSupportUi } from "./support-ui.js";
import { createWorkflowRunPayloadUi } from "./run-payload-ui.js";
import { createWorkflowAppFormUi } from "./app-form-ui.js";
import { createWorkflowConfigUi } from "./config-ui.js";
import { createWorkflowTemplateUi } from "./template-ui.js";
import {
  buildAppFormUiDeps,
  buildConfigUiDeps,
  buildRunPayloadUiDeps,
  buildSupportUiDeps,
  buildTemplateUiDeps,
} from "./app-core-services-support.js";

function createWorkflowCoreServices(ctx = {}) {
  const { els } = ctx;

  const supportUi = createWorkflowSupportUi(els, buildSupportUiDeps(ctx));
  const runPayloadUi = createWorkflowRunPayloadUi(els, buildRunPayloadUiDeps({ ...ctx, supportUi }));
  const appFormUi = createWorkflowAppFormUi(els, buildAppFormUiDeps(ctx));
  const configUi = createWorkflowConfigUi(els, buildConfigUiDeps(ctx));
  const templateUi = createWorkflowTemplateUi(els, buildTemplateUiDeps({ ...ctx, runPayloadUi }));

  return {
    ...supportUi,
    ...runPayloadUi,
    ...appFormUi,
    ...configUi,
    ...templateUi,
  };
}

export { createWorkflowCoreServices };
