import { NODE_CONFIG_TEMPLATES_CORE_DATA_PIPELINE } from "./defaults-templates-core-data-pipeline.js";
import { NODE_CONFIG_TEMPLATES_CORE_DATA_REGISTRY } from "./defaults-templates-core-data-registry.js";

export const NODE_CONFIG_TEMPLATES_CORE_DATA = {
  ...NODE_CONFIG_TEMPLATES_CORE_DATA_PIPELINE,
  ...NODE_CONFIG_TEMPLATES_CORE_DATA_REGISTRY,
};
