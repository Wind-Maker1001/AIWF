import { NODE_CONFIG_TEMPLATES_CORE_ANALYTICS } from "./defaults-templates-core-analytics.js";
import { NODE_CONFIG_TEMPLATES_CORE_DATA } from "./defaults-templates-core-data.js";

export const NODE_CONFIG_TEMPLATES_CORE = {
  ...NODE_CONFIG_TEMPLATES_CORE_DATA,
  ...NODE_CONFIG_TEMPLATES_CORE_ANALYTICS,
};
