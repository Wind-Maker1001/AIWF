import { NODE_CONFIG_TEMPLATES_CORE } from "./defaults-templates-core.js";
import { NODE_CONFIG_TEMPLATES_EXTENDED } from "./defaults-templates-extended.js";

export const NODE_CONFIG_TEMPLATES = {
  ...NODE_CONFIG_TEMPLATES_CORE,
  ...NODE_CONFIG_TEMPLATES_EXTENDED,
};
