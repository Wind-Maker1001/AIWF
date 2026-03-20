import { NODE_CONFIG_TEMPLATES_EXTENDED_AI } from "./defaults-templates-extended-ai.js";
import { NODE_CONFIG_TEMPLATES_EXTENDED_RUNTIME } from "./defaults-templates-extended-runtime.js";

export const NODE_CONFIG_TEMPLATES_EXTENDED = {
  ...NODE_CONFIG_TEMPLATES_EXTENDED_RUNTIME,
  ...NODE_CONFIG_TEMPLATES_EXTENDED_AI,
};
