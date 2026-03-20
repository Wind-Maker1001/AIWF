import { NODE_CONFIG_TEMPLATES_EXTENDED_RUNTIME_STREAM } from "./defaults-templates-extended-runtime-stream.js";
import { NODE_CONFIG_TEMPLATES_EXTENDED_RUNTIME_OPS } from "./defaults-templates-extended-runtime-ops.js";
import { NODE_CONFIG_TEMPLATES_EXTENDED_RUNTIME_VECTOR } from "./defaults-templates-extended-runtime-vector.js";

export const NODE_CONFIG_TEMPLATES_EXTENDED_RUNTIME = {
  ...NODE_CONFIG_TEMPLATES_EXTENDED_RUNTIME_STREAM,
  ...NODE_CONFIG_TEMPLATES_EXTENDED_RUNTIME_OPS,
  ...NODE_CONFIG_TEMPLATES_EXTENDED_RUNTIME_VECTOR,
};
