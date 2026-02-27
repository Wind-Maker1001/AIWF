const { registerBuiltinWorkflowDomains } = require("./domains/builtin_domains");

function registerBuiltinWorkflowChiplets(registry, deps) {
  registerBuiltinWorkflowDomains(registry, deps);
  return registry;
}

module.exports = {
  registerBuiltinWorkflowChiplets,
};
