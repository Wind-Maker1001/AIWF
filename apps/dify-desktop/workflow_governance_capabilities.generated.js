const GOVERNANCE_CAPABILITY_SCHEMA_VERSION = "governance_capabilities.v1";
const GOVERNANCE_CAPABILITY_SOURCE_AUTHORITY = "apps/glue-python/aiwf/governance_surface.py";
const GOVERNANCE_CAPABILITY_ITEMS = Object.freeze([
  {
    "constant": "QUALITY_RULE_SETS",
    "capability": "quality_rule_sets",
    "route_prefix": "/governance/quality-rule-sets",
    "owned_route_prefixes": [
      "/governance/quality-rule-sets"
    ],
    "route_constant_map": {
      "PRIMARY": "/governance/quality-rule-sets",
      "QUALITY_RULE_SETS": "/governance/quality-rule-sets"
    }
  },
  {
    "constant": "WORKFLOW_SANDBOX_RULES",
    "capability": "workflow_sandbox_rules",
    "route_prefix": "/governance/workflow-sandbox/rules",
    "owned_route_prefixes": [
      "/governance/workflow-sandbox/rules",
      "/governance/workflow-sandbox/rule-versions"
    ],
    "route_constant_map": {
      "PRIMARY": "/governance/workflow-sandbox/rules",
      "RULES": "/governance/workflow-sandbox/rules",
      "RULE_VERSIONS": "/governance/workflow-sandbox/rule-versions"
    }
  },
  {
    "constant": "WORKFLOW_SANDBOX_AUTOFIX",
    "capability": "workflow_sandbox_autofix",
    "route_prefix": "/governance/workflow-sandbox/autofix-state",
    "owned_route_prefixes": [
      "/governance/workflow-sandbox/autofix-state",
      "/governance/workflow-sandbox/autofix-actions"
    ],
    "route_constant_map": {
      "PRIMARY": "/governance/workflow-sandbox/autofix-state",
      "AUTOFIX_STATE": "/governance/workflow-sandbox/autofix-state",
      "AUTOFIX_ACTIONS": "/governance/workflow-sandbox/autofix-actions"
    }
  },
  {
    "constant": "WORKFLOW_APPS",
    "capability": "workflow_apps",
    "route_prefix": "/governance/workflow-apps",
    "owned_route_prefixes": [
      "/governance/workflow-apps"
    ],
    "route_constant_map": {
      "PRIMARY": "/governance/workflow-apps",
      "WORKFLOW_APPS": "/governance/workflow-apps"
    }
  },
  {
    "constant": "WORKFLOW_VERSIONS",
    "capability": "workflow_versions",
    "route_prefix": "/governance/workflow-versions",
    "owned_route_prefixes": [
      "/governance/workflow-versions"
    ],
    "route_constant_map": {
      "PRIMARY": "/governance/workflow-versions",
      "WORKFLOW_VERSIONS": "/governance/workflow-versions"
    }
  },
  {
    "constant": "MANUAL_REVIEWS",
    "capability": "manual_reviews",
    "route_prefix": "/governance/manual-reviews",
    "owned_route_prefixes": [
      "/governance/manual-reviews"
    ],
    "route_constant_map": {
      "PRIMARY": "/governance/manual-reviews",
      "MANUAL_REVIEWS": "/governance/manual-reviews"
    }
  },
  {
    "constant": "RUN_BASELINES",
    "capability": "run_baselines",
    "route_prefix": "/governance/run-baselines",
    "owned_route_prefixes": [
      "/governance/run-baselines"
    ],
    "route_constant_map": {
      "PRIMARY": "/governance/run-baselines",
      "RUN_BASELINES": "/governance/run-baselines"
    }
  }
]);
const GOVERNANCE_CAPABILITIES = Object.freeze({
  "QUALITY_RULE_SETS": {
    "capability": "quality_rule_sets",
    "route_prefix": "/governance/quality-rule-sets",
    "owned_route_prefixes": [
      "/governance/quality-rule-sets"
    ]
  },
  "WORKFLOW_SANDBOX_RULES": {
    "capability": "workflow_sandbox_rules",
    "route_prefix": "/governance/workflow-sandbox/rules",
    "owned_route_prefixes": [
      "/governance/workflow-sandbox/rules",
      "/governance/workflow-sandbox/rule-versions"
    ]
  },
  "WORKFLOW_SANDBOX_AUTOFIX": {
    "capability": "workflow_sandbox_autofix",
    "route_prefix": "/governance/workflow-sandbox/autofix-state",
    "owned_route_prefixes": [
      "/governance/workflow-sandbox/autofix-state",
      "/governance/workflow-sandbox/autofix-actions"
    ]
  },
  "WORKFLOW_APPS": {
    "capability": "workflow_apps",
    "route_prefix": "/governance/workflow-apps",
    "owned_route_prefixes": [
      "/governance/workflow-apps"
    ]
  },
  "WORKFLOW_VERSIONS": {
    "capability": "workflow_versions",
    "route_prefix": "/governance/workflow-versions",
    "owned_route_prefixes": [
      "/governance/workflow-versions"
    ]
  },
  "MANUAL_REVIEWS": {
    "capability": "manual_reviews",
    "route_prefix": "/governance/manual-reviews",
    "owned_route_prefixes": [
      "/governance/manual-reviews"
    ]
  },
  "RUN_BASELINES": {
    "capability": "run_baselines",
    "route_prefix": "/governance/run-baselines",
    "owned_route_prefixes": [
      "/governance/run-baselines"
    ]
  }
});
const GOVERNANCE_CAPABILITY_ROUTE_CONSTANTS = Object.freeze({
  "QUALITY_RULE_SETS": {
    "PRIMARY": "/governance/quality-rule-sets",
    "QUALITY_RULE_SETS": "/governance/quality-rule-sets"
  },
  "WORKFLOW_SANDBOX_RULES": {
    "PRIMARY": "/governance/workflow-sandbox/rules",
    "RULES": "/governance/workflow-sandbox/rules",
    "RULE_VERSIONS": "/governance/workflow-sandbox/rule-versions"
  },
  "WORKFLOW_SANDBOX_AUTOFIX": {
    "PRIMARY": "/governance/workflow-sandbox/autofix-state",
    "AUTOFIX_STATE": "/governance/workflow-sandbox/autofix-state",
    "AUTOFIX_ACTIONS": "/governance/workflow-sandbox/autofix-actions"
  },
  "WORKFLOW_APPS": {
    "PRIMARY": "/governance/workflow-apps",
    "WORKFLOW_APPS": "/governance/workflow-apps"
  },
  "WORKFLOW_VERSIONS": {
    "PRIMARY": "/governance/workflow-versions",
    "WORKFLOW_VERSIONS": "/governance/workflow-versions"
  },
  "MANUAL_REVIEWS": {
    "PRIMARY": "/governance/manual-reviews",
    "MANUAL_REVIEWS": "/governance/manual-reviews"
  },
  "RUN_BASELINES": {
    "PRIMARY": "/governance/run-baselines",
    "RUN_BASELINES": "/governance/run-baselines"
  }
});
const GOVERNANCE_CAPABILITY_BY_NAME = Object.freeze({
  "quality_rule_sets": {
    "constant": "QUALITY_RULE_SETS",
    "capability": "quality_rule_sets",
    "route_prefix": "/governance/quality-rule-sets",
    "owned_route_prefixes": [
      "/governance/quality-rule-sets"
    ],
    "route_constant_map": {
      "PRIMARY": "/governance/quality-rule-sets",
      "QUALITY_RULE_SETS": "/governance/quality-rule-sets"
    }
  },
  "workflow_sandbox_rules": {
    "constant": "WORKFLOW_SANDBOX_RULES",
    "capability": "workflow_sandbox_rules",
    "route_prefix": "/governance/workflow-sandbox/rules",
    "owned_route_prefixes": [
      "/governance/workflow-sandbox/rules",
      "/governance/workflow-sandbox/rule-versions"
    ],
    "route_constant_map": {
      "PRIMARY": "/governance/workflow-sandbox/rules",
      "RULES": "/governance/workflow-sandbox/rules",
      "RULE_VERSIONS": "/governance/workflow-sandbox/rule-versions"
    }
  },
  "workflow_sandbox_autofix": {
    "constant": "WORKFLOW_SANDBOX_AUTOFIX",
    "capability": "workflow_sandbox_autofix",
    "route_prefix": "/governance/workflow-sandbox/autofix-state",
    "owned_route_prefixes": [
      "/governance/workflow-sandbox/autofix-state",
      "/governance/workflow-sandbox/autofix-actions"
    ],
    "route_constant_map": {
      "PRIMARY": "/governance/workflow-sandbox/autofix-state",
      "AUTOFIX_STATE": "/governance/workflow-sandbox/autofix-state",
      "AUTOFIX_ACTIONS": "/governance/workflow-sandbox/autofix-actions"
    }
  },
  "workflow_apps": {
    "constant": "WORKFLOW_APPS",
    "capability": "workflow_apps",
    "route_prefix": "/governance/workflow-apps",
    "owned_route_prefixes": [
      "/governance/workflow-apps"
    ],
    "route_constant_map": {
      "PRIMARY": "/governance/workflow-apps",
      "WORKFLOW_APPS": "/governance/workflow-apps"
    }
  },
  "workflow_versions": {
    "constant": "WORKFLOW_VERSIONS",
    "capability": "workflow_versions",
    "route_prefix": "/governance/workflow-versions",
    "owned_route_prefixes": [
      "/governance/workflow-versions"
    ],
    "route_constant_map": {
      "PRIMARY": "/governance/workflow-versions",
      "WORKFLOW_VERSIONS": "/governance/workflow-versions"
    }
  },
  "manual_reviews": {
    "constant": "MANUAL_REVIEWS",
    "capability": "manual_reviews",
    "route_prefix": "/governance/manual-reviews",
    "owned_route_prefixes": [
      "/governance/manual-reviews"
    ],
    "route_constant_map": {
      "PRIMARY": "/governance/manual-reviews",
      "MANUAL_REVIEWS": "/governance/manual-reviews"
    }
  },
  "run_baselines": {
    "constant": "RUN_BASELINES",
    "capability": "run_baselines",
    "route_prefix": "/governance/run-baselines",
    "owned_route_prefixes": [
      "/governance/run-baselines"
    ],
    "route_constant_map": {
      "PRIMARY": "/governance/run-baselines",
      "RUN_BASELINES": "/governance/run-baselines"
    }
  }
});

function normalizeGovernanceCapability(capability) {
  return String(capability || "").trim();
}

function getGovernanceCapabilityItem(capability) {
  const normalizedCapability = normalizeGovernanceCapability(capability);
  return normalizedCapability ? (GOVERNANCE_CAPABILITY_BY_NAME[normalizedCapability] || null) : null;
}

function resolveGovernanceCapabilityRoutePrefix(capability, preferredOwnedPrefix = "") {
  const item = getGovernanceCapabilityItem(capability);
  if (!item) return "";
  const preferred = String(preferredOwnedPrefix || "").trim();
  const primary = String(item.route_prefix || "").trim();
  if (preferred) {
    if (primary === preferred) return primary;
    const owned = Array.isArray(item.owned_route_prefixes) ? item.owned_route_prefixes : [];
    return owned.find((entry) => String(entry || "").trim() === preferred) || "";
  }
  return primary;
}

function governanceCapabilityOwnsRoutePrefix(capability, routePrefix) {
  const item = getGovernanceCapabilityItem(capability);
  const normalizedRoutePrefix = String(routePrefix || "").trim();
  if (!item || !normalizedRoutePrefix) return false;
  if (String(item.route_prefix || "").trim() === normalizedRoutePrefix) return true;
  const owned = Array.isArray(item.owned_route_prefixes) ? item.owned_route_prefixes : [];
  return owned.some((entry) => String(entry || "").trim() === normalizedRoutePrefix);
}

module.exports = {
  GOVERNANCE_CAPABILITY_SCHEMA_VERSION,
  GOVERNANCE_CAPABILITY_SOURCE_AUTHORITY,
  GOVERNANCE_CAPABILITY_ITEMS,
  GOVERNANCE_CAPABILITIES,
  GOVERNANCE_CAPABILITY_ROUTE_CONSTANTS,
  GOVERNANCE_CAPABILITY_BY_NAME,
  getGovernanceCapabilityItem,
  resolveGovernanceCapabilityRoutePrefix,
  governanceCapabilityOwnsRoutePrefix,
};
