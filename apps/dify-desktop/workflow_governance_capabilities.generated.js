const GOVERNANCE_CAPABILITY_SCHEMA_VERSION = "governance_capabilities.v1";
const GOVERNANCE_CAPABILITY_SOURCE_AUTHORITY = "apps/glue-python/aiwf/governance_surface.py";
const GOVERNANCE_CAPABILITY_ITEMS = Object.freeze([
  {
    "constant": "QUALITY_RULE_SETS",
    "capability": "quality_rule_sets",
    "schema_version": "quality_rule_set.v1",
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
    "schema_version": "workflow_sandbox_alert_rules.v1",
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
    "schema_version": "workflow_sandbox_autofix_state.v1",
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
    "schema_version": "workflow_app_registry_entry.v1",
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
    "schema_version": "workflow_version_snapshot.v1",
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
    "schema_version": "manual_review_item.v1",
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
    "schema_version": "run_baseline_entry.v1",
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
    "schema_version": "quality_rule_set.v1",
    "route_prefix": "/governance/quality-rule-sets",
    "owned_route_prefixes": [
      "/governance/quality-rule-sets"
    ]
  },
  "WORKFLOW_SANDBOX_RULES": {
    "capability": "workflow_sandbox_rules",
    "schema_version": "workflow_sandbox_alert_rules.v1",
    "route_prefix": "/governance/workflow-sandbox/rules",
    "owned_route_prefixes": [
      "/governance/workflow-sandbox/rules",
      "/governance/workflow-sandbox/rule-versions"
    ]
  },
  "WORKFLOW_SANDBOX_AUTOFIX": {
    "capability": "workflow_sandbox_autofix",
    "schema_version": "workflow_sandbox_autofix_state.v1",
    "route_prefix": "/governance/workflow-sandbox/autofix-state",
    "owned_route_prefixes": [
      "/governance/workflow-sandbox/autofix-state",
      "/governance/workflow-sandbox/autofix-actions"
    ]
  },
  "WORKFLOW_APPS": {
    "capability": "workflow_apps",
    "schema_version": "workflow_app_registry_entry.v1",
    "route_prefix": "/governance/workflow-apps",
    "owned_route_prefixes": [
      "/governance/workflow-apps"
    ]
  },
  "WORKFLOW_VERSIONS": {
    "capability": "workflow_versions",
    "schema_version": "workflow_version_snapshot.v1",
    "route_prefix": "/governance/workflow-versions",
    "owned_route_prefixes": [
      "/governance/workflow-versions"
    ]
  },
  "MANUAL_REVIEWS": {
    "capability": "manual_reviews",
    "schema_version": "manual_review_item.v1",
    "route_prefix": "/governance/manual-reviews",
    "owned_route_prefixes": [
      "/governance/manual-reviews"
    ]
  },
  "RUN_BASELINES": {
    "capability": "run_baselines",
    "schema_version": "run_baseline_entry.v1",
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

module.exports = {
  GOVERNANCE_CAPABILITY_SCHEMA_VERSION,
  GOVERNANCE_CAPABILITY_SOURCE_AUTHORITY,
  GOVERNANCE_CAPABILITY_ITEMS,
  GOVERNANCE_CAPABILITIES,
  GOVERNANCE_CAPABILITY_ROUTE_CONSTANTS,
};
