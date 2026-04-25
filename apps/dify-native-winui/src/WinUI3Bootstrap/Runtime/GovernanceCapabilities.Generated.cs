using System;

namespace AIWF.Native.Runtime;

public static class GovernanceCapabilitiesGenerated
{
    public const string SchemaVersion = "governance_capabilities.v1";
    public const string SourceAuthority = "apps/glue-python/aiwf/governance_surface.py";

    public const string QUALITY_RULE_SETS = "quality_rule_sets";
    public const string QUALITY_RULE_SETS_ROUTE_PREFIX = "/governance/quality-rule-sets";
    public const string QUALITY_RULE_SETS_QUALITY_RULE_SETS_ROUTE_PREFIX = "/governance/quality-rule-sets";
    public const string WORKFLOW_SANDBOX_RULES = "workflow_sandbox_rules";
    public const string WORKFLOW_SANDBOX_RULES_ROUTE_PREFIX = "/governance/workflow-sandbox/rules";
    public const string WORKFLOW_SANDBOX_RULES_RULES_ROUTE_PREFIX = "/governance/workflow-sandbox/rules";
    public const string WORKFLOW_SANDBOX_RULES_RULE_VERSIONS_ROUTE_PREFIX = "/governance/workflow-sandbox/rule-versions";
    public const string WORKFLOW_SANDBOX_AUTOFIX = "workflow_sandbox_autofix";
    public const string WORKFLOW_SANDBOX_AUTOFIX_ROUTE_PREFIX = "/governance/workflow-sandbox/autofix-state";
    public const string WORKFLOW_SANDBOX_AUTOFIX_AUTOFIX_STATE_ROUTE_PREFIX = "/governance/workflow-sandbox/autofix-state";
    public const string WORKFLOW_SANDBOX_AUTOFIX_AUTOFIX_ACTIONS_ROUTE_PREFIX = "/governance/workflow-sandbox/autofix-actions";
    public const string WORKFLOW_APPS = "workflow_apps";
    public const string WORKFLOW_APPS_ROUTE_PREFIX = "/governance/workflow-apps";
    public const string WORKFLOW_APPS_WORKFLOW_APPS_ROUTE_PREFIX = "/governance/workflow-apps";
    public const string WORKFLOW_VERSIONS = "workflow_versions";
    public const string WORKFLOW_VERSIONS_ROUTE_PREFIX = "/governance/workflow-versions";
    public const string WORKFLOW_VERSIONS_WORKFLOW_VERSIONS_ROUTE_PREFIX = "/governance/workflow-versions";
    public const string MANUAL_REVIEWS = "manual_reviews";
    public const string MANUAL_REVIEWS_ROUTE_PREFIX = "/governance/manual-reviews";
    public const string MANUAL_REVIEWS_MANUAL_REVIEWS_ROUTE_PREFIX = "/governance/manual-reviews";
    public const string RUN_BASELINES = "run_baselines";
    public const string RUN_BASELINES_ROUTE_PREFIX = "/governance/run-baselines";
    public const string RUN_BASELINES_RUN_BASELINES_ROUTE_PREFIX = "/governance/run-baselines";

    public static string ResolveRoutePrefix(string capability, string? preferredOwnedPrefix = null)
    {
        var normalizedCapability = (capability ?? string.Empty).Trim();
        return normalizedCapability switch
        {
            QUALITY_RULE_SETS => ResolvePreferredOrPrimary(QUALITY_RULE_SETS_ROUTE_PREFIX, preferredOwnedPrefix),
            WORKFLOW_SANDBOX_RULES => ResolvePreferredOrPrimary(WORKFLOW_SANDBOX_RULES_ROUTE_PREFIX, preferredOwnedPrefix, "/governance/workflow-sandbox/rule-versions"),
            WORKFLOW_SANDBOX_AUTOFIX => ResolvePreferredOrPrimary(WORKFLOW_SANDBOX_AUTOFIX_ROUTE_PREFIX, preferredOwnedPrefix, "/governance/workflow-sandbox/autofix-actions"),
            WORKFLOW_APPS => ResolvePreferredOrPrimary(WORKFLOW_APPS_ROUTE_PREFIX, preferredOwnedPrefix),
            WORKFLOW_VERSIONS => ResolvePreferredOrPrimary(WORKFLOW_VERSIONS_ROUTE_PREFIX, preferredOwnedPrefix),
            MANUAL_REVIEWS => ResolvePreferredOrPrimary(MANUAL_REVIEWS_ROUTE_PREFIX, preferredOwnedPrefix),
            RUN_BASELINES => ResolvePreferredOrPrimary(RUN_BASELINES_ROUTE_PREFIX, preferredOwnedPrefix),
            _ => string.Empty,
        };
    }

    public static bool CapabilityOwnsRoutePrefix(string capability, string routePrefix)
    {
        var normalizedCapability = (capability ?? string.Empty).Trim();
        return normalizedCapability switch
        {
            QUALITY_RULE_SETS => RouteBelongsToCapability(routePrefix, QUALITY_RULE_SETS_ROUTE_PREFIX),
            WORKFLOW_SANDBOX_RULES => RouteBelongsToCapability(routePrefix, WORKFLOW_SANDBOX_RULES_ROUTE_PREFIX, "/governance/workflow-sandbox/rule-versions"),
            WORKFLOW_SANDBOX_AUTOFIX => RouteBelongsToCapability(routePrefix, WORKFLOW_SANDBOX_AUTOFIX_ROUTE_PREFIX, "/governance/workflow-sandbox/autofix-actions"),
            WORKFLOW_APPS => RouteBelongsToCapability(routePrefix, WORKFLOW_APPS_ROUTE_PREFIX),
            WORKFLOW_VERSIONS => RouteBelongsToCapability(routePrefix, WORKFLOW_VERSIONS_ROUTE_PREFIX),
            MANUAL_REVIEWS => RouteBelongsToCapability(routePrefix, MANUAL_REVIEWS_ROUTE_PREFIX),
            RUN_BASELINES => RouteBelongsToCapability(routePrefix, RUN_BASELINES_ROUTE_PREFIX),
            _ => false,
        };
    }

    private static string ResolvePreferredOrPrimary(string primaryRoutePrefix, string? preferredOwnedPrefix, params string[] ownedRoutePrefixes)
    {
        var preferred = (preferredOwnedPrefix ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(preferred))
        {
            return primaryRoutePrefix;
        }

        if (string.Equals(primaryRoutePrefix, preferred, StringComparison.Ordinal))
        {
            return primaryRoutePrefix;
        }

        foreach (var routePrefix in ownedRoutePrefixes)
        {
            if (string.Equals(routePrefix, preferred, StringComparison.Ordinal))
            {
                return routePrefix;
            }
        }

        return string.Empty;
    }

    private static bool RouteBelongsToCapability(string routePrefix, string primaryRoutePrefix, params string[] ownedRoutePrefixes)
    {
        var normalizedRoutePrefix = (routePrefix ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(normalizedRoutePrefix))
        {
            return false;
        }

        if (string.Equals(primaryRoutePrefix, normalizedRoutePrefix, StringComparison.Ordinal))
        {
            return true;
        }

        foreach (var ownedRoutePrefix in ownedRoutePrefixes)
        {
            if (string.Equals(ownedRoutePrefix, normalizedRoutePrefix, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }
}
