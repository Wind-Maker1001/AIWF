using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace AIWF.Native.Runtime;

public sealed record GovernanceManualReviewItem(
    string RunId,
    string ReviewKey,
    string WorkflowId,
    string NodeId,
    string Reviewer,
    string Comment,
    string CreatedAt,
    string DecidedAt,
    string Status,
    bool Approved)
{
    public string DisplayText =>
        string.IsNullOrWhiteSpace(WorkflowId)
            ? $"{RunId} | {ReviewKey} | {Status}"
            : $"{WorkflowId} | {RunId} | {ReviewKey} | {Status}";
}

public sealed record GovernanceWorkflowRunItem(
    string RunId,
    string WorkflowId,
    string Status,
    bool Ok,
    string Timestamp)
{
    public string DisplayText =>
        string.IsNullOrWhiteSpace(WorkflowId)
            ? $"{RunId} | {Status}"
            : $"{WorkflowId} | {RunId} | {Status}";

    public override string ToString() => DisplayText;
}

public sealed record GovernanceTimelineEntry(
    string NodeId,
    string Type,
    string Status,
    string StartedAt,
    string EndedAt,
    double Seconds)
{
    public string DisplayText => $"{NodeId} | {Type} | {Status} | {Seconds:0.###}s";

    public override string ToString() => DisplayText;
}

public sealed record GovernanceFailureSummaryEntry(
    string NodeType,
    int Failed,
    string Sample)
{
    public string DisplayText => $"{NodeType} | failed={Failed} | {Sample}";

    public override string ToString() => DisplayText;
}

public sealed record GovernanceAuditEventItem(
    string Timestamp,
    string Action,
    string DetailSummary)
{
    public string DisplayText => $"{Timestamp} | {Action} | {DetailSummary}";

    public override string ToString() => DisplayText;
}

public sealed record GovernanceQualityRuleSetItem(
    string Id,
    string Name,
    string Version,
    string Scope,
    string RulesJson)
{
    public string DisplayText => $"{Id} | {Name} | {Version} | {Scope}";

    public override string ToString() => DisplayText;
}

public sealed record GovernanceSandboxRuleVersionItem(
    string VersionId,
    string Timestamp,
    string Reason,
    string RulesJson)
{
    public string DisplayText => $"{VersionId} | {Reason} | {Timestamp}";

    public override string ToString() => DisplayText;
}

public sealed record GovernanceSandboxAutoFixState(
    string ForcedIsolationMode,
    string ForcedUntil,
    int GreenStreak,
    JsonArray ViolationEvents,
    JsonArray LastActions)
{
    public string DisplayText => $"forced={ForcedIsolationMode} | until={ForcedUntil} | green={GreenStreak}";

    public override string ToString() => DisplayText;
}

public sealed record GovernanceSandboxAutoFixActionItem(
    string Timestamp,
    int Count,
    string ActionsText)
{
    public string DisplayText => $"{Timestamp} | count={Count} | {ActionsText}";

    public override string ToString() => DisplayText;
}

public sealed record GovernanceSurfaceItem(
    string Capability,
    string RoutePrefix,
    IReadOnlyList<string> OwnedRoutePrefixes,
    string StateOwner,
    string ControlPlaneRole,
    bool LifecycleMutationAllowed)
{
    public override string ToString() => $"{Capability} | {RoutePrefix} | {StateOwner} | {ControlPlaneRole}";
}

public sealed record GovernanceControlPlaneBoundary(
    string SchemaVersion,
    string Status,
    string ControlPlaneRole,
    string GovernanceStateControlPlaneOwner,
    string JobLifecycleControlPlaneOwner,
    string OperatorSemanticsAuthorityOwner,
    string WorkflowAuthoringSurfaceOwner,
    string MetaRoute,
    IReadOnlyList<GovernanceSurfaceItem> GovernanceSurfaces)
{
    public GovernanceSurfaceItem? FindSurface(string capability) =>
        GovernanceSurfaces.FirstOrDefault(item => string.Equals(item.Capability, capability, StringComparison.Ordinal));

    public string ResolveRoutePrefix(string capability, string fallbackPrefix, string? preferredOwnedPrefix = null)
    {
        var surface = FindSurface(capability);
        if (surface is null)
        {
            return fallbackPrefix;
        }

        if (!string.IsNullOrWhiteSpace(preferredOwnedPrefix))
        {
            var owned = surface.OwnedRoutePrefixes.FirstOrDefault(prefix =>
                string.Equals(prefix, preferredOwnedPrefix, StringComparison.Ordinal));
            if (string.IsNullOrWhiteSpace(owned))
            {
                var preferredLeaf = preferredOwnedPrefix.Trim().Trim('/').Split('/').LastOrDefault() ?? string.Empty;
                if (!string.IsNullOrWhiteSpace(preferredLeaf))
                {
                    owned = surface.OwnedRoutePrefixes.FirstOrDefault(prefix =>
                    {
                        var candidateLeaf = prefix.Trim().Trim('/').Split('/').LastOrDefault() ?? string.Empty;
                        return candidateLeaf.StartsWith(preferredLeaf, StringComparison.Ordinal);
                    });
                }
            }
            if (!string.IsNullOrWhiteSpace(owned))
            {
                return owned;
            }
        }

        return string.IsNullOrWhiteSpace(surface.RoutePrefix) ? fallbackPrefix : surface.RoutePrefix;
    }
}

public sealed class GovernanceBridgeClient
{
    private readonly HttpClient _http;
    private readonly Dictionary<string, GovernanceControlPlaneBoundary> _boundaryCache = new(StringComparer.OrdinalIgnoreCase);

    public GovernanceBridgeClient(HttpClient http)
    {
        _http = http;
    }

    public async Task<GovernanceControlPlaneBoundary> GetGovernanceControlPlaneBoundaryAsync(
        string baseUrl,
        string? apiKey,
        CancellationToken cancellationToken = default)
    {
        var normalizedBase = NormalizeBaseUrl(baseUrl);
        if (_boundaryCache.TryGetValue(normalizedBase, out var cached))
        {
            return cached;
        }

        using var request = BuildRequest(HttpMethod.Get, normalizedBase, "/governance/meta/control-plane", apiKey);
        using var response = await _http.SendAsync(request, cancellationToken);
        var body = await response.Content.ReadAsStringAsync(cancellationToken);
        var root = ParseRoot(body, response.IsSuccessStatusCode);
        if (root?["boundary"] is not JsonObject boundaryObject)
        {
            throw new InvalidOperationException("governance control plane boundary response missing boundary payload");
        }

        var boundary = ParseGovernanceControlPlaneBoundary(boundaryObject);
        _boundaryCache[normalizedBase] = boundary;
        return boundary;
    }

    public async Task<IReadOnlyList<GovernanceManualReviewItem>> ListManualReviewsAsync(
        string baseUrl,
        string? apiKey,
        int limit = 120,
        CancellationToken cancellationToken = default)
    {
        var safeLimit = Math.Clamp(limit, 1, 5000);
        var routePrefix = await ResolveGovernanceRoutePrefixAsync(baseUrl, apiKey, GovernanceCapabilitiesGenerated.MANUAL_REVIEWS, cancellationToken: cancellationToken);
        using var request = BuildRequest(HttpMethod.Get, baseUrl, $"{routePrefix}?limit={safeLimit}", apiKey);
        using var response = await _http.SendAsync(request, cancellationToken);
        var body = await response.Content.ReadAsStringAsync(cancellationToken);
        return ParseManualReviewItems(body, response.IsSuccessStatusCode);
    }

    public async Task<IReadOnlyList<GovernanceManualReviewItem>> ListManualReviewHistoryAsync(
        string baseUrl,
        string? apiKey,
        int limit = 120,
        string? runId = null,
        string? reviewer = null,
        string? status = null,
        CancellationToken cancellationToken = default)
    {
        var query = new List<string> { $"limit={Math.Clamp(limit, 1, 5000)}" };
        if (!string.IsNullOrWhiteSpace(runId)) query.Add($"run_id={Uri.EscapeDataString(runId.Trim())}");
        if (!string.IsNullOrWhiteSpace(reviewer)) query.Add($"reviewer={Uri.EscapeDataString(reviewer.Trim())}");
        if (!string.IsNullOrWhiteSpace(status)) query.Add($"status={Uri.EscapeDataString(status.Trim())}");
        var routePrefix = await ResolveGovernanceRoutePrefixAsync(baseUrl, apiKey, GovernanceCapabilitiesGenerated.MANUAL_REVIEWS, cancellationToken: cancellationToken);
        using var request = BuildRequest(HttpMethod.Get, baseUrl, $"{routePrefix}/history?{string.Join("&", query)}", apiKey);
        using var response = await _http.SendAsync(request, cancellationToken);
        var body = await response.Content.ReadAsStringAsync(cancellationToken);
        return ParseManualReviewItems(body, response.IsSuccessStatusCode);
    }

    public async Task<GovernanceManualReviewItem> SubmitManualReviewAsync(
        string baseUrl,
        string? apiKey,
        string runId,
        string reviewKey,
        bool approved,
        string reviewer,
        string comment,
        CancellationToken cancellationToken = default)
    {
        var payload = new JsonObject
        {
            ["run_id"] = runId,
            ["review_key"] = reviewKey,
            ["approved"] = approved,
            ["reviewer"] = reviewer,
            ["comment"] = comment,
        };
        var routePrefix = await ResolveGovernanceRoutePrefixAsync(baseUrl, apiKey, GovernanceCapabilitiesGenerated.MANUAL_REVIEWS, cancellationToken: cancellationToken);
        using var request = BuildRequest(HttpMethod.Post, baseUrl, $"{routePrefix}/submit", apiKey);
        request.Content = new StringContent(payload.ToJsonString(), Encoding.UTF8, "application/json");
        using var response = await _http.SendAsync(request, cancellationToken);
        var body = await response.Content.ReadAsStringAsync(cancellationToken);
        return ParseManualReviewItem(body, response.IsSuccessStatusCode);
    }

    public async Task<IReadOnlyList<GovernanceWorkflowRunItem>> ListWorkflowRunsAsync(
        string baseUrl,
        string? apiKey,
        int limit = 80,
        CancellationToken cancellationToken = default)
    {
        var safeLimit = Math.Clamp(limit, 1, 5000);
        var routePrefix = await ResolveGovernanceRoutePrefixAsync(baseUrl, apiKey, GovernanceCapabilitiesGenerated.WORKFLOW_RUN_AUDIT, cancellationToken: cancellationToken);
        using var request = BuildRequest(HttpMethod.Get, baseUrl, $"{routePrefix}?limit={safeLimit}", apiKey);
        using var response = await _http.SendAsync(request, cancellationToken);
        var body = await response.Content.ReadAsStringAsync(cancellationToken);
        var root = ParseRoot(body, response.IsSuccessStatusCode);
        var array = root?["items"] as JsonArray;
        if (array is null) return Array.Empty<GovernanceWorkflowRunItem>();
        return array.OfType<JsonObject>().Select(ParseWorkflowRunItem).ToList();
    }

    public async Task<IReadOnlyList<GovernanceTimelineEntry>> GetWorkflowRunTimelineAsync(
        string baseUrl,
        string? apiKey,
        string runId,
        CancellationToken cancellationToken = default)
    {
        var routePrefix = await ResolveGovernanceRoutePrefixAsync(baseUrl, apiKey, GovernanceCapabilitiesGenerated.WORKFLOW_RUN_AUDIT, cancellationToken: cancellationToken);
        using var request = BuildRequest(HttpMethod.Get, baseUrl, $"{routePrefix}/{Uri.EscapeDataString(runId.Trim())}/timeline", apiKey);
        using var response = await _http.SendAsync(request, cancellationToken);
        var body = await response.Content.ReadAsStringAsync(cancellationToken);
        var root = ParseRoot(body, response.IsSuccessStatusCode);
        var array = root?["timeline"] as JsonArray;
        if (array is null) return Array.Empty<GovernanceTimelineEntry>();
        return array.OfType<JsonObject>().Select(ParseTimelineEntry).ToList();
    }

    public async Task<IReadOnlyList<GovernanceFailureSummaryEntry>> GetWorkflowFailureSummaryAsync(
        string baseUrl,
        string? apiKey,
        int limit = 120,
        CancellationToken cancellationToken = default)
    {
        var routePrefix = await ResolveGovernanceRoutePrefixAsync(baseUrl, apiKey, GovernanceCapabilitiesGenerated.WORKFLOW_RUN_AUDIT, cancellationToken: cancellationToken);
        using var request = BuildRequest(HttpMethod.Get, baseUrl, $"{routePrefix}/failure-summary?limit={Math.Clamp(limit, 1, 5000)}", apiKey);
        using var response = await _http.SendAsync(request, cancellationToken);
        var body = await response.Content.ReadAsStringAsync(cancellationToken);
        var root = ParseRoot(body, response.IsSuccessStatusCode);
        var byNode = root?["by_node"] as JsonObject;
        if (byNode is null) return Array.Empty<GovernanceFailureSummaryEntry>();
        var items = new List<GovernanceFailureSummaryEntry>();
        foreach (var property in byNode)
        {
            if (property.Value is not JsonObject node) continue;
            var sample = node["samples"] is JsonArray samples
                ? samples.OfType<JsonValue>().Select(v => v.GetValue<string>()).FirstOrDefault() ?? string.Empty
                : string.Empty;
            items.Add(new GovernanceFailureSummaryEntry(
                NodeType: property.Key,
                Failed: node["failed"]?.GetValue<int?>() ?? 0,
                Sample: sample));
        }
        return items.OrderByDescending(item => item.Failed).ToList();
    }

    public async Task<IReadOnlyList<GovernanceAuditEventItem>> ListWorkflowAuditEventsAsync(
        string baseUrl,
        string? apiKey,
        int limit = 80,
        string? action = null,
        CancellationToken cancellationToken = default)
    {
        var query = new List<string> { $"limit={Math.Clamp(limit, 1, 5000)}" };
        if (!string.IsNullOrWhiteSpace(action)) query.Add($"action={Uri.EscapeDataString(action.Trim())}");
        var routePrefix = await ResolveGovernanceRoutePrefixAsync(
            baseUrl,
            apiKey,
            GovernanceCapabilitiesGenerated.WORKFLOW_RUN_AUDIT,
            preferredOwnedPrefix: GovernanceCapabilitiesGenerated.WORKFLOW_RUN_AUDIT_WORKFLOW_AUDIT_EVENTS_ROUTE_PREFIX,
            cancellationToken: cancellationToken);
        using var request = BuildRequest(HttpMethod.Get, baseUrl, $"{routePrefix}?{string.Join("&", query)}", apiKey);
        using var response = await _http.SendAsync(request, cancellationToken);
        var body = await response.Content.ReadAsStringAsync(cancellationToken);
        var root = ParseRoot(body, response.IsSuccessStatusCode);
        var array = root?["items"] as JsonArray;
        if (array is null) return Array.Empty<GovernanceAuditEventItem>();
        return array.OfType<JsonObject>().Select(ParseAuditEventItem).ToList();
    }

    public async Task<IReadOnlyList<GovernanceQualityRuleSetItem>> ListQualityRuleSetsAsync(
        string baseUrl,
        string? apiKey,
        int limit = 120,
        CancellationToken cancellationToken = default)
    {
        var routePrefix = await ResolveGovernanceRoutePrefixAsync(baseUrl, apiKey, GovernanceCapabilitiesGenerated.QUALITY_RULE_SETS, cancellationToken: cancellationToken);
        using var request = BuildRequest(HttpMethod.Get, baseUrl, $"{routePrefix}?limit={Math.Clamp(limit, 1, 5000)}", apiKey);
        using var response = await _http.SendAsync(request, cancellationToken);
        var body = await response.Content.ReadAsStringAsync(cancellationToken);
        var root = ParseRoot(body, response.IsSuccessStatusCode);
        var array = root?["sets"] as JsonArray;
        if (array is null) return Array.Empty<GovernanceQualityRuleSetItem>();
        return array.OfType<JsonObject>().Select(ParseQualityRuleSetItem).ToList();
    }

    public async Task<GovernanceQualityRuleSetItem> SaveQualityRuleSetAsync(
        string baseUrl,
        string? apiKey,
        string id,
        string name,
        string version,
        string scope,
        JsonObject rules,
        CancellationToken cancellationToken = default)
    {
        var payload = new JsonObject
        {
            ["set"] = new JsonObject
            {
                ["id"] = id,
                ["name"] = name,
                ["version"] = version,
                ["scope"] = scope,
                ["rules"] = rules
            }
        };
        var routePrefix = await ResolveGovernanceRoutePrefixAsync(baseUrl, apiKey, GovernanceCapabilitiesGenerated.QUALITY_RULE_SETS, cancellationToken: cancellationToken);
        using var request = BuildRequest(HttpMethod.Put, baseUrl, $"{routePrefix}/{Uri.EscapeDataString(id)}", apiKey);
        request.Content = new StringContent(payload.ToJsonString(), Encoding.UTF8, "application/json");
        using var response = await _http.SendAsync(request, cancellationToken);
        var body = await response.Content.ReadAsStringAsync(cancellationToken);
        var root = ParseRoot(body, response.IsSuccessStatusCode);
        if (root?["set"] is JsonObject item)
        {
            return ParseQualityRuleSetItem(item);
        }

        throw new InvalidOperationException("quality rule set response missing set payload");
    }

    public async Task DeleteQualityRuleSetAsync(
        string baseUrl,
        string? apiKey,
        string id,
        CancellationToken cancellationToken = default)
    {
        var routePrefix = await ResolveGovernanceRoutePrefixAsync(baseUrl, apiKey, GovernanceCapabilitiesGenerated.QUALITY_RULE_SETS, cancellationToken: cancellationToken);
        using var request = BuildRequest(HttpMethod.Delete, baseUrl, $"{routePrefix}/{Uri.EscapeDataString(id)}", apiKey);
        using var response = await _http.SendAsync(request, cancellationToken);
        var body = await response.Content.ReadAsStringAsync(cancellationToken);
        _ = ParseRoot(body, response.IsSuccessStatusCode);
    }

    public async Task<JsonObject> GetWorkflowSandboxRulesAsync(
        string baseUrl,
        string? apiKey,
        CancellationToken cancellationToken = default)
    {
        var routePrefix = await ResolveGovernanceRoutePrefixAsync(baseUrl, apiKey, GovernanceCapabilitiesGenerated.WORKFLOW_SANDBOX_RULES, cancellationToken: cancellationToken);
        using var request = BuildRequest(HttpMethod.Get, baseUrl, routePrefix, apiKey);
        using var response = await _http.SendAsync(request, cancellationToken);
        var body = await response.Content.ReadAsStringAsync(cancellationToken);
        var root = ParseRoot(body, response.IsSuccessStatusCode);
        return (root?["rules"] as JsonObject)?.DeepClone() as JsonObject ?? new JsonObject();
    }

    public async Task<string> SaveWorkflowSandboxRulesAsync(
        string baseUrl,
        string? apiKey,
        JsonObject rules,
        string reason = "winui_governance_edit",
        CancellationToken cancellationToken = default)
    {
        var payload = new JsonObject
        {
            ["rules"] = rules,
            ["meta"] = new JsonObject
            {
                ["reason"] = reason
            }
        };
        var routePrefix = await ResolveGovernanceRoutePrefixAsync(baseUrl, apiKey, GovernanceCapabilitiesGenerated.WORKFLOW_SANDBOX_RULES, cancellationToken: cancellationToken);
        using var request = BuildRequest(HttpMethod.Put, baseUrl, routePrefix, apiKey);
        request.Content = new StringContent(payload.ToJsonString(), Encoding.UTF8, "application/json");
        using var response = await _http.SendAsync(request, cancellationToken);
        var body = await response.Content.ReadAsStringAsync(cancellationToken);
        var root = ParseRoot(body, response.IsSuccessStatusCode);
        return root?["version_id"]?.GetValue<string>() ?? string.Empty;
    }

    public async Task<IReadOnlyList<GovernanceSandboxRuleVersionItem>> ListWorkflowSandboxRuleVersionsAsync(
        string baseUrl,
        string? apiKey,
        int limit = 120,
        CancellationToken cancellationToken = default)
    {
        var routePrefix = await ResolveGovernanceRoutePrefixAsync(
            baseUrl,
            apiKey,
            GovernanceCapabilitiesGenerated.WORKFLOW_SANDBOX_RULES,
            preferredOwnedPrefix: GovernanceCapabilitiesGenerated.WORKFLOW_SANDBOX_RULES_RULE_VERSIONS_ROUTE_PREFIX,
            cancellationToken: cancellationToken);
        using var request = BuildRequest(HttpMethod.Get, baseUrl, $"{routePrefix}?limit={Math.Clamp(limit, 1, 5000)}", apiKey);
        using var response = await _http.SendAsync(request, cancellationToken);
        var body = await response.Content.ReadAsStringAsync(cancellationToken);
        var root = ParseRoot(body, response.IsSuccessStatusCode);
        var array = root?["items"] as JsonArray;
        if (array is null) return Array.Empty<GovernanceSandboxRuleVersionItem>();
        return array.OfType<JsonObject>().Select(ParseSandboxRuleVersionItem).ToList();
    }

    public async Task<string> RollbackWorkflowSandboxRuleVersionAsync(
        string baseUrl,
        string? apiKey,
        string versionId,
        CancellationToken cancellationToken = default)
    {
        var routePrefix = await ResolveGovernanceRoutePrefixAsync(
            baseUrl,
            apiKey,
            GovernanceCapabilitiesGenerated.WORKFLOW_SANDBOX_RULES,
            preferredOwnedPrefix: GovernanceCapabilitiesGenerated.WORKFLOW_SANDBOX_RULES_RULE_VERSIONS_ROUTE_PREFIX,
            cancellationToken: cancellationToken);
        using var request = BuildRequest(HttpMethod.Post, baseUrl, $"{routePrefix}/{Uri.EscapeDataString(versionId)}/rollback", apiKey);
        request.Content = new StringContent("{}", Encoding.UTF8, "application/json");
        using var response = await _http.SendAsync(request, cancellationToken);
        var body = await response.Content.ReadAsStringAsync(cancellationToken);
        var root = ParseRoot(body, response.IsSuccessStatusCode);
        return root?["version_id"]?.GetValue<string>() ?? string.Empty;
    }

    public async Task<GovernanceSandboxAutoFixState> GetWorkflowSandboxAutoFixStateAsync(
        string baseUrl,
        string? apiKey,
        CancellationToken cancellationToken = default)
    {
        var routePrefix = await ResolveGovernanceRoutePrefixAsync(baseUrl, apiKey, GovernanceCapabilitiesGenerated.WORKFLOW_SANDBOX_AUTOFIX, cancellationToken: cancellationToken);
        using var request = BuildRequest(HttpMethod.Get, baseUrl, routePrefix, apiKey);
        using var response = await _http.SendAsync(request, cancellationToken);
        var body = await response.Content.ReadAsStringAsync(cancellationToken);
        var root = ParseRoot(body, response.IsSuccessStatusCode);
        return ParseSandboxAutoFixState(root?["state"] as JsonObject);
    }

    public async Task<GovernanceSandboxAutoFixState> SaveWorkflowSandboxAutoFixStateAsync(
        string baseUrl,
        string? apiKey,
        GovernanceSandboxAutoFixState state,
        CancellationToken cancellationToken = default)
    {
        var payload = new JsonObject
        {
            ["violation_events"] = state.ViolationEvents.DeepClone(),
            ["forced_isolation_mode"] = state.ForcedIsolationMode,
            ["forced_until"] = state.ForcedUntil,
            ["last_actions"] = state.LastActions.DeepClone(),
            ["green_streak"] = Math.Max(0, state.GreenStreak),
        };
        var routePrefix = await ResolveGovernanceRoutePrefixAsync(baseUrl, apiKey, GovernanceCapabilitiesGenerated.WORKFLOW_SANDBOX_AUTOFIX, cancellationToken: cancellationToken);
        using var request = BuildRequest(HttpMethod.Put, baseUrl, routePrefix, apiKey);
        request.Content = new StringContent(payload.ToJsonString(), Encoding.UTF8, "application/json");
        using var response = await _http.SendAsync(request, cancellationToken);
        var body = await response.Content.ReadAsStringAsync(cancellationToken);
        var root = ParseRoot(body, response.IsSuccessStatusCode);
        return ParseSandboxAutoFixState(root?["state"] as JsonObject);
    }

    public async Task<IReadOnlyList<GovernanceSandboxAutoFixActionItem>> ListWorkflowSandboxAutoFixActionsAsync(
        string baseUrl,
        string? apiKey,
        int limit = 80,
        CancellationToken cancellationToken = default)
    {
        var routePrefix = await ResolveGovernanceRoutePrefixAsync(
            baseUrl,
            apiKey,
            GovernanceCapabilitiesGenerated.WORKFLOW_SANDBOX_AUTOFIX,
            preferredOwnedPrefix: GovernanceCapabilitiesGenerated.WORKFLOW_SANDBOX_AUTOFIX_AUTOFIX_ACTIONS_ROUTE_PREFIX,
            cancellationToken: cancellationToken);
        using var request = BuildRequest(HttpMethod.Get, baseUrl, $"{routePrefix}?limit={Math.Clamp(limit, 1, 5000)}", apiKey);
        using var response = await _http.SendAsync(request, cancellationToken);
        var body = await response.Content.ReadAsStringAsync(cancellationToken);
        var root = ParseRoot(body, response.IsSuccessStatusCode);
        var array = root?["items"] as JsonArray;
        if (array is null) return Array.Empty<GovernanceSandboxAutoFixActionItem>();
        return array
            .OfType<JsonObject>()
            .Select(item => new GovernanceSandboxAutoFixActionItem(
                Timestamp: item["ts"]?.GetValue<string>() ?? string.Empty,
                Count: item["count"]?.GetValue<int?>() ?? 0,
                ActionsText: item["actions"] is JsonArray actions
                    ? string.Join(",", actions.OfType<JsonValue>().Select(v => v.GetValue<string>()))
                    : string.Empty))
            .ToList();
    }

    private static IReadOnlyList<GovernanceManualReviewItem> ParseManualReviewItems(string json, bool isSuccessStatusCode)
    {
        var root = ParseRoot(json, isSuccessStatusCode);
        var array = root?["items"] as JsonArray;
        if (array is null)
        {
            return Array.Empty<GovernanceManualReviewItem>();
        }

        return array
            .OfType<JsonObject>()
            .Select(ParseManualReviewItem)
            .ToList();
    }

    private static GovernanceManualReviewItem ParseManualReviewItem(string json, bool isSuccessStatusCode)
    {
        var root = ParseRoot(json, isSuccessStatusCode);
        if (root?["item"] is JsonObject item)
        {
            return ParseManualReviewItem(item);
        }

        throw new InvalidOperationException("manual review item response missing item payload");
    }

    private static GovernanceManualReviewItem ParseManualReviewItem(JsonObject item)
    {
        return new GovernanceManualReviewItem(
            RunId: item["run_id"]?.GetValue<string>() ?? string.Empty,
            ReviewKey: item["review_key"]?.GetValue<string>() ?? string.Empty,
            WorkflowId: item["workflow_id"]?.GetValue<string>() ?? string.Empty,
            NodeId: item["node_id"]?.GetValue<string>() ?? string.Empty,
            Reviewer: item["reviewer"]?.GetValue<string>() ?? string.Empty,
            Comment: item["comment"]?.GetValue<string>() ?? string.Empty,
            CreatedAt: item["created_at"]?.GetValue<string>() ?? string.Empty,
            DecidedAt: item["decided_at"]?.GetValue<string>() ?? string.Empty,
            Status: item["status"]?.GetValue<string>() ?? string.Empty,
            Approved: item["approved"]?.GetValue<bool?>() ?? false);
    }

    private static GovernanceWorkflowRunItem ParseWorkflowRunItem(JsonObject item)
    {
        return new GovernanceWorkflowRunItem(
            RunId: item["run_id"]?.GetValue<string>() ?? string.Empty,
            WorkflowId: item["workflow_id"]?.GetValue<string>() ?? string.Empty,
            Status: item["status"]?.GetValue<string>() ?? string.Empty,
            Ok: item["ok"]?.GetValue<bool?>() ?? false,
            Timestamp: item["ts"]?.GetValue<string>() ?? string.Empty);
    }

    private static GovernanceTimelineEntry ParseTimelineEntry(JsonObject item)
    {
        return new GovernanceTimelineEntry(
            NodeId: item["node_id"]?.GetValue<string>() ?? string.Empty,
            Type: item["type"]?.GetValue<string>() ?? string.Empty,
            Status: item["status"]?.GetValue<string>() ?? string.Empty,
            StartedAt: item["started_at"]?.GetValue<string>() ?? string.Empty,
            EndedAt: item["ended_at"]?.GetValue<string>() ?? string.Empty,
            Seconds: item["seconds"]?.GetValue<double?>() ?? 0);
    }

    private static GovernanceAuditEventItem ParseAuditEventItem(JsonObject item)
    {
        var detailSummary = item["detail"]?.ToJsonString(new JsonSerializerOptions { WriteIndented = false }) ?? "{}";
        return new GovernanceAuditEventItem(
            Timestamp: item["ts"]?.GetValue<string>() ?? string.Empty,
            Action: item["action"]?.GetValue<string>() ?? string.Empty,
            DetailSummary: detailSummary);
    }

    private static GovernanceQualityRuleSetItem ParseQualityRuleSetItem(JsonObject item)
    {
        var rulesJson = item["rules"]?.ToJsonString(new JsonSerializerOptions { WriteIndented = true }) ?? "{}";
        return new GovernanceQualityRuleSetItem(
            Id: item["id"]?.GetValue<string>() ?? string.Empty,
            Name: item["name"]?.GetValue<string>() ?? string.Empty,
            Version: item["version"]?.GetValue<string>() ?? string.Empty,
            Scope: item["scope"]?.GetValue<string>() ?? string.Empty,
            RulesJson: rulesJson);
    }

    private static GovernanceSandboxRuleVersionItem ParseSandboxRuleVersionItem(JsonObject item)
    {
        var rulesJson = item["rules"]?.ToJsonString(new JsonSerializerOptions { WriteIndented = true }) ?? "{}";
        var reason = item["meta"]?["reason"]?.GetValue<string>() ?? string.Empty;
        return new GovernanceSandboxRuleVersionItem(
            VersionId: item["version_id"]?.GetValue<string>() ?? string.Empty,
            Timestamp: item["ts"]?.GetValue<string>() ?? string.Empty,
            Reason: reason,
            RulesJson: rulesJson);
    }

    private static GovernanceSandboxAutoFixState ParseSandboxAutoFixState(JsonObject? state)
    {
        return new GovernanceSandboxAutoFixState(
            ForcedIsolationMode: state?["forced_isolation_mode"]?.GetValue<string>() ?? string.Empty,
            ForcedUntil: state?["forced_until"]?.GetValue<string>() ?? string.Empty,
            GreenStreak: state?["green_streak"]?.GetValue<int?>() ?? 0,
            ViolationEvents: (state?["violation_events"] as JsonArray)?.DeepClone() as JsonArray ?? new JsonArray(),
            LastActions: (state?["last_actions"] as JsonArray)?.DeepClone() as JsonArray ?? new JsonArray());
    }

    private static GovernanceControlPlaneBoundary ParseGovernanceControlPlaneBoundary(JsonObject root)
    {
        var surfaces = root["governance_surfaces"] as JsonArray;
        return new GovernanceControlPlaneBoundary(
            SchemaVersion: root["schema_version"]?.GetValue<string>() ?? string.Empty,
            Status: root["status"]?.GetValue<string>() ?? string.Empty,
            ControlPlaneRole: root["control_plane_role"]?.GetValue<string>() ?? string.Empty,
            GovernanceStateControlPlaneOwner: root["governance_state_control_plane_owner"]?.GetValue<string>() ?? string.Empty,
            JobLifecycleControlPlaneOwner: root["job_lifecycle_control_plane_owner"]?.GetValue<string>() ?? string.Empty,
            OperatorSemanticsAuthorityOwner: root["operator_semantics_authority_owner"]?.GetValue<string>() ?? string.Empty,
            WorkflowAuthoringSurfaceOwner: root["workflow_authoring_surface_owner"]?.GetValue<string>() ?? string.Empty,
            MetaRoute: root["meta_route"]?.GetValue<string>() ?? string.Empty,
            GovernanceSurfaces: surfaces is null
                ? Array.Empty<GovernanceSurfaceItem>()
                : surfaces.OfType<JsonObject>().Select(ParseGovernanceSurfaceItem).ToList());
    }

    private static GovernanceSurfaceItem ParseGovernanceSurfaceItem(JsonObject item)
    {
        var ownedPrefixes = item["owned_route_prefixes"] is JsonArray array
            ? array.OfType<JsonValue>()
                .Select(value => value.GetValue<string>())
                .Where(value => !string.IsNullOrWhiteSpace(value))
                .ToList()
            : new List<string>();
        return new GovernanceSurfaceItem(
            Capability: item["capability"]?.GetValue<string>() ?? string.Empty,
            RoutePrefix: item["route_prefix"]?.GetValue<string>() ?? string.Empty,
            OwnedRoutePrefixes: ownedPrefixes,
            StateOwner: item["state_owner"]?.GetValue<string>() ?? string.Empty,
            ControlPlaneRole: item["control_plane_role"]?.GetValue<string>() ?? string.Empty,
            LifecycleMutationAllowed: item["lifecycle_mutation_allowed"]?.GetValue<bool?>() ?? false);
    }

    private static JsonObject? ParseRoot(string json, bool isSuccessStatusCode)
    {
        JsonObject? root = null;
        try
        {
            root = JsonNode.Parse(json) as JsonObject;
        }
        catch
        {
        }

        if (!isSuccessStatusCode)
        {
            var message = root?["error"]?.GetValue<string>() ?? json;
            throw new InvalidOperationException(string.IsNullOrWhiteSpace(message) ? "governance request failed" : message);
        }

        return root;
    }

    private static HttpRequestMessage BuildRequest(HttpMethod method, string baseUrl, string endpointPath, string? apiKey)
    {
        var normalizedBase = NormalizeBaseUrl(baseUrl);
        var request = new HttpRequestMessage(method, $"{normalizedBase}{endpointPath}");
        request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
        var token = (apiKey ?? string.Empty).Trim();
        if (!string.IsNullOrWhiteSpace(token))
        {
            request.Headers.Add("X-API-Key", token);
        }

        return request;
    }

    private static string NormalizeBaseUrl(string baseUrl) => (baseUrl ?? string.Empty).Trim().TrimEnd('/');

    private async Task<string> ResolveGovernanceRoutePrefixAsync(
        string baseUrl,
        string? apiKey,
        string capability,
        string? preferredOwnedPrefix = null,
        CancellationToken cancellationToken = default)
    {
        var boundary = await GetGovernanceControlPlaneBoundaryAsync(baseUrl, apiKey, cancellationToken);
        var surface = boundary.FindSurface(capability);
        if (surface is null)
        {
            throw new InvalidOperationException($"governance boundary missing capability: {capability}");
        }
        if (string.IsNullOrWhiteSpace(surface.RoutePrefix))
        {
            throw new InvalidOperationException($"governance boundary route prefix missing for capability: {capability}");
        }
        return boundary.ResolveRoutePrefix(capability, surface.RoutePrefix, preferredOwnedPrefix);
    }
}
