using System.Text.Json.Nodes;

namespace AIWF.Native.Runtime;

public enum WorkflowAppPublishSourceKind
{
    Canvas,
    SqlStudio,
}

public sealed record WorkflowAppPublishSourceContext(
    WorkflowAppPublishSourceKind SourceKind,
    string AccelUrl,
    WorkflowGraphDocument Document);

public sealed record WorkflowAppPublishPreflightIssue(
    string Level,
    string Kind,
    string NodeId,
    string Message,
    string Path,
    string Code);

public sealed record WorkflowAppPublishPreflightReport(
    bool Ok,
    WorkflowAppPublishSourceKind SourceKind,
    string WorkflowId,
    IReadOnlyList<WorkflowAppPublishPreflightIssue> Issues,
    string Timestamp);

public sealed class WorkflowAppPublishPreflightCoordinator
{
    private readonly WorkflowRunnerAdapter _runnerAdapter;
    private readonly Func<string> _nowIso;

    public WorkflowAppPublishPreflightCoordinator(
        WorkflowRunnerAdapter runnerAdapter,
        Func<string>? nowIso = null)
    {
        _runnerAdapter = runnerAdapter;
        _nowIso = nowIso ?? (() => DateTimeOffset.UtcNow.ToString("O"));
    }

    public async Task<WorkflowAppPublishPreflightReport> RunPublishPreflightAsync(
        WorkflowAppPublishSourceContext sourceContext,
        string apiKey,
        CancellationToken cancellationToken = default)
    {
        var issues = new List<WorkflowAppPublishPreflightIssue>();
        var workflowDefinition = WorkflowCanvasDocumentBuilder.SerializeWorkflowDefinition(sourceContext.Document);

        try
        {
            var validation = await _runnerAdapter.PostJsonAsync(
                sourceContext.AccelUrl,
                apiKey,
                "/operators/workflow_contract_v1/validate",
                new JsonObject
                {
                    ["workflow_definition"] = workflowDefinition,
                    ["allow_version_migration"] = false,
                    ["require_non_empty_nodes"] = true,
                    ["validation_scope"] = "publish",
                },
                cancellationToken);

            var valid = validation["valid"]?.GetValue<bool?>() != false
                && !string.Equals(validation["status"]?.GetValue<string>(), "invalid", StringComparison.OrdinalIgnoreCase);
            if (!valid)
            {
                var errorItems = validation["error_items"] as JsonArray;
                if (errorItems is not null)
                {
                    foreach (var item in errorItems.OfType<JsonObject>())
                    {
                        issues.Add(new WorkflowAppPublishPreflightIssue(
                            "error",
                            "workflow_contract",
                            string.Empty,
                            item["message"]?.GetValue<string>() ?? "workflow contract invalid",
                            item["path"]?.GetValue<string>() ?? string.Empty,
                            item["code"]?.GetValue<string>() ?? string.Empty));
                    }
                }
                else
                {
                    issues.Add(new WorkflowAppPublishPreflightIssue(
                        "error",
                        "workflow_contract",
                        string.Empty,
                        validation["error"]?.GetValue<string>() ?? "workflow contract invalid",
                        string.Empty,
                        string.Empty));
                }
            }
        }
        catch (Exception ex)
        {
            issues.Add(new WorkflowAppPublishPreflightIssue(
                "error",
                "workflow_contract",
                string.Empty,
                ex.Message,
                string.Empty,
                string.Empty));
        }

        var connectionChecks = CollectConnectionChecks(sourceContext.Document);
        foreach (var connectionCheck in connectionChecks)
        {
            try
            {
                await _runnerAdapter.PostJsonAsync(
                    sourceContext.AccelUrl,
                    apiKey,
                    "/operators/data_source_browser_v1",
                    new JsonObject
                    {
                        ["source_type"] = connectionCheck.SourceType,
                        ["source"] = connectionCheck.Source,
                        ["op"] = "validate_connection",
                    },
                    cancellationToken);
            }
            catch (Exception ex)
            {
                issues.Add(new WorkflowAppPublishPreflightIssue(
                    "error",
                    "connection",
                    connectionCheck.NodeId,
                    ex.Message,
                    $"workflow.nodes[{connectionCheck.NodeId}].config.source",
                    string.Empty));
            }
        }

        return new WorkflowAppPublishPreflightReport(
            Ok: issues.All(static issue => !string.Equals(issue.Level, "error", StringComparison.OrdinalIgnoreCase)),
            SourceKind: sourceContext.SourceKind,
            WorkflowId: sourceContext.Document.WorkflowId,
            Issues: issues,
            Timestamp: _nowIso());
    }

    private static IReadOnlyList<ConnectionCheck> CollectConnectionChecks(WorkflowGraphDocument document)
    {
        var seen = new HashSet<string>(StringComparer.Ordinal);
        var result = new List<ConnectionCheck>();
        foreach (var node in document.Nodes)
        {
            if (!string.Equals(node.Type, "load_rows_v3", StringComparison.Ordinal))
            {
                continue;
            }

            var sourceType = node.Config["source_type"]?.GetValue<string>() ?? string.Empty;
            var source = node.Config["source"]?.GetValue<string>() ?? string.Empty;
            if (string.IsNullOrWhiteSpace(sourceType) || string.IsNullOrWhiteSpace(source))
            {
                result.Add(new ConnectionCheck(node.Id, sourceType, source));
                continue;
            }

            var key = $"{sourceType}::{source}";
            if (seen.Add(key))
            {
                result.Add(new ConnectionCheck(node.Id, sourceType, source));
            }
        }

        return result;
    }

    private sealed record ConnectionCheck(string NodeId, string SourceType, string Source);
}
