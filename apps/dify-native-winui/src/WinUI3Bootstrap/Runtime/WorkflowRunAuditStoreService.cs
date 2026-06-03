using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace AIWF.Native.Runtime;

public sealed class WorkflowRunAuditStoreService
{
    private readonly string _runHistoryPath;
    private readonly string? _legacyRunHistoryPath;

    public WorkflowRunAuditStoreService(string? runHistoryPath = null)
    {
        if (string.IsNullOrWhiteSpace(runHistoryPath))
        {
            var root = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData), "AIWF");
            _runHistoryPath = Path.Combine(root, "workflow_store", "run_history.jsonl");
            _legacyRunHistoryPath = Path.Combine(root, "workflow-run-history.jsonl");
        }
        else
        {
            _runHistoryPath = Path.GetFullPath(runHistoryPath);
            _legacyRunHistoryPath = null;
        }
    }

    public string RunHistoryPath => _runHistoryPath;

    public GovernanceWorkflowRunRecordDetail? AppendFromResponse(
        string rawJson,
        string fallbackRunId,
        JsonObject? payload = null,
        string? workflowId = null,
        string? runRequestKind = null,
        string? versionId = null,
        string? publishedVersionId = null,
        string? workflowDefinitionSource = null)
    {
        var record = TryCreateRecord(
            rawJson,
            fallbackRunId,
            payload,
            workflowId,
            runRequestKind,
            versionId,
            publishedVersionId,
            workflowDefinitionSource);
        if (record is null)
        {
            return null;
        }

        var dir = Path.GetDirectoryName(_runHistoryPath) ?? ".";
        Directory.CreateDirectory(dir);
        File.AppendAllText(_runHistoryPath, SerializeRecord(record) + Environment.NewLine, Encoding.UTF8);
        return record;
    }

    public IReadOnlyList<GovernanceWorkflowRunRecordDetail> ListRuns(int limit = 200)
    {
        var candidateFiles = new List<string>();
        if (File.Exists(_runHistoryPath))
        {
            candidateFiles.Add(_runHistoryPath);
        }

        if (!string.IsNullOrWhiteSpace(_legacyRunHistoryPath)
            && File.Exists(_legacyRunHistoryPath)
            && !string.Equals(_legacyRunHistoryPath, _runHistoryPath, StringComparison.OrdinalIgnoreCase))
        {
            candidateFiles.Add(_legacyRunHistoryPath);
        }

        if (candidateFiles.Count == 0)
        {
            return Array.Empty<GovernanceWorkflowRunRecordDetail>();
        }

        var safeLimit = Math.Clamp(limit, 1, 5000);
        var seen = new HashSet<string>(StringComparer.Ordinal);
        var items = new List<GovernanceWorkflowRunRecordDetail>();
        foreach (var filePath in candidateFiles)
        {
            var lines = File.ReadAllLines(filePath, Encoding.UTF8)
                .Where(static line => !string.IsNullOrWhiteSpace(line))
                .Reverse();
            foreach (var line in lines)
            {
                if (items.Count >= safeLimit)
                {
                    return items;
                }

                try
                {
                    if (JsonNode.Parse(line) is not JsonObject root)
                    {
                        continue;
                    }

                    var parsed = ParseRecord(root);
                    if (!seen.Add(parsed.RunId))
                    {
                        continue;
                    }

                    items.Add(parsed);
                }
                catch
                {
                }
            }
        }

        return items;
    }

    public GovernanceWorkflowRunRecordDetail? GetRun(string runId)
    {
        var target = (runId ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(target))
        {
            return null;
        }

        return ListRuns(5000).FirstOrDefault(item => string.Equals(item.RunId, target, StringComparison.Ordinal));
    }

    internal static GovernanceWorkflowRunRecordDetail? TryCreateRecord(
        string rawJson,
        string fallbackRunId,
        JsonObject? payload,
        string? workflowId,
        string? runRequestKind,
        string? versionId,
        string? publishedVersionId,
        string? workflowDefinitionSource)
    {
        JsonObject? root;
        try
        {
            root = JsonNode.Parse(rawJson) as JsonObject;
        }
        catch
        {
            return null;
        }

        if (root is null)
        {
            return null;
        }

        var resolvedRunId = ReadString(root["run_id"])
            ?? ReadString(root["job_id"])
            ?? (fallbackRunId ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(resolvedRunId))
        {
            return null;
        }

        var clonedPayload = CloneJsonObject(payload);
        var resolvedWorkflowId = ReadString(root["workflow_id"])
            ?? (workflowId ?? string.Empty).Trim()
            ?? string.Empty;
        var resolvedRunRequestKind = !string.IsNullOrWhiteSpace(runRequestKind)
            ? runRequestKind.Trim()
            : ReadString(clonedPayload["run_request_kind"]) ?? string.Empty;
        var resolvedVersionId = !string.IsNullOrWhiteSpace(versionId)
            ? versionId.Trim()
            : ReadString(clonedPayload["version_id"]) ?? string.Empty;
        var resolvedPublishedVersionId = !string.IsNullOrWhiteSpace(publishedVersionId)
            ? publishedVersionId.Trim()
            : ReadString(clonedPayload["published_version_id"]) ?? string.Empty;
        var resolvedDefinitionSource = !string.IsNullOrWhiteSpace(workflowDefinitionSource)
            ? workflowDefinitionSource.Trim()
            : ReadString(clonedPayload["workflow_definition_source"]) ?? string.Empty;
        var status = ReadString(root["status"]) ?? (root["ok"]?.GetValue<bool?>() == true ? "done" : string.Empty);
        var ok = root["ok"]?.GetValue<bool?>() ?? false;
        var ts = ReadString(root["ts"]) ?? DateTimeOffset.UtcNow.ToString("O");
        return new GovernanceWorkflowRunRecordDetail(
            RunId: resolvedRunId,
            WorkflowId: resolvedWorkflowId,
            Status: status,
            Ok: ok,
            Timestamp: ts,
            RunRequestKind: resolvedRunRequestKind,
            VersionId: resolvedVersionId,
            PublishedVersionId: resolvedPublishedVersionId,
            WorkflowDefinitionSource: resolvedDefinitionSource,
            Payload: clonedPayload,
            Steps: ParseSteps(root),
            ResultPayload: CloneJsonObject(root));
    }

    private static IReadOnlyList<GovernanceWorkflowRunStepItem> ParseSteps(JsonObject root)
    {
        if (root["node_runs"] is JsonArray nodeRuns)
        {
            return nodeRuns
                .OfType<JsonObject>()
                .Select(node =>
                    new GovernanceWorkflowRunStepItem(
                        StepId: ReadString(node["id"]) ?? string.Empty,
                        Status: ReadString(node["status"]) ?? string.Empty,
                        StartedAt: ReadString(node["started_at"]) ?? string.Empty,
                        EndedAt: ReadString(node["ended_at"]) ?? string.Empty,
                        Seconds: ReadDouble(node["seconds"]),
                        Error: ReadString(node["error"]) ?? ReadString((node["output"] as JsonObject)?["detail"]) ?? string.Empty))
                .ToArray();
        }

        if (root["steps"] is JsonArray steps)
        {
            return steps
                .OfType<JsonObject>()
                .Select(step =>
                    new GovernanceWorkflowRunStepItem(
                        StepId: ReadString(step["step_id"]) ?? string.Empty,
                        Status: ReadString(step["status"]) ?? string.Empty,
                        StartedAt: ReadString(step["started_at"]) ?? string.Empty,
                        EndedAt: ReadString(step["ended_at"]) ?? string.Empty,
                        Seconds: ComputeSeconds(ReadString(step["started_at"]) ?? string.Empty, ReadString(step["ended_at"]) ?? string.Empty),
                        Error: ReadString(step["error"]) ?? string.Empty))
                .ToArray();
        }

        return Array.Empty<GovernanceWorkflowRunStepItem>();
    }

    private static string SerializeRecord(GovernanceWorkflowRunRecordDetail record)
    {
        var payload = new JsonObject
        {
            ["run_id"] = record.RunId,
            ["workflow_id"] = record.WorkflowId,
            ["status"] = record.Status,
            ["ok"] = record.Ok,
            ["ts"] = record.Timestamp,
            ["run_request_kind"] = record.RunRequestKind,
            ["version_id"] = record.VersionId,
            ["published_version_id"] = record.PublishedVersionId,
            ["workflow_definition_source"] = record.WorkflowDefinitionSource,
            ["payload"] = CloneJsonObject(record.Payload),
            ["result"] = CloneJsonObject(record.ResultPayload),
        };
        return payload.ToJsonString();
    }

    private static GovernanceWorkflowRunRecordDetail ParseRecord(JsonObject root)
    {
        var resultPayload = root["result"] as JsonObject ?? new JsonObject();
        return new GovernanceWorkflowRunRecordDetail(
            RunId: ReadString(root["run_id"]) ?? string.Empty,
            WorkflowId: ReadString(root["workflow_id"]) ?? string.Empty,
            Status: ReadString(root["status"]) ?? string.Empty,
            Ok: root["ok"]?.GetValue<bool?>() ?? false,
            Timestamp: ReadString(root["ts"]) ?? string.Empty,
            RunRequestKind: ReadString(root["run_request_kind"]) ?? string.Empty,
            VersionId: ReadString(root["version_id"]) ?? string.Empty,
            PublishedVersionId: ReadString(root["published_version_id"]) ?? string.Empty,
            WorkflowDefinitionSource: ReadString(root["workflow_definition_source"]) ?? string.Empty,
            Payload: CloneJsonObject(root["payload"] as JsonObject),
            Steps: ParseSteps(resultPayload),
            ResultPayload: CloneJsonObject(resultPayload));
    }

    private static JsonObject CloneJsonObject(JsonObject? source)
    {
        return source is null
            ? new JsonObject()
            : JsonNode.Parse(source.ToJsonString()) as JsonObject ?? new JsonObject();
    }

    private static string? ReadString(JsonNode? node)
    {
        if (node is JsonValue value && value.TryGetValue<string>(out var text) && !string.IsNullOrWhiteSpace(text))
        {
            return text.Trim();
        }

        return null;
    }

    private static double ReadDouble(JsonNode? value)
    {
        return value is JsonValue jsonValue && jsonValue.TryGetValue<double>(out var doubleValue)
            ? doubleValue
            : value is JsonValue intValue && intValue.TryGetValue<int>(out var parsedInt)
                ? parsedInt
                : value is JsonValue longValue && longValue.TryGetValue<long>(out var parsedLong)
                    ? parsedLong
                    : 0;
    }

    private static double ComputeSeconds(string startedAt, string endedAt)
    {
        if (!DateTimeOffset.TryParse(startedAt, out var started)
            || !DateTimeOffset.TryParse(endedAt, out var ended))
        {
            return 0;
        }

        return Math.Max(0, Math.Round((ended - started).TotalSeconds, 3));
    }
}
