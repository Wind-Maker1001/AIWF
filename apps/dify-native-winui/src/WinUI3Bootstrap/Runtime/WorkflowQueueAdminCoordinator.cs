using System.Net;
using System.Text.Json.Nodes;

namespace AIWF.Native.Runtime;

public sealed record WorkflowQueueAdminRefreshResult(
    IReadOnlyList<WorkflowQueueTaskItem> QueueItems,
    WorkflowQueueControlState Control,
    IReadOnlyList<GovernanceWorkflowRunRecordDetail> RunHistory);

public sealed record WorkflowQueueReplayResult(
    WorkflowHttpResult Response,
    bool Ok,
    string StatusText);

public sealed class WorkflowQueueAdminCoordinator
{
    private readonly WorkflowQueueStoreService _queueStoreService;
    private readonly WorkflowRunAuditStoreService _runAuditStoreService;
    private readonly RunFlowCoordinator _runFlowCoordinator;
    private readonly WorkflowRunnerAdapter _runnerAdapter;
    private readonly Func<DateTimeOffset> _now;
    private readonly SemaphoreSlim _processLock = new(1, 1);

    public WorkflowQueueAdminCoordinator(
        WorkflowQueueStoreService queueStoreService,
        WorkflowRunAuditStoreService runAuditStoreService,
        RunFlowCoordinator runFlowCoordinator,
        WorkflowRunnerAdapter runnerAdapter,
        Func<DateTimeOffset>? now = null)
    {
        _queueStoreService = queueStoreService;
        _runAuditStoreService = runAuditStoreService;
        _runFlowCoordinator = runFlowCoordinator;
        _runnerAdapter = runnerAdapter;
        _now = now ?? (() => DateTimeOffset.UtcNow);
    }

    public WorkflowQueueAdminRefreshResult Refresh(int queueLimit = 120, int runLimit = 80)
    {
        return new WorkflowQueueAdminRefreshResult(
            _queueStoreService.ListTasks(queueLimit),
            _queueStoreService.LoadControl(),
            _runAuditStoreService.ListRuns(runLimit));
    }

    public WorkflowQueueAdminRefreshResult SetPaused(bool paused, int queueLimit = 120, int runLimit = 80)
    {
        _queueStoreService.SaveControl(new WorkflowQueueControlState(paused, _queueStoreService.LoadControl().Quotas));
        return Refresh(queueLimit, runLimit);
    }

    public async Task<WorkflowQueueAdminRefreshResult> ResumeAndProcessAsync(
        string apiKey,
        int queueLimit = 120,
        int runLimit = 80,
        CancellationToken cancellationToken = default)
    {
        _queueStoreService.SaveControl(new WorkflowQueueControlState(false, _queueStoreService.LoadControl().Quotas));
        await ProcessPendingAsync(apiKey, cancellationToken);
        return Refresh(queueLimit, runLimit);
    }

    public async Task<WorkflowQueueAdminRefreshResult> EnqueueLegacyFlowAsync(
        string baseUrl,
        string apiKey,
        string owner,
        string requestedJobId,
        string flow,
        JsonObject payload,
        string label,
        CancellationToken cancellationToken = default)
    {
        var normalizedFlow = NormalizeRequired(flow, "flow is required");
        var normalizedBaseUrl = NormalizeRequired(baseUrl, "bridge base URL is required");
        var normalizedLabel = string.IsNullOrWhiteSpace(label) ? "workflow_task" : label.Trim();
        var normalizedRequestedJobId = (requestedJobId ?? string.Empty).Trim();
        _queueStoreService.EnqueueTask(new WorkflowQueueTaskItem(
            TaskId: _queueStoreService.BuildTaskId(),
            Label: normalizedLabel,
            DispatchKind: "legacy_flow",
            DispatchBaseUrl: normalizedBaseUrl,
            WorkflowId: normalizedRequestedJobId,
            Owner: (owner ?? string.Empty).Trim(),
            RequestedJobId: normalizedRequestedJobId,
            Flow: normalizedFlow,
            Priority: 100,
            Status: "queued",
            CreatedAt: _now().ToString("O"),
            StartedAt: string.Empty,
            FinishedAt: string.Empty,
            RunId: string.Empty,
            Payload: CloneJsonObject(payload),
            ResultPayload: new JsonObject(),
            Error: string.Empty));

        if (!_queueStoreService.LoadControl().Paused)
        {
            await ProcessPendingAsync(apiKey, cancellationToken);
        }

        return Refresh();
    }

    public async Task<WorkflowQueueAdminRefreshResult> EnqueueCanvasDraftAsync(
        string accelUrl,
        string apiKey,
        string requestedJobId,
        WorkflowGraphDocument document,
        string label,
        CancellationToken cancellationToken = default)
    {
        var normalizedAccelUrl = NormalizeRequired(accelUrl, "accel URL is required");
        var effectiveJobId = string.IsNullOrWhiteSpace(requestedJobId) ? document.WorkflowId : requestedJobId.Trim();
        var requestPayload = WorkflowDraftRunCoordinator.BuildRequestPayload(effectiveJobId, document);
        _queueStoreService.EnqueueTask(new WorkflowQueueTaskItem(
            TaskId: _queueStoreService.BuildTaskId(),
            Label: string.IsNullOrWhiteSpace(label) ? document.WorkflowId : label.Trim(),
            DispatchKind: "draft_canvas",
            DispatchBaseUrl: normalizedAccelUrl,
            WorkflowId: document.WorkflowId,
            Owner: string.Empty,
            RequestedJobId: effectiveJobId,
            Flow: string.Empty,
            Priority: 100,
            Status: "queued",
            CreatedAt: _now().ToString("O"),
            StartedAt: string.Empty,
            FinishedAt: string.Empty,
            RunId: string.Empty,
            Payload: requestPayload,
            ResultPayload: new JsonObject(),
            Error: string.Empty));

        if (!_queueStoreService.LoadControl().Paused)
        {
            await ProcessPendingAsync(apiKey, cancellationToken);
        }

        return Refresh();
    }

    public WorkflowQueueAdminRefreshResult CancelTask(string taskId, int queueLimit = 120, int runLimit = 80)
    {
        _queueStoreService.CancelTask(taskId);
        return Refresh(queueLimit, runLimit);
    }

    public async Task<WorkflowQueueAdminRefreshResult> RetryTaskAsync(
        string taskId,
        string apiKey,
        int queueLimit = 120,
        int runLimit = 80,
        CancellationToken cancellationToken = default)
    {
        _queueStoreService.RetryTask(taskId);
        if (!_queueStoreService.LoadControl().Paused)
        {
            await ProcessPendingAsync(apiKey, cancellationToken);
        }

        return Refresh(queueLimit, runLimit);
    }

    public async Task<WorkflowQueueReplayResult> ReplayRunAsync(
        string baseUrl,
        string accelUrl,
        string apiKey,
        GovernanceWorkflowRunRecordDetail record,
        string? nodeId = null,
        CancellationToken cancellationToken = default)
    {
        var mode = ResolveReplayMode(record);
        var replayPayload = BuildReplayPayload(record, nodeId);
        WorkflowHttpResult response = mode switch
        {
            "reference" => await _runnerAdapter.RunWorkflowReferenceAsync(
                NormalizeRequired(baseUrl, "bridge base URL is required"),
                apiKey,
                ResolveReferenceJobId(record),
                replayPayload,
                cancellationToken),
            "legacy_flow" => await _runnerAdapter.RunFlowAsync(
                NormalizeRequired(baseUrl, "bridge base URL is required"),
                apiKey,
                ResolveLegacyReplayJobId(record),
                ResolveLegacyFlow(record),
                replayPayload,
                cancellationToken),
            _ => await _runnerAdapter.PostJsonRawAsync(
                NormalizeRequired(accelUrl, "accel URL is required"),
                apiKey,
                "/operators/workflow_draft_run_v1",
                replayPayload,
                cancellationToken),
        };

        if (response.IsSuccessStatusCode)
        {
            _runAuditStoreService.AppendFromResponse(
                response.Body,
                record.RunId,
                replayPayload,
                workflowId: string.IsNullOrWhiteSpace(record.WorkflowId) ? ResolveWorkflowId(replayPayload) : record.WorkflowId,
                runRequestKind: mode == "legacy_flow" ? "legacy_flow" : mode,
                versionId: record.VersionId,
                publishedVersionId: record.PublishedVersionId,
                workflowDefinitionSource: record.WorkflowDefinitionSource);
        }

        return new WorkflowQueueReplayResult(
            response,
            response.IsSuccessStatusCode,
            response.IsSuccessStatusCode
                ? $"Run replay completed: {record.RunId}"
                : $"Run replay failed: {(int)response.StatusCode}");
    }

    public async Task ProcessPendingAsync(string apiKey, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(apiKey))
        {
            return;
        }

        await _processLock.WaitAsync(cancellationToken);
        try
        {
            while (true)
            {
                var control = _queueStoreService.LoadControl();
                if (control.Paused)
                {
                    return;
                }

                var next = _queueStoreService.ListTasks(5000)
                    .FirstOrDefault(item => string.Equals(item.Status, "queued", StringComparison.Ordinal));
                if (next is null)
                {
                    return;
                }

                var running = _queueStoreService.MarkRunning(next.TaskId);
                if (running is null)
                {
                    continue;
                }

                await ExecuteTaskAsync(running, apiKey, cancellationToken);
            }
        }
        finally
        {
            _processLock.Release();
        }
    }

    private async Task ExecuteTaskAsync(
        WorkflowQueueTaskItem task,
        string apiKey,
        CancellationToken cancellationToken)
    {
        try
        {
            WorkflowHttpResult response;
            string? effectiveJobId = null;
            switch (task.DispatchKind)
            {
                case "legacy_flow":
                {
                    var exec = await _runFlowCoordinator.ExecuteAsync(
                        task.DispatchBaseUrl,
                        apiKey,
                        task.Owner,
                        task.RequestedJobId,
                        task.Flow,
                        CloneJsonObject(task.Payload),
                        cancellationToken);
                    response = new WorkflowHttpResult(exec.StatusCode, exec.IsSuccessStatusCode, exec.Body);
                    effectiveJobId = exec.EffectiveJobId;
                    break;
                }
                case "draft_canvas":
                    response = await _runnerAdapter.PostJsonRawAsync(
                        task.DispatchBaseUrl,
                        apiKey,
                        "/operators/workflow_draft_run_v1",
                        CloneJsonObject(task.Payload),
                        cancellationToken);
                    break;
                default:
                    throw new InvalidOperationException($"unsupported queue dispatch kind: {task.DispatchKind}");
            }

            JsonObject? resultPayload = ParseJsonObject(response.Body);
            string? runId = ReadString(resultPayload?["run_id"])
                ?? ReadString((resultPayload?["data"] as JsonObject)?["run_id"])
                ?? ReadString(task.Payload["run_id"])
                ?? effectiveJobId;
            string status = ResolveTerminalStatus(response.StatusCode, response.IsSuccessStatusCode, resultPayload);
            string workflowId = ReadString(resultPayload?["workflow_id"])
                ?? task.WorkflowId
                ?? string.Empty;
            if (response.IsSuccessStatusCode)
            {
                _runAuditStoreService.AppendFromResponse(
                    response.Body,
                    runId ?? task.TaskId,
                    CloneJsonObject(task.Payload),
                    workflowId: workflowId,
                    runRequestKind: task.DispatchKind == "legacy_flow" ? "legacy_flow" : "draft",
                    workflowDefinitionSource: task.DispatchKind == "legacy_flow" ? "legacy_flow_dispatch" : "draft_inline");
            }

            _queueStoreService.MarkFinished(
                task.TaskId,
                status,
                runId,
                resultPayload,
                response.IsSuccessStatusCode ? string.Empty : ReadString(resultPayload?["error"]) ?? $"HTTP {(int)response.StatusCode}",
                workflowId: workflowId,
                requestedJobId: effectiveJobId ?? task.RequestedJobId);
        }
        catch (Exception ex)
        {
            _queueStoreService.MarkFinished(
                task.TaskId,
                "failed",
                task.RunId,
                new JsonObject
                {
                    ["error"] = ex.Message,
                    ["status"] = "failed",
                    ["ok"] = false,
                },
                ex.Message);
        }
    }

    private static string ResolveReplayMode(GovernanceWorkflowRunRecordDetail record)
    {
        var kind = (record.RunRequestKind ?? string.Empty).Trim().ToLowerInvariant();
        if (string.Equals(kind, "reference", StringComparison.Ordinal))
        {
            return "reference";
        }

        if (string.Equals(kind, "legacy_flow", StringComparison.Ordinal))
        {
            return "legacy_flow";
        }

        return "draft";
    }

    private static JsonObject BuildReplayPayload(GovernanceWorkflowRunRecordDetail record, string? nodeId)
    {
        var payload = CloneJsonObject(record.Payload);
        var normalizedNodeId = (nodeId ?? string.Empty).Trim();
        if (!string.IsNullOrWhiteSpace(normalizedNodeId))
        {
            payload["resume"] = new JsonObject
            {
                ["run_id"] = record.RunId,
                ["node_id"] = normalizedNodeId,
                ["outputs"] = CloneJsonObject(record.ResultPayload["node_outputs"] as JsonObject),
            };
        }

        return payload;
    }

    private static string ResolveLegacyFlow(GovernanceWorkflowRunRecordDetail record)
    {
        return NormalizeRequired(ReadString(record.Payload["flow"]), "run replay payload missing flow");
    }

    private static string ResolveLegacyReplayJobId(GovernanceWorkflowRunRecordDetail record)
    {
        return NormalizeRequired(
            ReadString(record.Payload["job_id"])
            ?? record.WorkflowId
            ?? record.RunId,
            "run replay payload missing job id");
    }

    private static string ResolveReferenceJobId(GovernanceWorkflowRunRecordDetail record)
    {
        return NormalizeRequired(
            ReadString(record.Payload["job_id"])
            ?? record.WorkflowId
            ?? record.RunId,
            "reference replay payload missing job id");
    }

    private static string ResolveWorkflowId(JsonObject payload)
    {
        return ReadString(payload["workflow_id"])
            ?? ReadString((payload["workflow_definition"] as JsonObject)?["workflow_id"])
            ?? string.Empty;
    }

    private static string ResolveTerminalStatus(HttpStatusCode statusCode, bool isSuccessStatusCode, JsonObject? payload)
    {
        if (isSuccessStatusCode)
        {
            return ReadString(payload?["status"]) ?? "done";
        }

        return ReadString(payload?["status"]) ?? $"failed_http_{(int)statusCode}";
    }

    private static JsonObject? ParseJsonObject(string rawJson)
    {
        try
        {
            return JsonNode.Parse(rawJson) as JsonObject;
        }
        catch
        {
            return null;
        }
    }

    private static JsonObject CloneJsonObject(JsonObject? source)
    {
        return source is null
            ? new JsonObject()
            : JsonNode.Parse(source.ToJsonString()) as JsonObject ?? new JsonObject();
    }

    private static string NormalizeRequired(string? value, string errorMessage)
    {
        var text = (value ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(text))
        {
            throw new InvalidOperationException(errorMessage);
        }

        return text;
    }

    private static string? ReadString(JsonNode? node)
    {
        if (node is JsonValue value && value.TryGetValue<string>(out var text) && !string.IsNullOrWhiteSpace(text))
        {
            return text.Trim();
        }

        return null;
    }
}
