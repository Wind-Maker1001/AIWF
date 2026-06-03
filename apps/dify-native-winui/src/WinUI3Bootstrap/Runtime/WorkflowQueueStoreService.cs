using System.Text.Json;
using System.Text.Json.Nodes;

namespace AIWF.Native.Runtime;

public sealed record WorkflowQueueControlState(
    bool Paused,
    IReadOnlyDictionary<string, int> Quotas)
{
    public string DisplayText
    {
        get
        {
            var quotaText = Quotas.Count == 0
                ? "default"
                : string.Join(", ", Quotas.OrderBy(static item => item.Key, StringComparer.Ordinal)
                    .Select(static item => $"{item.Key}:{item.Value}"));
            return $"Queue: {(Paused ? "paused" : "running")} | quotas: {quotaText}";
        }
    }

    public override string ToString() => DisplayText;
}

public sealed record WorkflowQueueTaskItem(
    string TaskId,
    string Label,
    string DispatchKind,
    string DispatchBaseUrl,
    string WorkflowId,
    string Owner,
    string RequestedJobId,
    string Flow,
    int Priority,
    string Status,
    string CreatedAt,
    string StartedAt,
    string FinishedAt,
    string RunId,
    JsonObject Payload,
    JsonObject ResultPayload,
    string Error)
{
    public string DisplayText
    {
        get
        {
            var workflowText = string.IsNullOrWhiteSpace(WorkflowId) ? "-" : WorkflowId;
            var shortTaskId = TaskId.Length <= 8 ? TaskId : TaskId[..8];
            return $"{Label} | {workflowText} | {shortTaskId} | {Status}";
        }
    }

    public override string ToString() => DisplayText;
}

public sealed class WorkflowQueueStoreService
{
    private readonly string _queuePath;
    private readonly string _controlPath;
    private readonly Func<DateTimeOffset> _now;
    private readonly Func<string> _randomHex;

    public WorkflowQueueStoreService(
        string? queuePath = null,
        string? controlPath = null,
        Func<DateTimeOffset>? now = null,
        Func<string>? randomHex = null)
    {
        var storeDir = Path.Combine(
            Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
            "AIWF",
            "workflow_store");
        _queuePath = string.IsNullOrWhiteSpace(queuePath)
            ? Path.Combine(storeDir, "workflow_task_queue.json")
            : Path.GetFullPath(queuePath);
        _controlPath = string.IsNullOrWhiteSpace(controlPath)
            ? Path.Combine(storeDir, "workflow_queue_control.json")
            : Path.GetFullPath(controlPath);
        _now = now ?? (() => DateTimeOffset.UtcNow);
        _randomHex = randomHex ?? (() => Guid.NewGuid().ToString("N")[..8]);
    }

    public string QueuePath => _queuePath;

    public string ControlPath => _controlPath;

    public IReadOnlyList<WorkflowQueueTaskItem> ListTasks(int limit = 200)
    {
        var safeLimit = Math.Clamp(limit, 1, 5000);
        return LoadAllTasks()
            .OrderBy(static item => item.Priority)
            .ThenBy(static item => item.CreatedAt, StringComparer.Ordinal)
            .Take(safeLimit)
            .ToArray();
    }

    public WorkflowQueueControlState LoadControl()
    {
        var root = ReadJsonObject(_controlPath);
        return NormalizeControl(root);
    }

    public WorkflowQueueControlState SaveControl(WorkflowQueueControlState control)
    {
        var normalized = NormalizeControl(new JsonObject
        {
            ["paused"] = control.Paused,
            ["quotas"] = SerializeQuotas(control.Quotas),
        });
        WriteJsonObject(_controlPath, new JsonObject
        {
            ["paused"] = normalized.Paused,
            ["quotas"] = SerializeQuotas(normalized.Quotas),
        });
        return normalized;
    }

    public WorkflowQueueTaskItem EnqueueTask(WorkflowQueueTaskItem item)
    {
        var normalized = NormalizeTask(ToJsonObject(item), LoadAllTasks().Count);
        var items = LoadAllTasks();
        items.Add(normalized);
        SaveAllTasks(items);
        return normalized;
    }

    public WorkflowQueueTaskItem? MarkRunning(string taskId)
    {
        return UpdateTask(taskId, item =>
        {
            var status = NormalizeStatus(item.Status);
            if (!string.Equals(status, "queued", StringComparison.Ordinal))
            {
                return item;
            }

            return item with
            {
                Status = "running",
                StartedAt = _now().ToString("O"),
                FinishedAt = string.Empty,
                Error = string.Empty,
            };
        });
    }

    public WorkflowQueueTaskItem? MarkFinished(
        string taskId,
        string status,
        string? runId,
        JsonObject? resultPayload,
        string? error,
        string? workflowId = null,
        string? requestedJobId = null)
    {
        return UpdateTask(taskId, item => item with
        {
            Status = NormalizeStatus(status),
            FinishedAt = _now().ToString("O"),
            RunId = string.IsNullOrWhiteSpace(runId) ? item.RunId : runId.Trim(),
            ResultPayload = CloneJsonObject(resultPayload),
            Error = string.IsNullOrWhiteSpace(error) ? string.Empty : error.Trim(),
            WorkflowId = string.IsNullOrWhiteSpace(workflowId) ? item.WorkflowId : workflowId.Trim(),
            RequestedJobId = string.IsNullOrWhiteSpace(requestedJobId) ? item.RequestedJobId : requestedJobId.Trim(),
        });
    }

    public WorkflowQueueTaskItem? CancelTask(string taskId)
    {
        return UpdateTask(taskId, item =>
        {
            var status = NormalizeStatus(item.Status);
            if (string.Equals(status, "running", StringComparison.Ordinal))
            {
                throw new InvalidOperationException("task is running; cancel unsupported");
            }

            if (!string.Equals(status, "queued", StringComparison.Ordinal))
            {
                throw new InvalidOperationException($"task status is not cancellable: {status}");
            }

            return item with
            {
                Status = "canceled",
                FinishedAt = _now().ToString("O"),
            };
        });
    }

    public WorkflowQueueTaskItem RetryTask(string taskId)
    {
        var items = LoadAllTasks();
        var source = items.FirstOrDefault(item => string.Equals(item.TaskId, taskId.Trim(), StringComparison.Ordinal));
        if (source is null)
        {
            throw new InvalidOperationException("task not found");
        }

        var retry = source with
        {
            TaskId = BuildTaskId(),
            Status = "queued",
            CreatedAt = _now().ToString("O"),
            StartedAt = string.Empty,
            FinishedAt = string.Empty,
            RunId = string.Empty,
            ResultPayload = new JsonObject(),
            Error = string.Empty,
        };
        items.Add(retry);
        SaveAllTasks(items);
        return retry;
    }

    public string BuildTaskId()
    {
        return $"{_now():yyyyMMddHHmmss}_{_randomHex()}";
    }

    internal static WorkflowQueueControlState NormalizeControl(JsonObject? source)
    {
        var paused = source?["paused"]?.GetValue<bool?>() ?? false;
        var quotas = new Dictionary<string, int>(StringComparer.Ordinal);
        if (source?["quotas"] is JsonObject quotaObject)
        {
            foreach (var property in quotaObject)
            {
                var key = (property.Key ?? string.Empty).Trim();
                if (string.IsNullOrWhiteSpace(key))
                {
                    continue;
                }

                var parsed = property.Value is JsonValue value && (
                    value.TryGetValue<int>(out var intValue)
                    || (value.TryGetValue<long>(out var longValue) && (intValue = (int)longValue) >= 0))
                    ? intValue
                    : property.Value is JsonValue stringValue && stringValue.TryGetValue<string>(out var text) && int.TryParse(text, out var fromText)
                        ? fromText
                        : 0;
                if (parsed <= 0)
                {
                    continue;
                }

                quotas[key] = Math.Clamp(parsed, 1, 8);
            }
        }

        return new WorkflowQueueControlState(paused, quotas);
    }

    internal static WorkflowQueueTaskItem NormalizeTask(JsonObject? source, int index)
    {
        var item = source ?? new JsonObject();
        var taskId = ReadString(item["task_id"]) ?? $"task_{index + 1}";
        var label = ReadString(item["label"]) ?? "workflow_task";
        var dispatchKind = ReadString(item["dispatch_kind"]) ?? "legacy_flow";
        var dispatchBaseUrl = ReadString(item["dispatch_base_url"]) ?? string.Empty;
        var workflowId = ReadString(item["workflow_id"]) ?? string.Empty;
        var owner = ReadString(item["owner"]) ?? string.Empty;
        var requestedJobId = ReadString(item["requested_job_id"]) ?? string.Empty;
        var flow = ReadString(item["flow"]) ?? string.Empty;
        var priority = item["priority"]?.GetValue<int?>() ?? 100;
        var status = NormalizeStatus(ReadString(item["status"]) ?? "queued");
        var createdAt = ReadString(item["created_at"]) ?? DateTimeOffset.UtcNow.ToString("O");
        var startedAt = ReadString(item["started_at"]) ?? string.Empty;
        var finishedAt = ReadString(item["finished_at"]) ?? string.Empty;
        var runId = ReadString(item["run_id"]) ?? string.Empty;
        var error = ReadString(item["error"]) ?? string.Empty;
        return new WorkflowQueueTaskItem(
            taskId,
            label,
            dispatchKind,
            dispatchBaseUrl,
            workflowId,
            owner,
            requestedJobId,
            flow,
            Math.Clamp(priority, 0, 100000),
            status,
            createdAt,
            startedAt,
            finishedAt,
            runId,
            CloneJsonObject(item["payload"] as JsonObject),
            CloneJsonObject(item["result"] as JsonObject),
            error);
    }

    private WorkflowQueueTaskItem? UpdateTask(string taskId, Func<WorkflowQueueTaskItem, WorkflowQueueTaskItem> transform)
    {
        var normalizedTaskId = (taskId ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(normalizedTaskId))
        {
            throw new InvalidOperationException("task_id required");
        }

        var items = LoadAllTasks();
        var index = items.FindIndex(item => string.Equals(item.TaskId, normalizedTaskId, StringComparison.Ordinal));
        if (index < 0)
        {
            return null;
        }

        var updated = transform(items[index]);
        items[index] = NormalizeTask(ToJsonObject(updated), index);
        SaveAllTasks(items);
        return items[index];
    }

    private List<WorkflowQueueTaskItem> LoadAllTasks()
    {
        if (!File.Exists(_queuePath))
        {
            return [];
        }

        try
        {
            var root = JsonNode.Parse(File.ReadAllText(_queuePath)) switch
            {
                JsonObject objectRoot when objectRoot["items"] is JsonArray itemsArray => itemsArray,
                JsonArray directArray => directArray,
                _ => null,
            };
            if (root is null)
            {
                return [];
            }

            return root.OfType<JsonObject>()
                .Select((item, index) => NormalizeTask(item, index))
                .ToList();
        }
        catch
        {
            return [];
        }
    }

    private void SaveAllTasks(IReadOnlyList<WorkflowQueueTaskItem> items)
    {
        WriteJsonObject(_queuePath, new JsonObject
        {
            ["items"] = new JsonArray(items.Select(item => (JsonNode)ToJsonObject(item)).ToArray()),
        });
    }

    private static JsonObject ToJsonObject(WorkflowQueueTaskItem item)
    {
        return new JsonObject
        {
            ["task_id"] = item.TaskId,
            ["label"] = item.Label,
            ["dispatch_kind"] = item.DispatchKind,
            ["dispatch_base_url"] = item.DispatchBaseUrl,
            ["workflow_id"] = item.WorkflowId,
            ["owner"] = item.Owner,
            ["requested_job_id"] = item.RequestedJobId,
            ["flow"] = item.Flow,
            ["priority"] = item.Priority,
            ["status"] = item.Status,
            ["created_at"] = item.CreatedAt,
            ["started_at"] = item.StartedAt,
            ["finished_at"] = item.FinishedAt,
            ["run_id"] = item.RunId,
            ["payload"] = CloneJsonObject(item.Payload),
            ["result"] = CloneJsonObject(item.ResultPayload),
            ["error"] = item.Error,
        };
    }

    private static JsonObject SerializeQuotas(IReadOnlyDictionary<string, int> quotas)
    {
        var root = new JsonObject();
        foreach (var item in quotas.OrderBy(static item => item.Key, StringComparer.Ordinal))
        {
            root[item.Key] = item.Value;
        }

        return root;
    }

    private static JsonObject? ReadJsonObject(string path)
    {
        if (!File.Exists(path))
        {
            return null;
        }

        try
        {
            return JsonNode.Parse(File.ReadAllText(path)) as JsonObject;
        }
        catch
        {
            return null;
        }
    }

    private static void WriteJsonObject(string path, JsonObject root)
    {
        var directory = Path.GetDirectoryName(path) ?? ".";
        Directory.CreateDirectory(directory);
        File.WriteAllText(
            path,
            JsonSerializer.Serialize(root, new JsonSerializerOptions { WriteIndented = true }) + Environment.NewLine);
    }

    private static string NormalizeStatus(string status)
    {
        var normalized = (status ?? string.Empty).Trim().ToLowerInvariant();
        return string.IsNullOrWhiteSpace(normalized) ? "queued" : normalized;
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
}
