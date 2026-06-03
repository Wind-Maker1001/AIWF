using System.Text.Json;
using System.Text.Json.Nodes;

namespace AIWF.Native.Runtime;

public sealed record WorkflowDraftRunExecutionResult(
    JsonObject RequestPayload,
    string PrettyResponseJson,
    bool ParsedBindingState,
    RunResultBindingState BindingState,
    JsonObject? FinalOutput,
    WorkflowCanvasNodeOutputPresentation NodeOutputPresentation);

public sealed class WorkflowDraftRunCoordinator
{
    private readonly WorkflowRunnerAdapter _runner;

    public WorkflowDraftRunCoordinator(WorkflowRunnerAdapter runner)
    {
        _runner = runner;
    }

    public async Task<WorkflowDraftRunExecutionResult> ExecuteAsync(
        string accelUrl,
        string apiKey,
        string jobId,
        WorkflowGraphDocument document,
        CancellationToken cancellationToken = default)
    {
        var payload = BuildRequestPayload(jobId, document);

        var httpResult = await _runner.PostJsonRawAsync(
            accelUrl,
            apiKey,
            "/operators/workflow_draft_run_v1",
            payload,
            cancellationToken);

        JsonNode? parsed = null;
        try
        {
            parsed = JsonNode.Parse(httpResult.Body);
        }
        catch
        {
        }

        if (!httpResult.IsSuccessStatusCode)
        {
            var message = parsed?["error"]?.GetValue<string>()
                ?? $"HTTP {(int)httpResult.StatusCode}";
            throw new InvalidOperationException(message);
        }

        if (parsed is not JsonObject response)
        {
            throw new InvalidOperationException("JSON object response expected.");
        }

        var prettyResponseJson = JsonSerializer.Serialize(response, new JsonSerializerOptions
        {
            WriteIndented = true
        });
        var bindingState = RunResultBindingService.CreateParseFailureState("Not retried");
        var parsedBindingState = IsRecognizedRunResponse(response)
            && RunResultBindingService.TryCreateFromJson(
                prettyResponseJson,
                "Not retried",
                out bindingState);

        return new WorkflowDraftRunExecutionResult(
            RequestPayload: JsonNode.Parse(payload.ToJsonString()) as JsonObject ?? new JsonObject(),
            prettyResponseJson,
            parsedBindingState,
            bindingState,
            response["final_output"] as JsonObject,
            WorkflowCanvasNodeOutputPresenter.Create(response));
    }

    public static JsonObject BuildRequestPayload(string jobId, WorkflowGraphDocument document)
    {
        return new JsonObject
        {
            ["workflow_definition"] = WorkflowCanvasDocumentBuilder.SerializeWorkflowDefinition(document),
            ["job_id"] = string.IsNullOrWhiteSpace(jobId) ? document.WorkflowId : jobId.Trim(),
            ["run_id"] = Guid.NewGuid().ToString("N"),
            ["job_context"] = new JsonObject(),
            ["params"] = new JsonObject(),
        };
    }

    private static bool IsRecognizedRunResponse(JsonObject response)
    {
        return response["ok"] is not null
            || response["data"] is JsonObject
            || response["final_output"] is JsonObject
            || response["node_outputs"] is JsonObject
            || response["artifacts"] is JsonArray
            || response["job_id"] is not null;
    }
}
