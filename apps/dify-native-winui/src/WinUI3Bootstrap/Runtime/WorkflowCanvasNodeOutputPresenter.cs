using System.Text.Json.Nodes;

namespace AIWF.Native.Runtime;

public sealed record WorkflowCanvasNodeOutputState(
    string NodeKey,
    bool? Ok,
    string Status,
    int RowCount,
    string Subtitle,
    JsonObject RawOutput,
    bool HasChartData);

public sealed record WorkflowCanvasNodeOutputPresentation(
    IReadOnlyList<WorkflowCanvasNodeOutputState> Items,
    JsonObject? FirstRowsOutput,
    JsonObject? ChartSource);

public static class WorkflowCanvasNodeOutputPresenter
{
    public static WorkflowCanvasNodeOutputPresentation Create(JsonObject response)
    {
        var items = new List<WorkflowCanvasNodeOutputState>();
        JsonObject? firstRowsOutput = null;
        JsonObject? chartSource = null;

        if (response["node_outputs"] is JsonObject outputs)
        {
            foreach (var kv in outputs)
            {
                if (kv.Value is not JsonObject rawOutput)
                {
                    continue;
                }

                var ok = rawOutput["ok"]?.GetValue<bool?>();
                var status = rawOutput["status"]?.GetValue<string>() ?? (ok == true ? "done" : "error");
                var rowCount = rawOutput["rows"] is JsonArray rows ? rows.Count : 0;
                var hasChartData = rawOutput["chart_type"] is not null;
                var subtitle = ok == true ? $"{status} ({rowCount} rows)" : $"ERR: {status}";
                var clonedOutput = CloneJsonObject(rawOutput) ?? new JsonObject();

                items.Add(new WorkflowCanvasNodeOutputState(
                    kv.Key,
                    ok,
                    status,
                    rowCount,
                    subtitle,
                    clonedOutput,
                    hasChartData));

                firstRowsOutput ??= clonedOutput["rows"] is JsonArray ? clonedOutput : null;
                chartSource ??= hasChartData ? clonedOutput : null;
            }
        }

        if (response["final_output"] is JsonObject finalOutput)
        {
            firstRowsOutput ??= finalOutput["rows"] is JsonArray ? CloneJsonObject(finalOutput) : null;
            chartSource ??= finalOutput["chart_type"] is not null ? CloneJsonObject(finalOutput) : null;
        }

        return new WorkflowCanvasNodeOutputPresentation(items, firstRowsOutput, chartSource);
    }

    private static JsonObject? CloneJsonObject(JsonObject? source)
    {
        return source is null
            ? null
            : JsonNode.Parse(source.ToJsonString()) as JsonObject;
    }
}
