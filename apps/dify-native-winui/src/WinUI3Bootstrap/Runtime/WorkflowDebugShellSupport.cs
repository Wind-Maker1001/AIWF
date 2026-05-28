using System.Text.Json.Nodes;

namespace AIWF.Native.Runtime;

public static class WorkflowDebugShellSupport
{
    public static IReadOnlyList<string> ParseNodeIds(string raw)
    {
        return (raw ?? string.Empty)
            .Split([',', '\r', '\n'], StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Where(static item => !string.IsNullOrWhiteSpace(item))
            .Distinct(StringComparer.Ordinal)
            .ToArray();
    }

    public static JsonObject BuildRouteStats(
        int nodeCount,
        int userNodeCount,
        int edgeCount,
        IReadOnlyList<string> selectedNodeIds,
        double scale,
        double translateX,
        double translateY)
    {
        var selected = new JsonArray();
        foreach (var nodeId in selectedNodeIds)
        {
            selected.Add(JsonValue.Create(nodeId));
        }

        return new JsonObject
        {
            ["node_count"] = nodeCount,
            ["user_node_count"] = userNodeCount,
            ["edge_count"] = edgeCount,
            ["selected_node_ids"] = selected,
            ["scale"] = Math.Round(scale, 3),
            ["translate_x"] = Math.Round(translateX, 3),
            ["translate_y"] = Math.Round(translateY, 3),
        };
    }

    public static JsonObject ResolveWorkflowDefinitionForImport(JsonObject source)
    {
        if (source["workflow_definition"] is JsonObject workflowDefinition)
        {
            return JsonNode.Parse(workflowDefinition.ToJsonString()) as JsonObject ?? new JsonObject();
        }

        return JsonNode.Parse(source.ToJsonString()) as JsonObject ?? new JsonObject();
    }
}
