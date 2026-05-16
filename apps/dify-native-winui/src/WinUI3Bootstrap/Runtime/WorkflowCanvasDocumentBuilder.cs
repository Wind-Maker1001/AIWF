using System.Text.Json.Nodes;

namespace AIWF.Native.Runtime;

public sealed record CanvasWorkflowNodeState(
    string NodeKey,
    string WorkflowNodeType,
    string Title,
    string Subtitle,
    double X,
    double Y,
    JsonObject? WorkflowConfig);

public sealed record CanvasWorkflowEdgeState(string From, string To);

public sealed record CanvasWorkflowViewportState(double Scale, double TranslateX, double TranslateY);

public sealed record CanvasWorkflowSelectionState(IReadOnlyList<string> NodeIds);

public static class WorkflowCanvasDocumentBuilder
{
    public static WorkflowGraphDocument? Build(
        string workflowId,
        string workflowVersion,
        IReadOnlyList<CanvasWorkflowNodeState> nodes,
        IReadOnlyList<CanvasWorkflowEdgeState> edges,
        CanvasWorkflowViewportState viewport,
        CanvasWorkflowSelectionState selection)
    {
        var normalizedNodes = NormalizeNodes(nodes);
        if (normalizedNodes.Count == 0)
        {
            return null;
        }

        var allowedIds = normalizedNodes
            .Select(static node => node.NodeKey)
            .ToHashSet(StringComparer.Ordinal);
        var normalizedEdges = NormalizeEdges(edges, allowedIds);
        var incoming = normalizedEdges
            .GroupBy(static edge => edge.To, StringComparer.Ordinal)
            .ToDictionary(
                static group => group.Key,
                static group => (IReadOnlyList<string>)group.Select(static item => item.From).ToArray(),
                StringComparer.Ordinal);

        var documentNodes = normalizedNodes
            .Select(node =>
            {
                var config = CloneJsonObject(node.WorkflowConfig) ?? SqlCanvasNodeDefaults.Create(node.WorkflowNodeType);
                var predecessors = incoming.TryGetValue(node.NodeKey, out var values) ? values : Array.Empty<string>();
                ApplyRuntimeWorkflowInputs(node.WorkflowNodeType, config, predecessors);
                return new WorkflowGraphNodeDocument(
                    node.NodeKey,
                    node.WorkflowNodeType,
                    node.Title,
                    node.Subtitle,
                    node.X,
                    node.Y,
                    config);
            })
            .ToArray();

        return new WorkflowGraphDocument(
            WorkflowId: string.IsNullOrWhiteSpace(workflowId) ? "native_sql_workflow" : workflowId,
            Version: string.IsNullOrWhiteSpace(workflowVersion) ? "1.0.0" : workflowVersion,
            Nodes: documentNodes,
            Edges: normalizedEdges.Select(static edge => new WorkflowGraphEdgeDocument(edge.From, edge.To)).ToArray(),
            Viewport: new WorkflowGraphViewportDocument(viewport.Scale, viewport.TranslateX, viewport.TranslateY),
            Selection: new WorkflowGraphSelectionDocument(
                selection.NodeIds
                    .Where(static id => !string.IsNullOrWhiteSpace(id))
                    .Distinct(StringComparer.Ordinal)
                    .ToArray()));
    }

    public static JsonObject SerializeWorkflowDefinition(WorkflowGraphDocument document)
    {
        var nodes = new JsonArray();
        foreach (var node in document.Nodes)
        {
            nodes.Add(new JsonObject
            {
                ["id"] = node.Id,
                ["type"] = node.Type,
                ["x"] = node.X,
                ["y"] = node.Y,
                ["config"] = CloneJsonObject(node.Config),
            });
        }

        var edges = new JsonArray();
        foreach (var edge in document.Edges)
        {
            edges.Add(new JsonObject
            {
                ["from"] = edge.From,
                ["to"] = edge.To,
            });
        }

        return new JsonObject
        {
            ["workflow_id"] = document.WorkflowId,
            ["version"] = document.Version,
            ["nodes"] = nodes,
            ["edges"] = edges,
        };
    }

    private static IReadOnlyList<CanvasWorkflowNodeState> NormalizeNodes(IReadOnlyList<CanvasWorkflowNodeState> nodes)
    {
        var normalized = new List<CanvasWorkflowNodeState>(nodes.Count);
        var seenKeys = new HashSet<string>(StringComparer.Ordinal);
        foreach (var node in nodes)
        {
            var key = (node.NodeKey ?? string.Empty).Trim();
            var workflowNodeType = (node.WorkflowNodeType ?? string.Empty).Trim();
            if (string.IsNullOrWhiteSpace(key)
                || string.IsNullOrWhiteSpace(workflowNodeType)
                || !seenKeys.Add(key))
            {
                continue;
            }

            normalized.Add(node with
            {
                NodeKey = key,
                WorkflowNodeType = workflowNodeType,
                Title = string.IsNullOrWhiteSpace(node.Title) ? key : node.Title,
                Subtitle = node.Subtitle ?? string.Empty,
            });
        }

        return normalized;
    }

    private static IReadOnlyList<CanvasWorkflowEdgeState> NormalizeEdges(
        IReadOnlyList<CanvasWorkflowEdgeState> edges,
        ISet<string> allowedIds)
    {
        var normalized = new List<CanvasWorkflowEdgeState>(edges.Count);
        var seenPairs = new HashSet<string>(StringComparer.Ordinal);
        foreach (var edge in edges)
        {
            var from = (edge.From ?? string.Empty).Trim();
            var to = (edge.To ?? string.Empty).Trim();
            if (string.IsNullOrWhiteSpace(from)
                || string.IsNullOrWhiteSpace(to)
                || !allowedIds.Contains(from)
                || !allowedIds.Contains(to))
            {
                continue;
            }

            var pairKey = $"{from}->{to}";
            if (!seenPairs.Add(pairKey))
            {
                continue;
            }

            normalized.Add(new CanvasWorkflowEdgeState(from, to));
        }

        return normalized;
    }

    private static void ApplyRuntimeWorkflowInputs(string workflowNodeType, JsonObject config, IReadOnlyList<string> predecessors)
    {
        switch (workflowNodeType)
        {
            case "columnar_eval_v1":
            case "aggregate_rows_v2":
            case "query_lang_v1":
            case "sql_chart_v1":
                if (predecessors.Count > 0)
                {
                    config["rows"] = new JsonArray();
                    config["input_map"] = new JsonObject
                    {
                        ["rows"] = new JsonObject { ["from"] = predecessors[0], ["path"] = "rows" }
                    };
                }
                break;
            case "join_rows_v2":
                if (predecessors.Count > 0)
                {
                    config["left_rows"] = new JsonArray();
                }
                if (predecessors.Count > 1)
                {
                    config["right_rows"] = new JsonArray();
                }
                if (predecessors.Count > 0)
                {
                    config["input_map"] = new JsonObject
                    {
                        ["left_rows"] = new JsonObject { ["from"] = predecessors[0], ["path"] = "rows" },
                        ["right_rows"] = new JsonObject
                        {
                            ["from"] = predecessors.Count > 1 ? predecessors[1] : predecessors[0],
                            ["path"] = "rows"
                        }
                    };
                }
                break;
        }
    }

    private static JsonObject? CloneJsonObject(JsonObject? source)
    {
        return source is null
            ? null
            : JsonNode.Parse(source.ToJsonString()) as JsonObject;
    }
}
