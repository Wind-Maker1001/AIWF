namespace AIWF.Native.CanvasRuntime;

internal sealed record CanvasNodeState(
    string NodeKey,
    string Title,
    string Subtitle,
    double X,
    double Y,
    bool IsUserNode,
    bool IsArtifactNode,
    string? ArtifactPath,
    string? ArtifactKind);

internal sealed record CanvasEdgeState(string SourceKey, string TargetKey);

internal sealed record CanvasSnapshotRestorePlan(
    double CanvasWidth,
    double CanvasHeight,
    double ViewScale,
    double ViewTranslateX,
    double ViewTranslateY,
    IReadOnlyList<CanvasNodeState> Nodes,
    IReadOnlyList<CanvasEdgeState> Edges,
    int NextCustomNodeCounter);

internal static class CanvasSnapshotMapper
{
    public static CanvasSnapshot CreateSnapshot(
        double canvasWidth,
        double canvasHeight,
        double viewScale,
        double viewTranslateX,
        double viewTranslateY,
        IEnumerable<CanvasNodeState> nodes,
        IEnumerable<CanvasEdgeState> edges,
        double minScale,
        double maxScale)
    {
        var normalizedNodes = NormalizeNodes(nodes);
        var nodeKeys = normalizedNodes.Select(static x => x.NodeKey).ToHashSet(StringComparer.Ordinal);
        var normalizedEdges = NormalizeEdges(edges, nodeKeys);

        return new CanvasSnapshot
        {
            CanvasWidth = canvasWidth,
            CanvasHeight = canvasHeight,
            ViewScale = Math.Clamp(viewScale, minScale, maxScale),
            ViewTranslateX = viewTranslateX,
            ViewTranslateY = viewTranslateY,
            Nodes = normalizedNodes
                .Select(static node => new CanvasNodeSnapshot
                {
                    NodeKey = node.NodeKey,
                    Title = node.Title,
                    Subtitle = node.Subtitle,
                    X = node.X,
                    Y = node.Y,
                    IsUserNode = node.IsUserNode,
                    IsArtifactNode = node.IsArtifactNode,
                    ArtifactPath = node.ArtifactPath,
                    ArtifactKind = node.ArtifactKind
                })
                .ToList(),
            Edges = normalizedEdges
                .Select(static edge => new CanvasEdgeSnapshot
                {
                    SourceKey = edge.SourceKey,
                    TargetKey = edge.TargetKey
                })
                .ToList()
        };
    }

    public static CanvasSnapshotRestorePlan CreateRestorePlan(
        CanvasSnapshot snapshot,
        double defaultCanvasWidth,
        double defaultCanvasHeight,
        double gridSize,
        double minScale,
        double maxScale,
        double estimatedNodeWidth = 320,
        double estimatedNodeHeight = 220)
    {
        var normalizedNodes = NormalizeNodes(snapshot.Nodes.Select(static node => new CanvasNodeState(
            node.NodeKey,
            node.Title,
            node.Subtitle,
            Math.Max(0, node.X),
            Math.Max(0, node.Y),
            node.IsUserNode,
            node.IsArtifactNode,
            node.ArtifactPath,
            node.ArtifactKind)));

        var maxX = Math.Max(defaultCanvasWidth, snapshot.CanvasWidth);
        var maxY = Math.Max(defaultCanvasHeight, snapshot.CanvasHeight);
        foreach (var node in normalizedNodes)
        {
            maxX = Math.Max(maxX, node.X + estimatedNodeWidth);
            maxY = Math.Max(maxY, node.Y + estimatedNodeHeight);
        }

        var canvasWidth = Math.Ceiling(Math.Max(defaultCanvasWidth, maxX) / gridSize) * gridSize;
        var canvasHeight = Math.Ceiling(Math.Max(defaultCanvasHeight, maxY) / gridSize) * gridSize;
        var nodeKeys = normalizedNodes.Select(static x => x.NodeKey).ToHashSet(StringComparer.Ordinal);
        var normalizedEdges = NormalizeEdges(
            snapshot.Edges.Select(static edge => new CanvasEdgeState(edge.SourceKey, edge.TargetKey)),
            nodeKeys);

        return new CanvasSnapshotRestorePlan(
            CanvasWidth: canvasWidth,
            CanvasHeight: canvasHeight,
            ViewScale: Math.Clamp(snapshot.ViewScale, minScale, maxScale),
            ViewTranslateX: snapshot.ViewTranslateX,
            ViewTranslateY: snapshot.ViewTranslateY,
            Nodes: normalizedNodes,
            Edges: normalizedEdges,
            NextCustomNodeCounter: Math.Max(1, normalizedNodes.Count(static x => x.IsUserNode) + 1));
    }

    private static IReadOnlyList<CanvasNodeState> NormalizeNodes(IEnumerable<CanvasNodeState> nodes)
    {
        var normalizedNodes = new List<CanvasNodeState>();
        var seenKeys = new HashSet<string>(StringComparer.Ordinal);
        foreach (var node in nodes)
        {
            var key = (node.NodeKey ?? string.Empty).Trim();
            if (string.IsNullOrWhiteSpace(key) || !seenKeys.Add(key))
            {
                continue;
            }

            normalizedNodes.Add(node with
            {
                NodeKey = key,
                Title = node.Title ?? string.Empty,
                Subtitle = node.Subtitle ?? string.Empty,
                X = Math.Max(0, node.X),
                Y = Math.Max(0, node.Y)
            });
        }

        return normalizedNodes;
    }

    private static IReadOnlyList<CanvasEdgeState> NormalizeEdges(IEnumerable<CanvasEdgeState> edges, ISet<string> nodeKeys)
    {
        var normalizedEdges = new List<CanvasEdgeState>();
        var seenPairs = new HashSet<string>(StringComparer.Ordinal);
        foreach (var edge in edges)
        {
            var sourceKey = (edge.SourceKey ?? string.Empty).Trim();
            var targetKey = (edge.TargetKey ?? string.Empty).Trim();
            if (string.IsNullOrWhiteSpace(sourceKey)
                || string.IsNullOrWhiteSpace(targetKey)
                || !nodeKeys.Contains(sourceKey)
                || !nodeKeys.Contains(targetKey))
            {
                continue;
            }

            var pairKey = $"{sourceKey}->{targetKey}";
            if (!seenPairs.Add(pairKey))
            {
                continue;
            }

            normalizedEdges.Add(new CanvasEdgeState(sourceKey, targetKey));
        }

        return normalizedEdges;
    }
}
