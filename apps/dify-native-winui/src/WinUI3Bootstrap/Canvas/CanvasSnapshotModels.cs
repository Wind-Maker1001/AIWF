namespace AIWF.Native.CanvasRuntime;

internal sealed class CanvasSnapshot
{
    public double CanvasWidth { get; set; }
    public double CanvasHeight { get; set; }
    public double ViewScale { get; set; } = 1;
    public double ViewTranslateX { get; set; }
    public double ViewTranslateY { get; set; }
    public List<CanvasNodeSnapshot> Nodes { get; set; } = new();
    public List<CanvasEdgeSnapshot> Edges { get; set; } = new();
}

internal sealed class CanvasNodeSnapshot
{
    public string NodeKey { get; set; } = string.Empty;
    public string Title { get; set; } = string.Empty;
    public string Subtitle { get; set; } = string.Empty;
    public double X { get; set; }
    public double Y { get; set; }
    public bool IsUserNode { get; set; }
    public bool IsArtifactNode { get; set; }
    public string? ArtifactPath { get; set; }
    public string? ArtifactKind { get; set; }
}

internal sealed class CanvasEdgeSnapshot
{
    public string SourceKey { get; set; } = string.Empty;
    public string TargetKey { get; set; } = string.Empty;
}
