using Microsoft.UI.Xaml.Controls;
using Microsoft.UI.Xaml.Shapes;

namespace AIWF.Native;

public sealed partial class MainWindow
{
    private sealed class CanvasNodeTag
    {
        public required string NodeKey { get; init; }
        public string? ArtifactPath { get; set; }
        public string? ArtifactKind { get; init; }
        public bool IsUserNode { get; init; }
        public bool IsArtifactNode { get; init; }
        public TextBlock? TitleBlock { get; set; }
        public TextBlock? SubtitleBlock { get; set; }
    }

    private sealed class ConnectorTag
    {
        public required Border Node { get; init; }
        public required string Kind { get; init; } // "in" or "out"
    }

    private sealed class ConnectionEdge
    {
        public required Border Source { get; init; }
        public required Border Target { get; init; }
        public required Line Line { get; init; }
    }

    private sealed class NodeTemplate
    {
        public required string KeyPrefix { get; init; }
        public required string Title { get; init; }
        public required string Subtitle { get; init; }
        public Symbol Icon { get; init; } = Symbol.Page;
        public string? Group { get; init; }
    }

    private sealed class CopiedNodeTemplate
    {
        public string Title { get; set; } = string.Empty;
        public string Subtitle { get; set; } = string.Empty;
    }
}
