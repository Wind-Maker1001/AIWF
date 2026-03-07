using Microsoft.UI.Xaml.Controls;

namespace AIWF.Native;

public sealed partial class MainWindow
{
    private Border AddCanvasNode(
        string nodeKey,
        string title,
        string subtitle,
        double left,
        double top,
        string? artifactPath = null,
        string? artifactKind = null,
        bool isUserNode = false,
        bool select = false,
        bool dismissHint = false,
        bool ensureExtent = false,
        bool requestAutosave = false)
    {
        var node = CreateCanvasNode(
            nodeKey,
            title,
            subtitle,
            left,
            top,
            artifactPath,
            artifactKind,
            isUserNode);
        return InsertCanvasNode(
            node,
            select,
            dismissHint,
            ensureExtent,
            requestAutosave);
    }

    private Border InsertCanvasNode(
        Border node,
        bool select = false,
        bool dismissHint = false,
        bool ensureExtent = false,
        bool requestAutosave = false)
    {
        WorkspaceCanvas.Children.Add(node);
        if (select)
        {
            SelectNode(node);
        }

        if (dismissHint)
        {
            DismissCanvasHint();
        }

        if (ensureExtent)
        {
            EnsureCanvasExtentForViewportAndNodes();
        }

        if (requestAutosave)
        {
            RequestCanvasAutosave();
        }

        return node;
    }
}
