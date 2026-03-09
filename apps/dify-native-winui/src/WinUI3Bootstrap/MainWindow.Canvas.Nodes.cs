using AIWF.Native.CanvasRuntime;
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
        bool isArtifactNode = false,
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
            isUserNode,
            isArtifactNode);
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
        RegisterCoreCanvasNodeReference(node);
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

    private void RegisterCoreCanvasNodeReference(Border node)
    {
        if (node.Tag is not CanvasNodeTag tag)
        {
            return;
        }

        switch (tag.NodeKey)
        {
            case CanvasWorkflowScaffold.InputNodeKey:
                _inputNode = node;
                break;
            case CanvasWorkflowScaffold.CleanNodeKey:
                _cleanNode = node;
                break;
            case CanvasWorkflowScaffold.OutputNodeKey:
                _outputNode = node;
                break;
        }
    }

    private void UnregisterCoreCanvasNodeReference(Border node)
    {
        if (ReferenceEquals(_inputNode, node))
        {
            _inputNode = null;
        }

        if (ReferenceEquals(_cleanNode, node))
        {
            _cleanNode = null;
        }

        if (ReferenceEquals(_outputNode, node))
        {
            _outputNode = null;
        }
    }
}
