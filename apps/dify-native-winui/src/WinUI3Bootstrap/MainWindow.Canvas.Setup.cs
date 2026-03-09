using System.Linq;
using AIWF.Native.CanvasRuntime;
using Microsoft.UI.Xaml;
using Microsoft.UI.Xaml.Controls;
using Microsoft.UI.Xaml.Media;
using Microsoft.UI.Xaml.Shapes;

namespace AIWF.Native;

public sealed partial class MainWindow
{
    private void InitializeCanvasWorkspace()
    {
        ResetCanvasView();
        CanvasViewport.SizeChanged += OnCanvasViewportSizeChanged;
        WorkspaceCanvas.RightTapped += OnWorkspaceCanvasRightTapped;
        BuildCanvasGrid();
        SeedCanvasNodes();
        UpdateNodePropertyPanel();
        UpdateCanvasZoomIndicator();
    }

    private void OnCanvasViewportSizeChanged(object sender, SizeChangedEventArgs e)
    {
        if (!_didSetInitialCanvasView && CanvasViewport.ActualWidth > 0 && CanvasViewport.ActualHeight > 0)
        {
            ResetCanvasView();
            _didSetInitialCanvasView = true;
        }

        ClampCanvasTransform();
        UpdateCanvasZoomIndicator();
    }

    private void BuildCanvasGrid(bool force = false)
    {
        WorkspaceCanvas.Width = _canvasWidth;
        WorkspaceCanvas.Height = _canvasHeight;
        CanvasGridLayer.Width = _canvasWidth;
        CanvasGridLayer.Height = _canvasHeight;
        CanvasGridLayer.Visibility = _showCanvasGrid ? Visibility.Visible : Visibility.Collapsed;

        var sameExtent = Math.Abs(_gridBuiltWidth - _canvasWidth) < 0.1
            && Math.Abs(_gridBuiltHeight - _canvasHeight) < 0.1;
        if (!_showCanvasGrid)
        {
            if (CanvasGridLayer.Children.Count > 0)
            {
                CanvasGridLayer.Children.Clear();
            }
            _gridBuiltWidth = _canvasWidth;
            _gridBuiltHeight = _canvasHeight;
            return;
        }

        if (!force && sameExtent && CanvasGridLayer.Children.Count > 0)
        {
            return;
        }

        CanvasGridLayer.Children.Clear();
        var gridStep = CanvasGridSize;
        while ((_canvasWidth / gridStep) > 700 || (_canvasHeight / gridStep) > 500)
        {
            gridStep *= 2;
        }
        var majorEvery = gridStep * 5;
        var majorBrush = new SolidColorBrush(Windows.UI.Color.FromArgb(0x33, 0x55, 0x55, 0x55));
        var minorBrush = new SolidColorBrush(Windows.UI.Color.FromArgb(0x14, 0x55, 0x55, 0x55));
        for (var x = 0.0; x <= _canvasWidth; x += gridStep)
        {
            var isMajor = (x % majorEvery) == 0;
            CanvasGridLayer.Children.Add(new Line
            {
                X1 = x,
                Y1 = 0,
                X2 = x,
                Y2 = _canvasHeight,
                StrokeThickness = isMajor ? 1.1 : 1,
                Stroke = isMajor ? majorBrush : minorBrush
            });
        }

        for (var y = 0.0; y <= _canvasHeight; y += gridStep)
        {
            var isMajor = (y % majorEvery) == 0;
            CanvasGridLayer.Children.Add(new Line
            {
                X1 = 0,
                Y1 = y,
                X2 = _canvasWidth,
                Y2 = y,
                StrokeThickness = isMajor ? 1.1 : 1,
                Stroke = isMajor ? majorBrush : minorBrush
            });
        }

        _gridBuiltWidth = _canvasWidth;
        _gridBuiltHeight = _canvasHeight;
    }

    private void SeedCanvasNodes()
    {
        WorkspaceCanvas.Children.Clear();
        _artifactNodes.Clear();
        _connections.Clear();
        _inputNode = null;
        _cleanNode = null;
        _outputNode = null;
        _selectedNode = null;
        _multiSelectedNodes.Clear();
        _selectedConnection = null;
        EnsureCoreCanvasWorkflowScaffold();
        UpdateAllConnections();
        ApplyNodeSelectionVisuals();
        UpdateConnectionVisuals();
        UpdateNodePropertyPanel();
    }

    private void EnsureCoreCanvasWorkflowScaffold()
    {
        foreach (var node in CanvasWorkflowScaffold.CreateDefaultNodes())
        {
            if (FindCanvasNodeByKey(node.NodeKey) is not null)
            {
                continue;
            }

            AddCanvasNode(
                node.NodeKey,
                node.Title,
                node.Subtitle,
                node.X,
                node.Y,
                isUserNode: node.IsUserNode,
                isArtifactNode: node.IsArtifactNode);
        }

        foreach (var edge in CanvasWorkflowScaffold.CreateDefaultEdges())
        {
            var source = FindCanvasNodeByKey(edge.SourceKey);
            var target = FindCanvasNodeByKey(edge.TargetKey);
            if (source is null || target is null)
            {
                continue;
            }

            AddConnection(source, target, select: false, requestAutosave: false);
        }
    }

    private Border? FindCanvasNodeByKey(string nodeKey)
    {
        return GetCanvasNodeBorders().FirstOrDefault(node =>
            node.Tag is CanvasNodeTag tag
            && string.Equals(tag.NodeKey, nodeKey, StringComparison.Ordinal));
    }
}
