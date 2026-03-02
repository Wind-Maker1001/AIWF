using System;
using Microsoft.UI.Xaml;
using Microsoft.UI.Xaml.Controls;
using Microsoft.UI.Xaml.Input;
using Microsoft.UI.Xaml.Shapes;

namespace AIWF.Native;

public sealed partial class MainWindow
{
    private void OnNodeTitleChanged(object sender, TextChangedEventArgs e)
    {
        if (_selectedNode?.Tag is not CanvasNodeTag tag || tag.TitleBlock is null)
        {
            return;
        }

        tag.TitleBlock.Text = NodeTitleTextBox.Text.Trim();
        RequestCanvasAutosave();
    }

    private void OnNodeSubtitleChanged(object sender, TextChangedEventArgs e)
    {
        if (_selectedNode?.Tag is not CanvasNodeTag tag || tag.SubtitleBlock is null)
        {
            return;
        }

        tag.SubtitleBlock.Text = NodeSubtitleTextBox.Text.Trim();
        RequestCanvasAutosave();
    }

    private void OnCanvasNodePointerPressed(object sender, PointerRoutedEventArgs e)
    {
        if (sender is not Border node)
        {
            return;
        }

        if (e.OriginalSource is Ellipse && (e.OriginalSource as FrameworkElement)?.Tag is ConnectorTag)
        {
            return;
        }

        SelectNode(node);
        _draggingNode = node;
        _draggingNodePointerId = e.Pointer.PointerId;
        _dragStartPoint = e.GetCurrentPoint(WorkspaceCanvas).Position;
        _dragStartNodeLeft = Canvas.GetLeft(node);
        _dragStartNodeTop = Canvas.GetTop(node);
        _nodeMovedDuringDrag = false;
        SetCanvasManipulationEnabled(false);
        node.CapturePointer(e.Pointer);
        DismissCanvasHint();
        e.Handled = true;
    }

    private void OnCanvasNodePointerMoved(object sender, PointerRoutedEventArgs e)
    {
        if (_draggingNode is null || sender is not Border node || _draggingNodePointerId != e.Pointer.PointerId)
        {
            return;
        }

        var point = e.GetCurrentPoint(WorkspaceCanvas).Position;
        var dx = point.X - _dragStartPoint.X;
        var dy = point.Y - _dragStartPoint.Y;
        if (Math.Abs(dx) > 2 || Math.Abs(dy) > 2)
        {
            _nodeMovedDuringDrag = true;
        }

        var nextPos = CanvasRuntime.NodeDragMath.ComputeDragPosition(
            _dragStartNodeLeft,
            _dragStartNodeTop,
            _dragStartPoint,
            point);
        Canvas.SetLeft(node, nextPos.Left);
        Canvas.SetTop(node, nextPos.Top);
        UpdateAllConnections();
        e.Handled = true;
    }

    private void OnCanvasNodePointerReleased(object sender, PointerRoutedEventArgs e)
    {
        if (_draggingNode is null || sender is not Border node || _draggingNodePointerId != e.Pointer.PointerId)
        {
            return;
        }

        node.ReleasePointerCapture(e.Pointer);
        var snapped = CanvasRuntime.NodeDragMath.SnapToGrid(
            new CanvasRuntime.NodePosition(Canvas.GetLeft(node), Canvas.GetTop(node)),
            CanvasGridSize);
        Canvas.SetLeft(node, snapped.Left);
        Canvas.SetTop(node, snapped.Top);
        UpdateAllConnections();
        EnsureCanvasExtentForViewportAndNodes();
        if (!_nodeMovedDuringDrag)
        {
            TryOpenArtifactFromNode(node);
        }
        else
        {
            RequestCanvasAutosave();
        }

        _draggingNode = null;
        _draggingNodePointerId = 0;
        _nodeMovedDuringDrag = false;
        SetCanvasManipulationEnabled(true);
        e.Handled = true;
    }
}
