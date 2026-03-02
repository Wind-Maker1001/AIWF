using System;
using Microsoft.UI.Input;
using Microsoft.UI.Xaml;
using Microsoft.UI.Xaml.Controls;
using Microsoft.UI.Xaml.Input;
using Microsoft.UI.Xaml.Shapes;
using Windows.Foundation;

namespace AIWF.Native;

public sealed partial class MainWindow
{
    private void OnCanvasPointerPressed(object sender, PointerRoutedEventArgs e)
    {
        if (_isCreatingConnection)
        {
            return;
        }

        if (_draggingNode is not null || IsCanvasNodeSource(e.OriginalSource as DependencyObject))
        {
            return;
        }

        var point = e.GetCurrentPoint(CanvasViewport);
        var properties = point.Properties;
        var deviceType = point.PointerDeviceType;
        if (!properties.IsLeftButtonPressed && !properties.IsMiddleButtonPressed && !properties.IsRightButtonPressed)
        {
            return;
        }

        var isTouch = string.Equals(deviceType.ToString(), "Touch", StringComparison.OrdinalIgnoreCase);
        if (isTouch)
        {
            return;
        }

        var spacePressed = IsSpacePressed();
        if (properties.IsLeftButtonPressed && !properties.IsMiddleButtonPressed && !properties.IsRightButtonPressed)
        {
            if (spacePressed)
            {
                BeginCanvasPanning(e);
                return;
            }

            BeginMarqueeSelection(e);
            e.Handled = true;
            return;
        }

        if (properties.IsMiddleButtonPressed)
        {
            BeginCanvasPanning(e);
            return;
        }

        if (properties.IsRightButtonPressed)
        {
            SelectConnection(null);
        }
    }

    private bool IsSpacePressed()
    {
        var state = InputKeyboardSource.GetKeyStateForCurrentThread(Windows.System.VirtualKey.Space);
        var isDown = (state & Windows.UI.Core.CoreVirtualKeyStates.Down) == Windows.UI.Core.CoreVirtualKeyStates.Down;
        if (!isDown && _isSpaceHeld)
        {
            _isSpaceHeld = false;
        }

        return isDown || _isSpaceHeld;
    }

    private bool IsSpaceClickSuppressed()
    {
        if (_activeSection != NavSection.Canvas)
        {
            return false;
        }

        var elapsed = Environment.TickCount64 - _lastSpaceKeyTickMs;
        return elapsed >= 0 && elapsed <= 220;
    }

    private void SetCanvasManipulationEnabled(bool enabled)
    {
        CanvasViewport.ManipulationMode = enabled
            ? ManipulationModes.TranslateX
              | ManipulationModes.TranslateY
              | ManipulationModes.Scale
              | ManipulationModes.TranslateInertia
              | ManipulationModes.ScaleInertia
            : ManipulationModes.None;
    }

    private void BeginCanvasPanning(PointerRoutedEventArgs e)
    {
        SelectConnection(null);
        _isCanvasPanning = true;
        _isPointerPanningMode = true;
        _panStartPoint = e.GetCurrentPoint(CanvasViewport).Position;
        _panStartTranslateX = CanvasTransform.TranslateX;
        _panStartTranslateY = CanvasTransform.TranslateY;
        SetCanvasManipulationEnabled(false);
        CanvasViewport.CapturePointer(e.Pointer);
        BeginCanvasViewportInteraction();
        DismissCanvasHint();
        e.Handled = true;
    }

    private void OnCanvasPointerMoved(object sender, PointerRoutedEventArgs e)
    {
        if (_isCreatingConnection)
        {
            if (_connectionPreviewLine is not null && _connectionSourceNode is not null && e.Pointer.PointerId == _connectionPointerId)
            {
                var start = GetNodeOutputPoint(_connectionSourceNode);
                var end = e.GetCurrentPoint(WorkspaceCanvas).Position;
                _connectionPreviewLine.X1 = start.X;
                _connectionPreviewLine.Y1 = start.Y;
                _connectionPreviewLine.X2 = end.X;
                _connectionPreviewLine.Y2 = end.Y;
                e.Handled = true;
            }

            return;
        }

        if (_isMarqueeSelecting && e.Pointer.PointerId == _marqueePointerId)
        {
            UpdateMarqueeSelection(e.GetCurrentPoint(CanvasViewport).Position);
            e.Handled = true;
            return;
        }

        if (!_isCanvasPanning)
        {
            return;
        }

        var point = e.GetCurrentPoint(CanvasViewport).Position;
        var clamped = _canvasViewportEngine.PanAndClamp(
            _panStartPoint.X,
            _panStartPoint.Y,
            point.X,
            point.Y,
            _panStartTranslateX,
            _panStartTranslateY,
            CanvasTransform.ScaleX,
            _canvasWidth,
            _canvasHeight,
            CanvasViewport.ActualWidth,
            CanvasViewport.ActualHeight);
        CanvasTransform.TranslateX = clamped.TranslateX;
        CanvasTransform.TranslateY = clamped.TranslateY;
        e.Handled = true;
    }

    private void OnCanvasPointerReleased(object sender, PointerRoutedEventArgs e)
    {
        if (_isCreatingConnection && e.Pointer.PointerId == _connectionPointerId)
        {
            TryCompleteConnection(e);
            CanvasViewport.ReleasePointerCaptures();
            return;
        }

        if (_isMarqueeSelecting && e.Pointer.PointerId == _marqueePointerId)
        {
            EndMarqueeSelection();
            e.Handled = true;
            return;
        }

        if (!_isCanvasPanning)
        {
            return;
        }

        _isCanvasPanning = false;
        _isPointerPanningMode = false;
        CanvasViewport.ReleasePointerCaptures();
        SetCanvasManipulationEnabled(true);
        EndCanvasViewportInteraction();
        e.Handled = true;
    }

    private void BeginMarqueeSelection(PointerRoutedEventArgs e)
    {
        _isMarqueeSelecting = true;
        _isCanvasPanning = false;
        _isPointerPanningMode = false;
        _marqueePointerId = e.Pointer.PointerId;
        _marqueeStartPoint = e.GetCurrentPoint(CanvasViewport).Position;
        _selectedConnection = null;
        _selectedNode = null;
        _multiSelectedNodes.Clear();
        ApplyNodeSelectionVisuals();
        UpdateConnectionVisuals();
        UpdateNodePropertyPanel();
        CanvasMarqueeSelectionBox.Visibility = Visibility.Visible;
        CanvasMarqueeSelectionBox.Margin = new Thickness(_marqueeStartPoint.X, _marqueeStartPoint.Y, 0, 0);
        CanvasMarqueeSelectionBox.Width = 0;
        CanvasMarqueeSelectionBox.Height = 0;
        SetCanvasManipulationEnabled(false);
        CanvasViewport.CapturePointer(e.Pointer);
        DismissCanvasHint();
    }

    private void UpdateMarqueeSelection(Point currentViewportPoint)
    {
        var marquee = CanvasRuntime.CanvasInteractionMath.ComputeMarqueeBox(_marqueeStartPoint, currentViewportPoint);

        CanvasMarqueeSelectionBox.Margin = new Thickness(marquee.Left, marquee.Top, 0, 0);
        CanvasMarqueeSelectionBox.Width = marquee.Width;
        CanvasMarqueeSelectionBox.Height = marquee.Height;

        var rect = CanvasRuntime.CanvasInteractionMath.ToCanvasRect(
            marquee,
            CanvasTransform.TranslateX,
            CanvasTransform.TranslateY,
            CanvasTransform.ScaleX);
        _multiSelectedNodes.Clear();
        foreach (var node in GetCanvasNodeBorders())
        {
            if (node.Tag is not CanvasNodeTag tag || !tag.IsUserNode)
            {
                continue;
            }

            var nodeLeft = Canvas.GetLeft(node);
            var nodeTop = Canvas.GetTop(node);
            var nodeWidth = node.ActualWidth > 1 ? node.ActualWidth : node.Width;
            var nodeHeight = node.ActualHeight > 1 ? node.ActualHeight : Math.Max(node.MinHeight, 96);
            var nodeRect = new Windows.Foundation.Rect(nodeLeft, nodeTop, Math.Max(1, nodeWidth), Math.Max(1, nodeHeight));
            if (CanvasRuntime.CanvasInteractionMath.Intersects(nodeRect, rect))
            {
                _multiSelectedNodes.Add(node);
            }
        }

        ApplyNodeSelectionVisuals();
        UpdateNodePropertyPanel();
    }

    private void EndMarqueeSelection()
    {
        _isMarqueeSelecting = false;
        _marqueePointerId = 0;
        CanvasViewport.ReleasePointerCaptures();
        SetCanvasManipulationEnabled(true);
        CanvasMarqueeSelectionBox.Visibility = Visibility.Collapsed;
        CanvasMarqueeSelectionBox.Width = 0;
        CanvasMarqueeSelectionBox.Height = 0;
        if (_multiSelectedNodes.Count > 0)
        {
            SetInlineStatus($"已框选 {_multiSelectedNodes.Count} 个节点，按 Delete 可删除。", InlineStatusTone.Success);
        }
    }
}
