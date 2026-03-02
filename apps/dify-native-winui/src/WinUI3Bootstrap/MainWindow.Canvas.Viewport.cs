using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using Microsoft.UI.Xaml;
using Microsoft.UI.Xaml.Controls;
using Microsoft.UI.Xaml.Input;
using Microsoft.UI.Xaml.Shapes;
using Windows.Foundation;

namespace AIWF.Native;

public sealed partial class MainWindow
{
    private void OnCanvasPointerWheelChanged(object sender, PointerRoutedEventArgs e)
    {
        var delta = e.GetCurrentPoint(CanvasViewport).Properties.MouseWheelDelta;
        if (delta == 0)
        {
            return;
        }

        var factor = delta > 0 ? 1.1 : 0.9;
        var center = e.GetCurrentPoint(CanvasViewport).Position;
        BeginCanvasViewportInteraction();
        ApplyCanvasScale(center, factor);
        EndCanvasViewportInteraction();
        DismissCanvasHint();
        e.Handled = true;
    }

    private void OnCanvasManipulationStarted(object sender, ManipulationStartedRoutedEventArgs e)
    {
        if (_isMarqueeSelecting || _draggingNode is not null || _isCreatingConnection || _isPointerPanningMode)
        {
            e.Handled = true;
            return;
        }

        BeginCanvasViewportInteraction();
        DismissCanvasHint();
    }

    private void OnCanvasManipulationDelta(object sender, ManipulationDeltaRoutedEventArgs e)
    {
        if (_isMarqueeSelecting || _draggingNode is not null || _isCreatingConnection || _isPointerPanningMode)
        {
            e.Handled = true;
            return;
        }

        if (e.Delta.Scale != 0 && Math.Abs(e.Delta.Scale - 1) > 0.001)
        {
            ApplyCanvasScale(e.Position, e.Delta.Scale);
        }

        var translated = _canvasViewportEngine.TranslateAndClamp(
            new CanvasRuntime.CanvasViewportState(CanvasTransform.ScaleX, CanvasTransform.TranslateX, CanvasTransform.TranslateY),
            e.Delta.Translation.X,
            e.Delta.Translation.Y,
            _canvasWidth,
            _canvasHeight,
            CanvasViewport.ActualWidth,
            CanvasViewport.ActualHeight);
        CanvasTransform.TranslateX = translated.TranslateX;
        CanvasTransform.TranslateY = translated.TranslateY;
        e.Handled = true;
    }

    private void OnCanvasManipulationCompleted(object sender, ManipulationCompletedRoutedEventArgs e)
    {
        if (_isMarqueeSelecting || _draggingNode is not null || _isCreatingConnection || _isPointerPanningMode)
        {
            e.Handled = true;
            return;
        }

        EndCanvasViewportInteraction();
    }

    private void ApplyCanvasScale(Point center, double scaleFactor)
    {
        var current = new CanvasRuntime.CanvasViewportState(CanvasTransform.ScaleX, CanvasTransform.TranslateX, CanvasTransform.TranslateY);
        var next = _canvasViewportEngine.ApplyScale(current, center.X, center.Y, scaleFactor);
        if (Math.Abs(next.Scale - current.Scale) < 0.0001)
        {
            return;
        }

        CanvasTransform.ScaleX = next.Scale;
        CanvasTransform.ScaleY = next.Scale;
        CanvasTransform.TranslateX = next.TranslateX;
        CanvasTransform.TranslateY = next.TranslateY;
        ClampCanvasTransform();
        UpdateCanvasZoomIndicator();
    }

    private void OnCanvasZoomOutClick(object sender, RoutedEventArgs e)
    {
        var center = new Point(Math.Max(CanvasViewport.ActualWidth, 1) * 0.5, Math.Max(CanvasViewport.ActualHeight, 1) * 0.5);
        ApplyCanvasScale(center, 0.9);
        DismissCanvasHint();
    }

    private void OnCanvasZoomInClick(object sender, RoutedEventArgs e)
    {
        var center = new Point(Math.Max(CanvasViewport.ActualWidth, 1) * 0.5, Math.Max(CanvasViewport.ActualHeight, 1) * 0.5);
        ApplyCanvasScale(center, 1.1);
        DismissCanvasHint();
    }

    private void OnCanvasFitClick(object sender, RoutedEventArgs e)
    {
        FitCanvasToNodes();
        DismissCanvasHint();
    }

    private void OnCanvasZoomPercentTapped(object sender, TappedRoutedEventArgs e)
    {
        BeginZoomInlineEdit();
        e.Handled = true;
    }

    private void OnCanvasZoomEditTextBoxKeyDown(object sender, KeyRoutedEventArgs e)
    {
        if (e.Key == Windows.System.VirtualKey.Enter)
        {
            CommitZoomInlineEdit(applyChange: true);
            e.Handled = true;
            return;
        }

        if (e.Key == Windows.System.VirtualKey.Escape)
        {
            CommitZoomInlineEdit(applyChange: false);
            e.Handled = true;
        }
    }

    private void OnCanvasZoomEditTextBoxLostFocus(object sender, RoutedEventArgs e)
    {
        if (!_isZoomInlineEditing)
        {
            return;
        }

        CommitZoomInlineEdit(applyChange: true);
    }

    private void BeginZoomInlineEdit()
    {
        if (_isZoomInlineEditing)
        {
            return;
        }

        _isZoomInlineEditing = true;
        var currentPercent = Math.Round(Math.Max(CanvasTransform.ScaleX, 0.001) * 100);
        CanvasZoomTextBlock.Visibility = Visibility.Collapsed;
        CanvasZoomEditTextBox.Visibility = Visibility.Visible;
        CanvasZoomEditTextBox.Text = currentPercent.ToString("0");
        CanvasZoomEditTextBox.Focus(FocusState.Programmatic);
        CanvasZoomEditTextBox.SelectAll();
    }

    private void CommitZoomInlineEdit(bool applyChange)
    {
        if (!_isZoomInlineEditing)
        {
            return;
        }

        _isZoomInlineEditing = false;
        CanvasZoomEditTextBox.Visibility = Visibility.Collapsed;
        CanvasZoomTextBlock.Visibility = Visibility.Visible;

        if (!applyChange)
        {
            UpdateCanvasZoomIndicator();
            return;
        }

        var text = (CanvasZoomEditTextBox.Text ?? string.Empty).Trim().TrimEnd('%').Trim();
        if (!double.TryParse(text, out var percent) || double.IsNaN(percent) || double.IsInfinity(percent))
        {
            UpdateCanvasZoomIndicator();
            SetInlineStatus("请输入有效缩放值（60-240）。", InlineStatusTone.Error);
            return;
        }

        ApplyZoomPercent(percent);
    }

    private void ApplyZoomPercent(double percent)
    {
        var targetScale = Math.Clamp(percent / 100.0, CanvasMinScale, CanvasMaxScale);
        var center = new Point(
            Math.Max(CanvasViewport.ActualWidth, 1) * 0.5,
            Math.Max(CanvasViewport.ActualHeight, 1) * 0.5);
        var currentScale = Math.Max(CanvasTransform.ScaleX, 0.001);
        var factor = targetScale / currentScale;
        BeginCanvasViewportInteraction();
        ApplyCanvasScale(center, factor);
        EndCanvasViewportInteraction();
        DismissCanvasHint();
        SetInlineStatus($"缩放已设置为 {Math.Round(targetScale * 100):0}%。", InlineStatusTone.Success);
    }

    private void FitCanvasToNodes()
    {
        var allNodes = GetCanvasNodeBorders().ToList();
        var userNodes = allNodes
            .Where(x => x.Tag is CanvasNodeTag tag && tag.IsUserNode)
            .ToList();
        var nodes = userNodes.Count > 0 ? userNodes : allNodes;
        if (nodes.Count == 0)
        {
            ResetCanvasView();
            UpdateCanvasZoomIndicator();
            SetInlineStatus("画布为空，已重置视图。", InlineStatusTone.Neutral);
            return;
        }

        if (nodes.Count >= 4)
        {
            var centersX = nodes.Select(n =>
            {
                var l = Canvas.GetLeft(n);
                var w = n.ActualWidth > 0 ? n.ActualWidth : n.Width;
                return l + (Math.Max(w, 1) * 0.5);
            }).OrderBy(v => v).ToList();
            var centersY = nodes.Select(n =>
            {
                var t = Canvas.GetTop(n);
                var h = n.ActualHeight > 0 ? n.ActualHeight : Math.Max(n.MinHeight, 96);
                return t + (Math.Max(h, 1) * 0.5);
            }).OrderBy(v => v).ToList();
            var lowIndex = (int)Math.Floor((centersX.Count - 1) * 0.1);
            var highIndex = (int)Math.Ceiling((centersX.Count - 1) * 0.9);
            var minCx = centersX[Math.Clamp(lowIndex, 0, centersX.Count - 1)];
            var maxCx = centersX[Math.Clamp(highIndex, 0, centersX.Count - 1)];
            var minCy = centersY[Math.Clamp(lowIndex, 0, centersY.Count - 1)];
            var maxCy = centersY[Math.Clamp(highIndex, 0, centersY.Count - 1)];
            var filtered = nodes.Where(n =>
            {
                var l = Canvas.GetLeft(n);
                var t = Canvas.GetTop(n);
                var w = n.ActualWidth > 0 ? n.ActualWidth : n.Width;
                var h = n.ActualHeight > 0 ? n.ActualHeight : Math.Max(n.MinHeight, 96);
                var cx = l + (Math.Max(w, 1) * 0.5);
                var cy = t + (Math.Max(h, 1) * 0.5);
                return cx >= minCx && cx <= maxCx && cy >= minCy && cy <= maxCy;
            }).ToList();
            if (filtered.Count >= 2)
            {
                nodes = filtered;
            }
        }

        var minLeft = double.MaxValue;
        var minTop = double.MaxValue;
        var maxRight = 0.0;
        var maxBottom = 0.0;
        foreach (var node in nodes)
        {
            var left = Canvas.GetLeft(node);
            var top = Canvas.GetTop(node);
            var width = node.ActualWidth > 0 ? node.ActualWidth : node.Width;
            var height = node.ActualHeight > 0 ? node.ActualHeight : Math.Max(node.MinHeight, 96);
            minLeft = Math.Min(minLeft, left);
            minTop = Math.Min(minTop, top);
            maxRight = Math.Max(maxRight, left + width);
            maxBottom = Math.Max(maxBottom, top + height);
        }

        var viewportWidth = Math.Max(CanvasViewport.ActualWidth, 1.0);
        var viewportHeight = Math.Max(CanvasViewport.ActualHeight, 1.0);
        const double padding = 64;
        var fit = CanvasRuntime.CanvasFitCalculator.Calculate(
            minLeft,
            minTop,
            maxRight,
            maxBottom,
            viewportWidth,
            viewportHeight,
            CanvasMinScale,
            CanvasMaxScale,
            padding);
        CanvasTransform.ScaleX = fit.Scale;
        CanvasTransform.ScaleY = fit.Scale;
        CanvasTransform.TranslateX = fit.TranslateX;
        CanvasTransform.TranslateY = fit.TranslateY;
        UpdateCanvasZoomIndicator();
        SetInlineStatus("已定位至节点流。", InlineStatusTone.Success);
    }

    private void UpdateCanvasZoomIndicator()
    {
        if (CanvasZoomTextBlock is null)
        {
            return;
        }

        var scale = Math.Max(CanvasTransform.ScaleX, 0.001);
        CanvasZoomTextBlock.Text = $"{Math.Round(scale * 100):0}%";
    }

    private void BeginCanvasViewportInteraction()
    {
        _canvasInteractionSettleTimer?.Stop();
        CanvasGridLayer.Visibility = Visibility.Collapsed;
    }

    private void EndCanvasViewportInteraction()
    {
        if (_canvasInteractionSettleTimer is null)
        {
            CanvasGridLayer.Visibility = Visibility.Visible;
            return;
        }

        _canvasInteractionSettleTimer.Stop();
        _canvasInteractionSettleTimer.Start();
    }

    private static bool IsCanvasNodeSource(DependencyObject? source)
    {
        return ResolveNodeFromSource(source) is not null;
    }

    private void OnConnectionPointerPressed(object sender, PointerRoutedEventArgs e)
    {
        if (sender is not Line line)
        {
            return;
        }

        var edge = _connections.FirstOrDefault(x => x.Line == line);
        if (edge is null)
        {
            return;
        }

        SelectConnection(edge);
        e.Handled = true;
    }

    private void TryOpenArtifactFromNode(Border node)
    {
        if (node.Tag is not CanvasNodeTag tag)
        {
            return;
        }

        if (string.IsNullOrWhiteSpace(tag.ArtifactPath) || !File.Exists(tag.ArtifactPath))
        {
            return;
        }

        try
        {
            Process.Start(new ProcessStartInfo
            {
                FileName = tag.ArtifactPath,
                UseShellExecute = true
            });
            SetInlineStatus("已打开产物文件。", InlineStatusTone.Success);
        }
        catch (Exception ex)
        {
            SetInlineStatus($"打开文件失败：{ex.Message}", InlineStatusTone.Error);
        }
    }

    private static double SnapToGrid(double value)
    {
        return Math.Round(value / CanvasGridSize) * CanvasGridSize;
    }

    private void DismissCanvasHint()
    {
        CanvasHintPanel.Visibility = Visibility.Collapsed;
    }

    private void OnCanvasSplitHandlePointerPressed(object sender, PointerRoutedEventArgs e)
    {
        if (_isCanvasStacked)
        {
            return;
        }

        _isResizingCanvasPanels = true;
        _resizePointerId = e.Pointer.PointerId;
        _resizeStartX = e.GetCurrentPoint(CanvasSplitLayoutGrid).Position.X;
        _resizeStartLeftWidth = CanvasLeftColumn.ActualWidth;
        _resizeStartRightWidth = CanvasRightColumn.ActualWidth;
        CanvasSplitGrip.CapturePointer(e.Pointer);
        BeginSplitResizeInteraction();
        e.Handled = true;
    }

    private void OnCanvasSplitHandlePointerMoved(object sender, PointerRoutedEventArgs e)
    {
        if (!_isResizingCanvasPanels || e.Pointer.PointerId != _resizePointerId)
        {
            return;
        }

        var currentX = e.GetCurrentPoint(CanvasSplitLayoutGrid).Position.X;
        var delta = currentX - _resizeStartX;
        var total = _resizeStartLeftWidth + _resizeStartRightWidth;
        var minLeft = Math.Max(120, CanvasLeftColumn.MinWidth);
        var minRight = Math.Max(120, CanvasRightColumn.MinWidth);
        var calculated = CanvasRuntime.SplitLayoutController.CalculateColumns(
            total,
            _resizeStartLeftWidth,
            delta,
            minLeft,
            minRight);
        if (!calculated.IsValid)
        {
            return;
        }

        _pendingLeftWidth = calculated.Left;
        _pendingRightWidth = calculated.Right;
        _hasPendingPanelSplit = true;
        CanvasSplitGripTransform.TranslateX = calculated.Left - _resizeStartLeftWidth;
        e.Handled = true;
    }

    private void OnCanvasSplitHandlePointerReleased(object sender, PointerRoutedEventArgs e)
    {
        if (!_isResizingCanvasPanels || e.Pointer.PointerId != _resizePointerId)
        {
            return;
        }

        _isResizingCanvasPanels = false;
        _resizePointerId = 0;
        CanvasSplitGrip.ReleasePointerCapture(e.Pointer);
        ApplyPendingSplitResize();
        CanvasSplitGripTransform.TranslateX = 0;
        EndSplitResizeInteraction();
        e.Handled = true;
    }

    private void OnCanvasStackSplitHandlePointerPressed(object sender, PointerRoutedEventArgs e)
    {
        if (!_isCanvasStacked)
        {
            return;
        }

        _isResizingCanvasRows = true;
        _resizeRowPointerId = e.Pointer.PointerId;
        _resizeStartY = e.GetCurrentPoint(CanvasSplitLayoutGrid).Position.Y;
        _resizeStartTopHeight = CanvasLayoutRow0.ActualHeight;
        _resizeStartBottomHeight = CanvasLayoutRow2.ActualHeight;
        CanvasStackSplitGrip.CapturePointer(e.Pointer);
        BeginSplitResizeInteraction();
        e.Handled = true;
    }

    private void OnCanvasStackSplitHandlePointerMoved(object sender, PointerRoutedEventArgs e)
    {
        if (!_isResizingCanvasRows || e.Pointer.PointerId != _resizeRowPointerId)
        {
            return;
        }

        var currentY = e.GetCurrentPoint(CanvasSplitLayoutGrid).Position.Y;
        var delta = currentY - _resizeStartY;
        var total = _resizeStartTopHeight + _resizeStartBottomHeight;
        const double minTop = 240;
        const double minBottom = 200;
        var calculated = CanvasRuntime.SplitLayoutController.CalculateRows(
            total,
            _resizeStartTopHeight,
            delta,
            minTop,
            minBottom);
        if (!calculated.IsValid)
        {
            return;
        }

        _pendingTopHeight = calculated.Top;
        _pendingBottomHeight = calculated.Bottom;
        _hasPendingRowSplit = true;
        CanvasStackSplitGripTransform.TranslateY = calculated.Top - _resizeStartTopHeight;
        e.Handled = true;
    }

    private void OnCanvasStackSplitHandlePointerReleased(object sender, PointerRoutedEventArgs e)
    {
        if (!_isResizingCanvasRows || e.Pointer.PointerId != _resizeRowPointerId)
        {
            return;
        }

        _isResizingCanvasRows = false;
        _resizeRowPointerId = 0;
        CanvasStackSplitGrip.ReleasePointerCapture(e.Pointer);
        ApplyPendingSplitResize();
        CanvasStackSplitGripTransform.TranslateY = 0;
        EndSplitResizeInteraction();
        e.Handled = true;
    }

    private void BeginSplitResizeInteraction()
    {
        _hasPendingPanelSplit = false;
        _hasPendingRowSplit = false;
        CanvasGridLayer.Visibility = Visibility.Collapsed;
    }

    private void EndSplitResizeInteraction()
    {
        CanvasGridLayer.Visibility = Visibility.Visible;
    }

    private void ApplyPendingSplitResize()
    {
        if (_hasPendingPanelSplit)
        {
            CanvasLeftColumn.Width = new GridLength(_pendingLeftWidth, GridUnitType.Pixel);
            CanvasRightColumn.Width = new GridLength(_pendingRightWidth, GridUnitType.Pixel);
            _hasPendingPanelSplit = false;
        }

        if (_hasPendingRowSplit)
        {
            CanvasLayoutRow0.Height = new GridLength(_pendingTopHeight, GridUnitType.Pixel);
            CanvasLayoutRow2.Height = new GridLength(_pendingBottomHeight, GridUnitType.Pixel);
            _hasPendingRowSplit = false;
        }
    }

    private void EnsureCanvasExtentForViewportAndNodes()
    {
        var viewportWidth = Math.Max(CanvasViewport.ActualWidth, 1);
        var viewportHeight = Math.Max(CanvasViewport.ActualHeight, 1);
        var scale = Math.Max(CanvasTransform.ScaleX, 0.001);
        var viewLeft = (-CanvasTransform.TranslateX) / scale;
        var viewTop = (-CanvasTransform.TranslateY) / scale;
        var viewRight = (viewportWidth - CanvasTransform.TranslateX) / scale;
        var viewBottom = (viewportHeight - CanvasTransform.TranslateY) / scale;

        var extendLeft = viewLeft < CanvasExtendThreshold;
        var extendTop = viewTop < CanvasExtendThreshold;
        var extendRight = viewRight > _canvasWidth - CanvasExtendThreshold;
        var extendBottom = viewBottom > _canvasHeight - CanvasExtendThreshold;

        if (!extendLeft && !extendTop && !extendRight && !extendBottom)
        {
            return;
        }

        var shiftX = 0.0;
        var shiftY = 0.0;
        if (extendLeft && _canvasWidth < MaxCanvasWidth)
        {
            shiftX = Math.Min(CanvasExtendChunk, MaxCanvasWidth - _canvasWidth);
        }
        if (extendTop && _canvasHeight < MaxCanvasHeight)
        {
            shiftY = Math.Min(CanvasExtendChunk, MaxCanvasHeight - _canvasHeight);
        }

        var growRight = 0.0;
        if (extendRight && _canvasWidth < MaxCanvasWidth)
        {
            growRight = Math.Min(CanvasExtendChunk, MaxCanvasWidth - _canvasWidth - shiftX);
        }

        var growBottom = 0.0;
        if (extendBottom && _canvasHeight < MaxCanvasHeight)
        {
            growBottom = Math.Min(CanvasExtendChunk, MaxCanvasHeight - _canvasHeight - shiftY);
        }

        _canvasWidth += shiftX + growRight;
        _canvasHeight += shiftY + growBottom;

        if (shiftX > 0 || shiftY > 0)
        {
            foreach (var child in WorkspaceCanvas.Children)
            {
                if (child is not FrameworkElement element)
                {
                    continue;
                }

                Canvas.SetLeft(element, Canvas.GetLeft(element) + shiftX);
                Canvas.SetTop(element, Canvas.GetTop(element) + shiftY);
            }

            CanvasTransform.TranslateX -= shiftX * scale;
            CanvasTransform.TranslateY -= shiftY * scale;
        }

        ClampCanvasTransform();
        UpdateAllConnections();
        BuildCanvasGrid();
    }

    private void OnResetCanvasViewClick(object sender, RoutedEventArgs e)
    {
        if (IsSpaceClickSuppressed())
        {
            return;
        }

        FitCanvasToNodes();
        SetInlineStatus("已定位到节点流。", InlineStatusTone.Success);
    }

    private void ResetCanvasView()
    {
        CanvasTransform.ScaleX = 1;
        CanvasTransform.ScaleY = 1;
        CanvasTransform.TranslateX = 0;
        CanvasTransform.TranslateY = 0;
        UpdateCanvasZoomIndicator();
    }

    private void ClampCanvasTransform()
    {
        var viewportWidth = CanvasViewport.ActualWidth;
        var viewportHeight = CanvasViewport.ActualHeight;
        if (viewportWidth <= 0 || viewportHeight <= 0)
        {
            return;
        }

        var state = new CanvasRuntime.CanvasViewportState(CanvasTransform.ScaleX, CanvasTransform.TranslateX, CanvasTransform.TranslateY);
        var clamped = _canvasViewportEngine.ClampTranslation(state, _canvasWidth, _canvasHeight, viewportWidth, viewportHeight);
        CanvasTransform.ScaleX = clamped.Scale;
        CanvasTransform.ScaleY = clamped.Scale;
        CanvasTransform.TranslateX = clamped.TranslateX;
        CanvasTransform.TranslateY = clamped.TranslateY;
    }
}
