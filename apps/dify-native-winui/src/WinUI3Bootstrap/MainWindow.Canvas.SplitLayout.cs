using Microsoft.UI.Xaml;
using Microsoft.UI.Xaml.Controls;
using Microsoft.UI.Xaml.Input;

namespace AIWF.Native;

public sealed partial class MainWindow
{
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
}
