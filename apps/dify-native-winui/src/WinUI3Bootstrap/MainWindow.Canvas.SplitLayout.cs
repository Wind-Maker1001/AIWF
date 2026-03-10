using Microsoft.UI.Xaml;
using Microsoft.UI.Xaml.Controls;
using Microsoft.UI.Xaml.Input;
using Microsoft.UI.Xaml.Media;
using System;
using System.Runtime.InteropServices;

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
        ApplyVerticalSplitHandleVisual(hovered: true, dragging: true);
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
        ApplyVerticalSplitHandleVisual(hovered: false, dragging: false);
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
        ApplyHorizontalSplitHandleVisual(hovered: true, dragging: true);
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
        ApplyHorizontalSplitHandleVisual(hovered: false, dragging: false);
        e.Handled = true;
    }

    private void OnCanvasSplitHandlePointerEntered(object sender, PointerRoutedEventArgs e)
    {
        if (_isCanvasStacked)
        {
            return;
        }

        ApplyVerticalSplitHandleVisual(hovered: true, dragging: _isResizingCanvasPanels);
        SetNativeCursor(CursorSizeWE);
    }

    private void OnCanvasSplitHandlePointerExited(object sender, PointerRoutedEventArgs e)
    {
        if (_isResizingCanvasPanels)
        {
            return;
        }

        SetNativeCursor(CursorArrow);
        ApplyVerticalSplitHandleVisual(hovered: false, dragging: false);
    }

    private void OnCanvasStackSplitHandlePointerEntered(object sender, PointerRoutedEventArgs e)
    {
        if (!_isCanvasStacked)
        {
            return;
        }

        ApplyHorizontalSplitHandleVisual(hovered: true, dragging: _isResizingCanvasRows);
        SetNativeCursor(CursorSizeNS);
    }

    private void OnCanvasStackSplitHandlePointerExited(object sender, PointerRoutedEventArgs e)
    {
        if (_isResizingCanvasRows)
        {
            return;
        }

        SetNativeCursor(CursorArrow);
        ApplyHorizontalSplitHandleVisual(hovered: false, dragging: false);
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

    private void ApplyVerticalSplitHandleVisual(bool hovered, bool dragging)
    {
        CanvasSplitGrip.Background = new SolidColorBrush(
            dragging
                ? Windows.UI.Color.FromArgb(0x22, 0xD7, 0x26, 0x2E)
                : hovered
                    ? Windows.UI.Color.FromArgb(0x14, 0x21, 0x24, 0x2B)
                    : Windows.UI.Color.FromArgb(0x00, 0x00, 0x00, 0x00));
        CanvasSplitTrack.Width = dragging ? 3.5 : hovered ? 3 : 1.5;
        CanvasSplitTrack.Background = new SolidColorBrush(
            dragging
                ? Windows.UI.Color.FromArgb(0xCC, 0xB5, 0x1C, 0x23)
                : hovered
                    ? Windows.UI.Color.FromArgb(0xAA, 0x52, 0x5A, 0x66)
                    : Windows.UI.Color.FromArgb(0x2A, 0x6B, 0x72, 0x80));
        CanvasSplitDots.Opacity = dragging ? 1 : hovered ? 0.95 : 0.5;
        CanvasSplitDots.Spacing = dragging ? 5 : hovered ? 4.5 : 4;
    }

    private void ApplyHorizontalSplitHandleVisual(bool hovered, bool dragging)
    {
        CanvasStackSplitGrip.Background = new SolidColorBrush(
            dragging
                ? Windows.UI.Color.FromArgb(0x22, 0xD7, 0x26, 0x2E)
                : hovered
                    ? Windows.UI.Color.FromArgb(0x14, 0x21, 0x24, 0x2B)
                    : Windows.UI.Color.FromArgb(0x00, 0x00, 0x00, 0x00));
        CanvasStackSplitTrack.Height = dragging ? 3.5 : hovered ? 3 : 1.5;
        CanvasStackSplitTrack.Background = new SolidColorBrush(
            dragging
                ? Windows.UI.Color.FromArgb(0xCC, 0xB5, 0x1C, 0x23)
                : hovered
                    ? Windows.UI.Color.FromArgb(0xAA, 0x52, 0x5A, 0x66)
                    : Windows.UI.Color.FromArgb(0x2A, 0x6B, 0x72, 0x80));
        CanvasStackSplitDots.Opacity = dragging ? 1 : hovered ? 0.95 : 0.5;
        CanvasStackSplitDots.Spacing = dragging ? 5 : hovered ? 4.5 : 4;
    }

    private const int CursorArrow = 32512;
    private const int CursorSizeWE = 32644;
    private const int CursorSizeNS = 32645;

    private static void SetNativeCursor(int cursorId)
    {
        var cursor = LoadCursor(IntPtr.Zero, cursorId);
        if (cursor != IntPtr.Zero)
        {
            SetCursor(cursor);
        }
    }

    [DllImport("user32.dll", SetLastError = true)]
    private static extern IntPtr LoadCursor(IntPtr hInstance, int lpCursorName);

    [DllImport("user32.dll", SetLastError = true)]
    private static extern IntPtr SetCursor(IntPtr hCursor);

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
