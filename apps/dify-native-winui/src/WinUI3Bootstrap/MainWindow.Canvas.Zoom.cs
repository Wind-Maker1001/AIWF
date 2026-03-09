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
}
