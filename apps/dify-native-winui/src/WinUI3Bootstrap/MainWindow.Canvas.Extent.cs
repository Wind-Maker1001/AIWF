using Microsoft.UI.Xaml;
using Microsoft.UI.Xaml.Controls;
using Windows.Foundation;

namespace AIWF.Native;

public sealed partial class MainWindow
{
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
