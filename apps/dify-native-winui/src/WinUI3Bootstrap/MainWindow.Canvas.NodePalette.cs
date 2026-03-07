using System;
using Microsoft.UI.Xaml;
using Microsoft.UI.Xaml.Controls;
using Windows.Foundation;

namespace AIWF.Native;

public sealed partial class MainWindow
{
    private void OnAddNodeClick(object sender, RoutedEventArgs e)
    {
        PrepareAddNodeFlyoutLayout();
        var viewportWidth = Math.Max(CanvasViewport.ActualWidth, 360);
        var viewportHeight = Math.Max(CanvasViewport.ActualHeight, 280);
        var popupWidth = _addNodeFlyoutRoot?.Width ?? 500;
        var popupHeight = _addNodeFlyoutRoot?.Height > 0 ? _addNodeFlyoutRoot.Height : (_addNodeFlyoutRoot?.MaxHeight ?? 560);
        var anchor = new Point(12, 12);
        if (sender is FrameworkElement element)
        {
            try
            {
                var transform = element.TransformToVisual(CanvasViewport);
                anchor = transform.TransformPoint(new Point(0, element.ActualHeight + 6));
            }
            catch
            {
                anchor = new Point(12, 12);
            }
        }

        var x = Math.Clamp(anchor.X, 8, Math.Max(8, viewportWidth - popupWidth - 8));
        var y = Math.Clamp(anchor.Y, 8, Math.Max(8, viewportHeight - popupHeight - 8));
        _addNodeFlyout.ShowAt(CanvasViewport, new Microsoft.UI.Xaml.Controls.Primitives.FlyoutShowOptions
        {
            Position = new Point(x, y)
        });
    }

    private void PrepareAddNodeFlyoutLayout()
    {
        if (_addNodeFlyoutRoot is null)
        {
            return;
        }

        var viewportWidth = Math.Max(CanvasViewport.ActualWidth, 360);
        var viewportHeight = Math.Max(CanvasViewport.ActualHeight, 280);
        var targetWidth = Math.Clamp(viewportWidth - 16, 420, 620);
        var targetHeight = Math.Clamp(viewportHeight - 16, 260, 760);
        _addNodeFlyoutRoot.Width = targetWidth;
        _addNodeFlyoutRoot.MaxHeight = targetHeight;
        _addNodeFlyoutRoot.Height = targetHeight;

        var stackWidth = Math.Max(320, targetWidth - 44);
        if (_addNodeFlyoutStack is not null)
        {
            _addNodeFlyoutStack.Width = stackWidth;
        }

        if (_addNodeFlyoutScroller is not null)
        {
            _addNodeFlyoutScroller.Width = Math.Max(320, targetWidth - 20);
            _addNodeFlyoutScroller.Height = Math.Max(140, targetHeight - 86);
        }

        for (var i = 0; i < _addNodeFlyoutGroupGrids.Count; i++)
        {
            _addNodeFlyoutGroupGrids[i].Width = stackWidth;
        }
    }

    private void OnAddNodeTemplateClick(object sender, RoutedEventArgs e)
    {
        if (sender is not FrameworkElement { Tag: NodeTemplate template })
        {
            return;
        }

        _addNodeFlyout.Hide();
        SpawnNodeFromTemplate(template);
    }

    private void SpawnNodeFromTemplate(NodeTemplate template)
    {
        var (centerX, centerY) = GetCanvasViewportCenter();
        var x = SnapToGrid(centerX - 110);
        var y = SnapToGrid(centerY - 48);

        if (Math.Abs(_lastNodeSpawnPoint.X - x) < 1 && Math.Abs(_lastNodeSpawnPoint.Y - y) < 1)
        {
            x = SnapToGrid(x + 40);
            y = SnapToGrid(y + 40);
        }

        _lastNodeSpawnPoint = new Point(x, y);
        var isBlank = string.Equals(template.KeyPrefix, "custom", StringComparison.OrdinalIgnoreCase);
        var title = isBlank ? $"节点 {_customNodeCounter++}" : template.Title;
        var subtitle = isBlank ? "请输入说明" : template.Subtitle;
        var keyPrefix = string.IsNullOrWhiteSpace(template.KeyPrefix) ? "node" : template.KeyPrefix;
        AddCanvasNode(
            $"{keyPrefix}-{Guid.NewGuid():N}",
            title,
            subtitle,
            x,
            y,
            isUserNode: true,
            select: true,
            dismissHint: true,
            ensureExtent: true,
            requestAutosave: true);
    }

    private (double x, double y) GetCanvasViewportCenter()
    {
        var viewportWidth = Math.Max(CanvasViewport.ActualWidth, 600);
        var viewportHeight = Math.Max(CanvasViewport.ActualHeight, 420);
        var scale = Math.Max(CanvasTransform.ScaleX, 0.001);
        var centerX = ((viewportWidth * 0.5) - CanvasTransform.TranslateX) / scale;
        var centerY = ((viewportHeight * 0.5) - CanvasTransform.TranslateY) / scale;
        return (centerX, centerY);
    }
}
