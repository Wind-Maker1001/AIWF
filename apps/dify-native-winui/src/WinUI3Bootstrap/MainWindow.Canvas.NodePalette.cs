using System;
using Microsoft.UI.Xaml;
using Microsoft.UI.Xaml.Controls;
using Microsoft.UI.Xaml.Media.Animation;
using Windows.Foundation;

namespace AIWF.Native;

public sealed partial class MainWindow
{
    private void OnAddNodeClick(object sender, RoutedEventArgs e)
    {
        ToggleNodeLibraryDrawer();
    }

    private void OnAddNodeTemplateClick(object sender, RoutedEventArgs e)
    {
        if (sender is not FrameworkElement { Tag: NodeTemplate template })
        {
            return;
        }

        SetNodeLibraryOpen(false);
        SpawnNodeFromTemplate(template);
    }

    private void OnNodeLibraryCloseClick(object sender, RoutedEventArgs e)
    {
        SetNodeLibraryOpen(false);
    }

    private void OnNodeLibrarySearchTextChanged(object sender, TextChangedEventArgs e)
    {
        RebuildNodeLibraryItems(NodeLibrarySearchTextBox?.Text);
    }

    private void ToggleNodeLibraryDrawer()
    {
        SetNodeLibraryOpen(!_isNodeLibraryOpen);
    }

    private void SetNodeLibraryOpen(bool isOpen)
    {
        if (NodeLibraryPane is null)
        {
            return;
        }

        if (isOpen == _isNodeLibraryOpen
            && NodeLibraryPane.Visibility == (isOpen ? Visibility.Visible : Visibility.Collapsed))
        {
            return;
        }

        _isNodeLibraryOpen = isOpen;
        var targetWidth = isOpen ? CalculateNodeLibraryWidth() : 0;
        var fromWidth = NodeLibraryPane.ActualWidth > 0 ? NodeLibraryPane.ActualWidth : NodeLibraryPane.Width;
        var fromOpacity = NodeLibraryPane.Opacity;
        var toOpacity = isOpen ? 1.0 : 0.0;

        if (isOpen)
        {
            NodeLibraryPane.Visibility = Visibility.Visible;
            if (NodeLibraryItemsPanel is not null && NodeLibraryItemsPanel.Children.Count == 0)
            {
                InitializeNodeLibraryDrawer();
            }
        }

        var storyboard = new Storyboard();
        var widthAnimation = new DoubleAnimation
        {
            From = fromWidth,
            To = targetWidth,
            Duration = TimeSpan.FromMilliseconds(170),
            EnableDependentAnimation = true
        };
        Storyboard.SetTarget(widthAnimation, NodeLibraryPane);
        Storyboard.SetTargetProperty(widthAnimation, "Width");
        storyboard.Children.Add(widthAnimation);

        var opacityAnimation = new DoubleAnimation
        {
            From = fromOpacity,
            To = toOpacity,
            Duration = TimeSpan.FromMilliseconds(140),
            EnableDependentAnimation = true
        };
        Storyboard.SetTarget(opacityAnimation, NodeLibraryPane);
        Storyboard.SetTargetProperty(opacityAnimation, "Opacity");
        storyboard.Children.Add(opacityAnimation);

        storyboard.Completed += (_, _) =>
        {
            NodeLibraryPane.Width = targetWidth;
            NodeLibraryPane.Opacity = toOpacity;
            NodeLibraryPane.Visibility = isOpen ? Visibility.Visible : Visibility.Collapsed;
            if (isOpen)
            {
                NodeLibrarySearchTextBox?.Focus(FocusState.Programmatic);
                NodeLibrarySearchTextBox?.SelectAll();
            }
            FitCanvasToNodes();
        };
        storyboard.Begin();
    }

    private double CalculateNodeLibraryWidth()
    {
        var availableWidth = Math.Max(CanvasEditorPane.ActualWidth, 720);
        var preferred = availableWidth * 0.29;
        return Math.Clamp(preferred, NodeLibraryMinWidth, NodeLibraryMaxWidth);
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
