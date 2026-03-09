using System;
using System.Linq;
using Microsoft.UI.Xaml;
using Microsoft.UI.Xaml.Controls;
using Microsoft.UI.Xaml.Input;
using Microsoft.UI.Xaml.Shapes;
using Windows.Foundation;

namespace AIWF.Native;

public sealed partial class MainWindow
{
    private void InitializeCanvasContextMenus()
    {
        var centerItem = new MenuFlyoutItem { Text = "视图居中到此处" };
        centerItem.Click += OnCenterViewHereClick;
        var pasteItem = new MenuFlyoutItem { Text = "粘贴节点" };
        pasteItem.Click += OnPasteNodeClick;
        _canvasBlankFlyout.Items.Add(centerItem);
        _canvasBlankFlyout.Items.Add(new MenuFlyoutSeparator());
        _canvasBlankFlyout.Items.Add(pasteItem);
        _canvasBlankFlyout.Opening += (_, _) =>
        {
            pasteItem.IsEnabled = _copiedNodeTemplate is not null;
        };

        var copyNodeItem = new MenuFlyoutItem { Text = "复制节点" };
        copyNodeItem.Click += OnCopyNodeClick;
        var pasteNodeItem = new MenuFlyoutItem { Text = "粘贴节点" };
        pasteNodeItem.Click += OnPasteNodeClick;
        var deleteNodeItem = new MenuFlyoutItem { Text = "删除节点" };
        deleteNodeItem.Click += OnDeleteNodeFromContextClick;
        _canvasNodeFlyout.Items.Add(copyNodeItem);
        _canvasNodeFlyout.Items.Add(pasteNodeItem);
        _canvasNodeFlyout.Items.Add(new MenuFlyoutSeparator());
        _canvasNodeFlyout.Items.Add(deleteNodeItem);
        _canvasNodeFlyout.Opening += (_, _) =>
        {
            var canDelete = _contextNode?.Tag is CanvasNodeTag tag && tag.IsUserNode;
            pasteNodeItem.IsEnabled = _copiedNodeTemplate is not null;
            deleteNodeItem.IsEnabled = canDelete;
        };

        var deleteEdgeItem = new MenuFlyoutItem { Text = "删除连线" };
        deleteEdgeItem.Click += OnDeleteConnectionFromContextClick;
        _canvasConnectionFlyout.Items.Add(deleteEdgeItem);
        _canvasConnectionFlyout.Opening += (_, _) =>
        {
            deleteEdgeItem.IsEnabled = _contextConnection is not null;
        };
    }

    private void OnWorkspaceCanvasRightTapped(object sender, RightTappedRoutedEventArgs e)
    {
        if (IsCanvasNodeSource(e.OriginalSource as DependencyObject) || e.OriginalSource is Line)
        {
            return;
        }

        _contextNode = null;
        _contextConnection = null;
        _contextCanvasPoint = e.GetPosition(WorkspaceCanvas);
        _canvasBlankFlyout.ShowAt(CanvasViewport, new Microsoft.UI.Xaml.Controls.Primitives.FlyoutShowOptions
        {
            Position = e.GetPosition(CanvasViewport)
        });
        e.Handled = true;
    }

    private void OnCanvasNodeRightTapped(object sender, RightTappedRoutedEventArgs e)
    {
        if (sender is not Border node)
        {
            return;
        }

        _contextConnection = null;
        _contextNode = node;
        _contextCanvasPoint = e.GetPosition(WorkspaceCanvas);
        SelectNode(node);
        _canvasNodeFlyout.ShowAt(CanvasViewport, new Microsoft.UI.Xaml.Controls.Primitives.FlyoutShowOptions
        {
            Position = e.GetPosition(CanvasViewport)
        });
        e.Handled = true;
    }

    private void OnConnectionRightTapped(object sender, RightTappedRoutedEventArgs e)
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

        _contextNode = null;
        _contextConnection = edge;
        _contextCanvasPoint = e.GetPosition(WorkspaceCanvas);
        SelectConnection(edge);
        _canvasConnectionFlyout.ShowAt(CanvasViewport, new Microsoft.UI.Xaml.Controls.Primitives.FlyoutShowOptions
        {
            Position = e.GetPosition(CanvasViewport)
        });
        e.Handled = true;
    }

    private void OnCenterViewHereClick(object sender, RoutedEventArgs e)
    {
        CenterCanvasViewOn(_contextCanvasPoint);
    }

    private void CenterCanvasViewOn(Point targetCanvasPoint)
    {
        var scale = Math.Max(CanvasTransform.ScaleX, 0.001);
        var viewportWidth = Math.Max(CanvasViewport.ActualWidth, 1);
        var viewportHeight = Math.Max(CanvasViewport.ActualHeight, 1);
        CanvasTransform.TranslateX = (viewportWidth * 0.5) - (targetCanvasPoint.X * scale);
        CanvasTransform.TranslateY = (viewportHeight * 0.5) - (targetCanvasPoint.Y * scale);
        ClampCanvasTransform();
    }

    private void OnCopyNodeClick(object sender, RoutedEventArgs e)
    {
        if (_contextNode?.Tag is not CanvasNodeTag tag)
        {
            return;
        }

        _copiedNodeTemplate = new CopiedNodeTemplate
        {
            Title = tag.TitleBlock?.Text ?? string.Empty,
            Subtitle = tag.SubtitleBlock?.Text ?? string.Empty
        };
        SetInlineStatus("已复制节点。", InlineStatusTone.Success);
    }

    private void OnPasteNodeClick(object sender, RoutedEventArgs e)
    {
        if (_copiedNodeTemplate is null)
        {
            return;
        }

        var title = string.IsNullOrWhiteSpace(_copiedNodeTemplate.Title) ? $"节点 {_customNodeCounter}" : _copiedNodeTemplate.Title;
        var subtitle = _copiedNodeTemplate.Subtitle ?? string.Empty;
        _customNodeCounter++;
        AddCanvasNode(
            $"node-{Guid.NewGuid():N}",
            title,
            subtitle,
            Math.Max(0, SnapToGrid(_contextCanvasPoint.X - 110)),
            Math.Max(0, SnapToGrid(_contextCanvasPoint.Y - 48)),
            isUserNode: true,
            select: true,
            dismissHint: true,
            ensureExtent: true,
            requestAutosave: true);
        SetInlineStatus("已粘贴节点。", InlineStatusTone.Success);
    }

    private void OnDeleteNodeFromContextClick(object sender, RoutedEventArgs e)
    {
        if (_contextNode is null)
        {
            return;
        }

        SelectNode(_contextNode);
        DeleteSelectedUserNode();
    }

    private void OnDeleteConnectionFromContextClick(object sender, RoutedEventArgs e)
    {
        if (_contextConnection is null)
        {
            return;
        }

        SelectConnection(_contextConnection);
        DeleteSelectedConnection();
    }

    private void OnDeleteNodeClick(object sender, RoutedEventArgs e)
    {
        DeleteSelectedUserNode();
    }

    private void OnDeleteConnectionClick(object sender, RoutedEventArgs e)
    {
        DeleteSelectedConnection();
    }
}
