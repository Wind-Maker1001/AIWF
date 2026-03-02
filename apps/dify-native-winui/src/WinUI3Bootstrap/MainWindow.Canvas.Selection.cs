using Microsoft.UI.Xaml;
using Microsoft.UI.Xaml.Controls;
using Microsoft.UI.Xaml.Media;
using AIWF.Native.Runtime;

namespace AIWF.Native;

public sealed partial class MainWindow
{
    private static Border? ResolveNodeFromSource(DependencyObject? source)
    {
        var current = source;
        while (current is not null)
        {
            if (current is Border border && border.Tag is CanvasNodeTag)
            {
                return border;
            }

            current = VisualTreeHelper.GetParent(current);
        }

        return null;
    }

    private void SelectNode(Border? node)
    {
        _selectedNode = node;
        _multiSelectedNodes.Clear();
        _selectedConnection = null;
        ApplyNodeSelectionVisuals();
        UpdateConnectionVisuals();
        UpdateNodePropertyPanel();
    }

    private void ApplyNodeSelectionVisuals()
    {
        foreach (var child in WorkspaceCanvas.Children)
        {
            if (child is not Border border || border.Tag is not CanvasNodeTag)
            {
                continue;
            }

            var isActive = border == _selectedNode || _multiSelectedNodes.Contains(border);
            var visual = CanvasSelectionPresenter.ResolveNode(isActive);
            border.BorderThickness = new Thickness(visual.BorderThickness);
            border.BorderBrush = new SolidColorBrush(visual.BorderColor);
        }
    }

    private void SelectConnection(ConnectionEdge? edge)
    {
        _selectedConnection = edge;
        _selectedNode = null;
        _multiSelectedNodes.Clear();
        ApplyNodeSelectionVisuals();
        UpdateConnectionVisuals();
        UpdateNodePropertyPanel();
    }

    private void UpdateConnectionVisuals()
    {
        foreach (var edge in _connections)
        {
            var isActive = edge == _selectedConnection;
            var visual = CanvasSelectionPresenter.ResolveConnection(isActive);
            edge.Line.StrokeThickness = visual.StrokeThickness;
            edge.Line.Stroke = new SolidColorBrush(visual.StrokeColor);
        }
    }

    private void UpdateNodePropertyPanel()
    {
        if (_selectedConnection is not null)
        {
            CanvasSelectionInfoTextBlock.Text = "已选中连线";
            NodeTitleTextBox.Text = string.Empty;
            NodeSubtitleTextBox.Text = string.Empty;
            NodeTitleTextBox.IsEnabled = false;
            NodeSubtitleTextBox.IsEnabled = false;
            DeleteNodeButton.IsEnabled = false;
            DeleteConnectionButton.IsEnabled = true;
            return;
        }

        if (_multiSelectedNodes.Count > 0)
        {
            CanvasSelectionInfoTextBlock.Text = $"已框选：{_multiSelectedNodes.Count} 个节点";
            NodeTitleTextBox.Text = string.Empty;
            NodeSubtitleTextBox.Text = string.Empty;
            NodeTitleTextBox.IsEnabled = false;
            NodeSubtitleTextBox.IsEnabled = false;
            DeleteNodeButton.IsEnabled = true;
            DeleteConnectionButton.IsEnabled = false;
            return;
        }

        if (_selectedNode?.Tag is not CanvasNodeTag tag)
        {
            CanvasSelectionInfoTextBlock.Text = "未选中内容";
            NodeTitleTextBox.Text = string.Empty;
            NodeSubtitleTextBox.Text = string.Empty;
            NodeTitleTextBox.IsEnabled = false;
            NodeSubtitleTextBox.IsEnabled = false;
            DeleteNodeButton.IsEnabled = false;
            DeleteConnectionButton.IsEnabled = false;
            return;
        }

        CanvasSelectionInfoTextBlock.Text = $"已选中：{tag.NodeKey}";
        NodeTitleTextBox.Text = tag.TitleBlock?.Text ?? string.Empty;
        NodeSubtitleTextBox.Text = tag.SubtitleBlock?.Text ?? string.Empty;
        NodeTitleTextBox.IsEnabled = true;
        NodeSubtitleTextBox.IsEnabled = true;
        DeleteNodeButton.IsEnabled = tag.IsUserNode;
        DeleteConnectionButton.IsEnabled = false;
    }
}
