using AIWF.Native.Runtime;
using Microsoft.UI.Xaml;
using Microsoft.UI.Xaml.Controls;
using Microsoft.UI.Xaml.Media;

namespace AIWF.Native;

public sealed partial class MainWindow
{
    private void ResetCanvasSelectionState()
    {
        _selectedNode = null;
        _selectedConnection = null;
        _multiSelectedNodes.Clear();
        _highlightedNodes.Clear();
        _lastHighlightedConnection = null;
        _contextNode = null;
        _contextConnection = null;
    }

    private void DetachNodeFromSelection(Border node)
    {
        _multiSelectedNodes.Remove(node);
        _highlightedNodes.Remove(node);
        if (_selectedNode == node)
        {
            _selectedNode = null;
        }

        if (_contextNode == node)
        {
            _contextNode = null;
        }
    }

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
        var desiredSelection = new HashSet<Border>();
        if (_selectedNode is not null)
        {
            desiredSelection.Add(_selectedNode);
        }

        foreach (var node in _multiSelectedNodes)
        {
            desiredSelection.Add(node);
        }

        var delta = CanvasRuntime.CanvasSelectionDiff.Calculate(_highlightedNodes, desiredSelection);
        if (delta.Activated.Count == 0 && delta.Deactivated.Count == 0)
        {
            return;
        }

        foreach (var node in delta.Deactivated)
        {
            ApplyNodeSelectionVisual(node, active: false);
        }

        foreach (var node in delta.Activated)
        {
            ApplyNodeSelectionVisual(node, active: true);
        }

        _highlightedNodes.Clear();
        foreach (var node in desiredSelection)
        {
            _highlightedNodes.Add(node);
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
        if (ReferenceEquals(_lastHighlightedConnection, _selectedConnection))
        {
            return;
        }

        if (_lastHighlightedConnection is not null)
        {
            ApplyConnectionSelectionVisual(_lastHighlightedConnection, active: false);
        }

        if (_selectedConnection is not null)
        {
            ApplyConnectionSelectionVisual(_selectedConnection, active: true);
        }

        _lastHighlightedConnection = _selectedConnection;
    }

    private static void ApplyNodeSelectionVisual(Border border, bool active)
    {
        var visual = active ? ActiveNodeSelectionVisual : InactiveNodeSelectionVisual;
        border.BorderThickness = new Thickness(visual.BorderThickness);
        border.BorderBrush = active ? ActiveNodeSelectionBrush : InactiveNodeSelectionBrush;
    }

    private static void ApplyConnectionSelectionVisual(ConnectionEdge edge, bool active)
    {
        var visual = active ? ActiveConnectionSelectionVisual : InactiveConnectionSelectionVisual;
        edge.Line.StrokeThickness = visual.StrokeThickness;
        edge.Line.Stroke = active ? ActiveConnectionSelectionBrush : InactiveConnectionSelectionBrush;
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
            RenderSelectedNodeWorkflowEditor(null);
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
            RenderSelectedNodeWorkflowEditor(null);
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
            RenderSelectedNodeWorkflowEditor(null);
            return;
        }

        CanvasSelectionInfoTextBlock.Text = $"已选中：{tag.NodeKey}";
        NodeTitleTextBox.Text = tag.TitleBlock?.Text ?? string.Empty;
        NodeSubtitleTextBox.Text = tag.SubtitleBlock?.Text ?? string.Empty;
        NodeTitleTextBox.IsEnabled = true;
        NodeSubtitleTextBox.IsEnabled = true;
        DeleteNodeButton.IsEnabled = tag.IsUserNode;
        DeleteConnectionButton.IsEnabled = false;
        RenderSelectedNodeWorkflowEditor(tag);
    }
}
