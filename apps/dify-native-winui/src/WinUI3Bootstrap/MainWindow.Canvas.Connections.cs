using System;
using System.Linq;
using Microsoft.UI.Xaml;
using Microsoft.UI.Xaml.Controls;
using Microsoft.UI.Xaml.Input;
using Microsoft.UI.Xaml.Media;
using Microsoft.UI.Xaml.Shapes;
using Windows.Foundation;

namespace AIWF.Native;

public sealed partial class MainWindow
{
    private void OnConnectorPointerPressed(object sender, PointerRoutedEventArgs e)
    {
        if (sender is not Ellipse ellipse || ellipse.Tag is not ConnectorTag connectorTag)
        {
            return;
        }

        if (connectorTag.Kind != "out")
        {
            return;
        }

        BeginConnection(connectorTag.Node, e);
    }

    private void BeginConnection(Border sourceNode, PointerRoutedEventArgs e)
    {
        CancelConnectionPreview();
        _isCreatingConnection = true;
        _connectionSourceNode = sourceNode;
        _connectionPointerId = e.Pointer.PointerId;
        SelectNode(sourceNode);
        DismissCanvasHint();

        var start = GetNodeOutputPoint(sourceNode);
        var pointer = e.GetCurrentPoint(WorkspaceCanvas).Position;
        _connectionPreviewLine = new Line
        {
            X1 = start.X,
            Y1 = start.Y,
            X2 = pointer.X,
            Y2 = pointer.Y,
            StrokeThickness = 2,
            Stroke = new SolidColorBrush(Windows.UI.Color.FromArgb(0xFF, 0xC6, 0x28, 0x28)),
            StrokeDashArray = new DoubleCollection { 4, 3 },
            IsHitTestVisible = false
        };
        Canvas.SetZIndex(_connectionPreviewLine, 2);
        WorkspaceCanvas.Children.Add(_connectionPreviewLine);
        CanvasViewport.CapturePointer(e.Pointer);
        e.Handled = true;
    }

    private void TryCompleteConnection(PointerRoutedEventArgs e)
    {
        if (!_isCreatingConnection || _connectionSourceNode is null)
        {
            return;
        }

        var target = ResolveNodeFromSource(e.OriginalSource as DependencyObject);
        if (target is not null && target != _connectionSourceNode)
        {
            AddConnection(_connectionSourceNode, target);
        }

        CancelConnectionPreview();
        e.Handled = true;
    }

    private void CancelConnectionPreview()
    {
        if (_connectionPreviewLine is not null)
        {
            WorkspaceCanvas.Children.Remove(_connectionPreviewLine);
            _connectionPreviewLine = null;
        }

        _isCreatingConnection = false;
        _connectionSourceNode = null;
        _connectionPointerId = 0;
    }

    private void AddConnection(Border source, Border target, bool select = true)
    {
        foreach (var edge in _connections)
        {
            if (edge.Source == source && edge.Target == target)
            {
                return;
            }
        }

        var line = new Line
        {
            StrokeThickness = 2,
            Stroke = new SolidColorBrush(Windows.UI.Color.FromArgb(0xCC, 0x11, 0x11, 0x11)),
            IsHitTestVisible = true
        };
        line.PointerPressed += OnConnectionPointerPressed;
        line.RightTapped += OnConnectionRightTapped;
        Canvas.SetZIndex(line, 1);
        WorkspaceCanvas.Children.Add(line);
        var edgeItem = new ConnectionEdge
        {
            Source = source,
            Target = target,
            Line = line
        };
        _connections.Add(edgeItem);
        if (select)
        {
            SelectConnection(edgeItem);
        }

        UpdateAllConnections();
        RequestCanvasAutosave();
    }

    private void RemoveConnectionsForNode(Border node)
    {
        var removedAny = false;
        for (var i = _connections.Count - 1; i >= 0; i--)
        {
            var edge = _connections[i];
            if (edge.Source != node && edge.Target != node)
            {
                continue;
            }

            if (_selectedConnection == edge)
            {
                _selectedConnection = null;
            }

            WorkspaceCanvas.Children.Remove(edge.Line);
            _connections.RemoveAt(i);
            removedAny = true;
        }

        UpdateConnectionVisuals();
        UpdateNodePropertyPanel();
        if (removedAny)
        {
            RequestCanvasAutosave();
        }
    }

    private void UpdateAllConnections()
    {
        foreach (var edge in _connections)
        {
            var start = GetNodeOutputPoint(edge.Source);
            var end = GetNodeInputPoint(edge.Target);
            edge.Line.X1 = start.X;
            edge.Line.Y1 = start.Y;
            edge.Line.X2 = end.X;
            edge.Line.Y2 = end.Y;
        }
    }

    private static Point GetNodeOutputPoint(Border node)
    {
        var left = Canvas.GetLeft(node);
        var top = Canvas.GetTop(node);
        var width = node.ActualWidth > 0 ? node.ActualWidth : node.Width;
        var height = node.ActualHeight > 0 ? node.ActualHeight : Math.Max(node.MinHeight, 96);
        return new Point(left + width, top + (height * 0.5));
    }

    private static Point GetNodeInputPoint(Border node)
    {
        var left = Canvas.GetLeft(node);
        var top = Canvas.GetTop(node);
        var height = node.ActualHeight > 0 ? node.ActualHeight : Math.Max(node.MinHeight, 96);
        return new Point(left, top + (height * 0.5));
    }

    private bool DeleteSelectedConnection()
    {
        if (_selectedConnection is null)
        {
            return false;
        }

        WorkspaceCanvas.Children.Remove(_selectedConnection.Line);
        _connections.Remove(_selectedConnection);
        _selectedConnection = null;
        UpdateConnectionVisuals();
        UpdateNodePropertyPanel();
        SetInlineStatus("已删除连线。", InlineStatusTone.Success);
        RequestCanvasAutosave();
        return true;
    }

    private bool DeleteSelectedUserNode()
    {
        if (_multiSelectedNodes.Count > 0)
        {
            var removed = 0;
            foreach (var node in _multiSelectedNodes.ToList())
            {
                if (node.Tag is not CanvasNodeTag multiTag || !multiTag.IsUserNode)
                {
                    continue;
                }

                RemoveConnectionsForNode(node);
                WorkspaceCanvas.Children.Remove(node);
                removed++;
            }

            _multiSelectedNodes.Clear();
            _selectedNode = null;
            ApplyNodeSelectionVisuals();
            UpdateNodePropertyPanel();
            if (removed > 0)
            {
                SetInlineStatus($"已删除 {removed} 个节点。", InlineStatusTone.Success);
                RequestCanvasAutosave();
                return true;
            }
        }

        if (_selectedNode is null || _selectedNode.Tag is not CanvasNodeTag tag || !tag.IsUserNode)
        {
            return false;
        }

        RemoveConnectionsForNode(_selectedNode);
        WorkspaceCanvas.Children.Remove(_selectedNode);
        _selectedNode = null;
        UpdateNodePropertyPanel();
        SetInlineStatus("已删除节点。", InlineStatusTone.Success);
        RequestCanvasAutosave();
        return true;
    }
}
