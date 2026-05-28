using System.Text.Json.Nodes;
using AIWF.Native.Runtime;
using Microsoft.UI.Xaml;
using Microsoft.UI.Xaml.Controls;

namespace AIWF.Native;

public sealed partial class MainWindow
{
    private void InitializeWorkflowDebugShellSection()
    {
        WorkflowDebugShellBorder.Visibility = IsWorkflowDebugShellEnabled
            ? Visibility.Visible
            : Visibility.Collapsed;
    }

    private void OnToggleWorkflowDebugLinkClick(object sender, RoutedEventArgs e)
    {
        if (!IsWorkflowDebugShellEnabled)
        {
            return;
        }

        var fromId = (WorkflowDebugLinkFromTextBox.Text ?? string.Empty).Trim();
        var toId = (WorkflowDebugLinkToTextBox.Text ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(fromId) || string.IsNullOrWhiteSpace(toId))
        {
            WorkflowDebugOutputTextBox.Text = "Both from/to node ids are required.";
            return;
        }

        var from = FindCanvasNodeByKey(fromId);
        var to = FindCanvasNodeByKey(toId);
        if (from is null || to is null)
        {
            WorkflowDebugOutputTextBox.Text = "Node id not found.";
            return;
        }

        var existing = _connections.FirstOrDefault(edge =>
            edge.Source.Tag is CanvasNodeTag sourceTag
            && edge.Target.Tag is CanvasNodeTag targetTag
            && string.Equals(sourceTag.NodeKey, fromId, StringComparison.Ordinal)
            && string.Equals(targetTag.NodeKey, toId, StringComparison.Ordinal));
        if (existing is not null)
        {
            _connectionIndex.Remove(existing.Source, existing.Target, existing);
            WorkspaceCanvas.Children.Remove(existing.Line);
            _connections.Remove(existing);
            if (_selectedConnection == existing)
            {
                _selectedConnection = null;
            }

            UpdateConnectionVisuals();
            UpdateNodePropertyPanel();
            RequestCanvasAutosave();
            WorkflowDebugOutputTextBox.Text = $"Removed link: {fromId} -> {toId}";
            return;
        }

        AddConnection(from, to, select: false);
        WorkflowDebugOutputTextBox.Text = $"Added link: {fromId} -> {toId}";
    }

    private void OnSelectWorkflowDebugNodesClick(object sender, RoutedEventArgs e)
    {
        if (!IsWorkflowDebugShellEnabled)
        {
            return;
        }

        var ids = WorkflowDebugShellSupport.ParseNodeIds(WorkflowDebugSelectNodesTextBox.Text);
        var nodes = ids
            .Select(FindCanvasNodeByKey)
            .Where(static node => node is not null)
            .Cast<Border>()
            .Distinct()
            .ToList();
        _selectedConnection = null;
        _multiSelectedNodes.Clear();
        _selectedNode = null;
        if (nodes.Count == 1)
        {
            SelectNode(nodes[0]);
            WorkflowDebugOutputTextBox.Text = $"Selected node: {ids[0]}";
            return;
        }

        if (nodes.Count > 1)
        {
            foreach (var node in nodes)
            {
                _multiSelectedNodes.Add(node);
            }

            ApplyNodeSelectionVisuals();
            UpdateConnectionVisuals();
            UpdateNodePropertyPanel();
            WorkflowDebugOutputTextBox.Text = $"Selected nodes: {string.Join(", ", ids)}";
            return;
        }

        UpdateNodePropertyPanel();
        WorkflowDebugOutputTextBox.Text = "No matching nodes found.";
    }

    private void OnExportWorkflowDebugGraphClick(object sender, RoutedEventArgs e)
    {
        if (!IsWorkflowDebugShellEnabled)
        {
            return;
        }

        var document = BuildWorkflowGraphDocumentFromCanvas();
        if (document is null)
        {
            WorkflowDebugOutputTextBox.Text = "{}";
            return;
        }

        WorkflowDebugOutputTextBox.Text = PrettyJson(
            WorkflowCanvasDocumentBuilder.SerializeWorkflowDefinition(document).ToJsonString());
    }

    private void OnImportWorkflowDebugGraphClick(object sender, RoutedEventArgs e)
    {
        if (!IsWorkflowDebugShellEnabled)
        {
            return;
        }

        try
        {
            var raw = JsonNode.Parse(WorkflowDebugOutputTextBox.Text) as JsonObject
                ?? throw new InvalidOperationException("Workflow debug import requires a JSON object.");
            var workflowDefinition = WorkflowDebugShellSupport.ResolveWorkflowDefinitionForImport(raw);
            var document = WorkflowCanvasDocumentBuilder.ImportWorkflowDefinition(workflowDefinition);
            ApplyWorkflowGraphDocumentToCanvas(document);
            SetActiveSection(NavSection.Canvas);
            WorkflowDebugOutputTextBox.Text = PrettyJson(workflowDefinition.ToJsonString());
        }
        catch (Exception ex)
        {
            WorkflowDebugOutputTextBox.Text = ex.Message;
        }
    }

    private void OnWorkflowDebugRouteStatsClick(object sender, RoutedEventArgs e)
    {
        if (!IsWorkflowDebugShellEnabled)
        {
            return;
        }

        var allNodes = GetCanvasNodeBorders().ToList();
        var selectedIds = _selectedNode?.Tag is CanvasNodeTag selectedTag
            ? new[] { selectedTag.NodeKey }
            : _multiSelectedNodes
                .Select(node => (node.Tag as CanvasNodeTag)?.NodeKey ?? string.Empty)
                .Where(static id => !string.IsNullOrWhiteSpace(id))
                .ToArray();
        var stats = WorkflowDebugShellSupport.BuildRouteStats(
            allNodes.Count,
            allNodes.Count(node => node.Tag is CanvasNodeTag tag && tag.IsUserNode),
            _connections.Count,
            selectedIds,
            CanvasTransform.ScaleX,
            CanvasTransform.TranslateX,
            CanvasTransform.TranslateY);
        WorkflowDebugOutputTextBox.Text = PrettyJson(stats.ToJsonString());
    }
}
