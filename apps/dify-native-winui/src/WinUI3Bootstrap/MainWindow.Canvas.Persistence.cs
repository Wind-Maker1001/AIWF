using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using AIWF.Native.CanvasRuntime;
using Microsoft.UI.Xaml;
using Microsoft.UI.Xaml.Controls;

namespace AIWF.Native;

public sealed partial class MainWindow
{
    private void OnSaveCanvasClick(object sender, RoutedEventArgs e)
    {
        SaveCanvasSnapshot(showStatus: true);
    }

    private void OnLoadCanvasClick(object sender, RoutedEventArgs e)
    {
        TryLoadCanvasSnapshot(showStatus: true, missingIsError: true);
    }

    private void OnNewCanvasClick(object sender, RoutedEventArgs e)
    {
        CreateNewCanvas();
    }

    private void OnClearCanvasClick(object sender, RoutedEventArgs e)
    {
        CreateNewCanvas();
        SetInlineStatus("画布已清空。", InlineStatusTone.Success);
    }

    private void CreateNewCanvas()
    {
        _suppressCanvasAutosave = true;
        try
        {
            ClearCanvasWorkspaceState();
            _canvasWidth = DefaultCanvasWidth;
            _canvasHeight = DefaultCanvasHeight;
            BuildCanvasGrid();
            SeedCanvasNodes();
            ResetCanvasView();
            ClampCanvasTransform();
            EnsureCanvasExtentForViewportAndNodes();
            UpdateNodePropertyPanel();
            SetInlineStatus("已新建空白画布。", InlineStatusTone.Success);
        }
        finally
        {
            _suppressCanvasAutosave = false;
        }

        SaveCanvasSnapshot(showStatus: false);
    }

    private void RequestCanvasAutosave()
    {
        if (_suppressCanvasAutosave)
        {
            return;
        }

        if (_canvasAutosaveTimer is null)
        {
            SaveCanvasSnapshot(showStatus: false);
            return;
        }

        _canvasAutosaveTimer.Stop();
        _canvasAutosaveTimer.Start();
    }

    private bool SaveCanvasSnapshot(bool showStatus)
    {
        if (_suppressCanvasAutosave)
        {
            return false;
        }

        try
        {
            var snapshot = BuildCanvasSnapshot();
            var dir = Path.GetDirectoryName(CanvasStateFilePath) ?? ".";
            Directory.CreateDirectory(dir);
            var json = JsonSerializer.Serialize(snapshot, new JsonSerializerOptions { WriteIndented = true });
            var shouldWrite = CanvasSnapshotWriteDecider.ShouldWrite(
                _lastSavedCanvasSnapshotJson,
                json,
                File.Exists(CanvasStateFilePath));
            if (shouldWrite)
            {
                File.WriteAllText(CanvasStateFilePath, json, Encoding.UTF8);
                _lastSavedCanvasSnapshotJson = json;
                if (showStatus)
                {
                    SetInlineStatus($"画布已保存：{CanvasStateFilePath}", InlineStatusTone.Success);
                }
            }
            else if (showStatus)
            {
                SetInlineStatus("画布无变化，无需保存。", InlineStatusTone.Neutral);
            }

            return shouldWrite;
        }
        catch (Exception ex)
        {
            if (showStatus)
            {
                SetInlineStatus($"保存画布失败：{ex.Message}", InlineStatusTone.Error);
            }

            return false;
        }
    }

    private CanvasSnapshot BuildCanvasSnapshot()
    {
        var nodes = new List<CanvasNodeState>();
        foreach (var node in GetCanvasNodeBorders())
        {
            if (node.Tag is not CanvasNodeTag tag)
            {
                continue;
            }

            nodes.Add(new CanvasNodeState(
                tag.NodeKey,
                tag.TitleBlock?.Text ?? string.Empty,
                tag.SubtitleBlock?.Text ?? string.Empty,
                Canvas.GetLeft(node),
                Canvas.GetTop(node),
                tag.IsUserNode,
                tag.IsArtifactNode,
                tag.ArtifactPath,
                tag.ArtifactKind));
        }

        var edges = _connections
            .Select(edge =>
            {
                if (edge.Source.Tag is not CanvasNodeTag sourceTag || edge.Target.Tag is not CanvasNodeTag targetTag)
                {
                    return null;
                }

                return new CanvasEdgeState(sourceTag.NodeKey, targetTag.NodeKey);
            })
            .Where(static edge => edge is not null)
            .Cast<CanvasEdgeState>()
            .ToList();

        return CanvasSnapshotMapper.CreateSnapshot(
            _canvasWidth,
            _canvasHeight,
            CanvasTransform.ScaleX,
            CanvasTransform.TranslateX,
            CanvasTransform.TranslateY,
            nodes,
            edges,
            CanvasMinScale,
            CanvasMaxScale);
    }

    private bool TryLoadCanvasSnapshot(bool showStatus, bool missingIsError)
    {
        try
        {
            if (!File.Exists(CanvasStateFilePath))
            {
                if (showStatus || missingIsError)
                {
                    SetInlineStatus("未找到已保存的画布。", missingIsError ? InlineStatusTone.Error : InlineStatusTone.Neutral);
                }

                return false;
            }

            var json = File.ReadAllText(CanvasStateFilePath, Encoding.UTF8);
            var snapshot = JsonSerializer.Deserialize<CanvasSnapshot>(json);
            if (snapshot is null)
            {
                if (showStatus)
                {
                    SetInlineStatus("加载画布失败：文件内容为空。", InlineStatusTone.Error);
                }

                return false;
            }

            _suppressCanvasAutosave = true;
            try
            {
                ApplyCanvasSnapshot(snapshot);
                _lastSavedCanvasSnapshotJson = json;
            }
            finally
            {
                _suppressCanvasAutosave = false;
            }

            if (showStatus)
            {
                SetInlineStatus($"画布已加载：{CanvasStateFilePath}", InlineStatusTone.Success);
            }

            return true;
        }
        catch (Exception ex)
        {
            if (showStatus)
            {
                SetInlineStatus($"加载画布失败：{ex.Message}", InlineStatusTone.Error);
            }

            return false;
        }
    }

    private void ApplyCanvasSnapshot(CanvasSnapshot snapshot)
    {
        ClearCanvasWorkspaceState();
        var restorePlan = CanvasSnapshotMapper.CreateRestorePlan(
            snapshot,
            DefaultCanvasWidth,
            DefaultCanvasHeight,
            CanvasGridSize,
            CanvasMinScale,
            CanvasMaxScale);

        var map = new Dictionary<string, Border>(StringComparer.Ordinal);
        foreach (var node in restorePlan.Nodes)
        {
            var border = AddCanvasNode(
                node.NodeKey,
                string.IsNullOrWhiteSpace(node.Title) ? "节点" : node.Title,
                node.Subtitle ?? string.Empty,
                node.X,
                node.Y,
                node.ArtifactPath,
                node.ArtifactKind,
                isUserNode: node.IsUserNode,
                isArtifactNode: node.IsArtifactNode);
            map[node.NodeKey] = border;
            if (node.IsArtifactNode)
            {
                _artifactNodes.Add(border);
            }
        }

        _canvasWidth = restorePlan.CanvasWidth;
        _canvasHeight = restorePlan.CanvasHeight;
        BuildCanvasGrid();

        foreach (var edge in restorePlan.Edges)
        {
            if (!map.TryGetValue(edge.SourceKey, out var source) || !map.TryGetValue(edge.TargetKey, out var target))
            {
                continue;
            }

            AddConnection(source, target, select: false, updateGeometry: false, requestAutosave: false);
        }

        EnsureCoreCanvasWorkflowScaffold();
        _customNodeCounter = restorePlan.NextCustomNodeCounter;
        UpdateAllConnections();
        CanvasTransform.ScaleX = restorePlan.ViewScale;
        CanvasTransform.ScaleY = restorePlan.ViewScale;
        CanvasTransform.TranslateX = restorePlan.ViewTranslateX;
        CanvasTransform.TranslateY = restorePlan.ViewTranslateY;
        ClampCanvasTransform();
        UpdateNodePropertyPanel();
        EnsureCanvasExtentForViewportAndNodes();
    }

    private void ClearCanvasWorkspaceState()
    {
        CancelConnectionPreview();
        _connections.Clear();
        _connectionIndex.Clear();
        _artifactNodes.Clear();
        ResetCanvasSelectionState();
        _inputNode = null;
        _cleanNode = null;
        _outputNode = null;
        _customNodeCounter = 1;
        WorkspaceCanvas.Children.Clear();
        _canvasAutosaveTimer?.Stop();
    }

    private IEnumerable<Border> GetCanvasNodeBorders()
    {
        foreach (var child in WorkspaceCanvas.Children)
        {
            if (child is Border border && border.Tag is CanvasNodeTag)
            {
                yield return border;
            }
        }
    }
}
