using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
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
            File.WriteAllText(CanvasStateFilePath, json, Encoding.UTF8);
            if (showStatus)
            {
                SetInlineStatus($"画布已保存：{CanvasStateFilePath}", InlineStatusTone.Success);
            }

            return true;
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
        var snapshot = new CanvasSnapshot
        {
            CanvasWidth = _canvasWidth,
            CanvasHeight = _canvasHeight,
            ViewScale = Math.Clamp(CanvasTransform.ScaleX, CanvasMinScale, CanvasMaxScale),
            ViewTranslateX = CanvasTransform.TranslateX,
            ViewTranslateY = CanvasTransform.TranslateY
        };

        foreach (var node in GetCanvasNodeBorders())
        {
            if (node.Tag is not CanvasNodeTag tag)
            {
                continue;
            }

            snapshot.Nodes.Add(new CanvasNodeDto
            {
                NodeKey = tag.NodeKey,
                Title = tag.TitleBlock?.Text ?? string.Empty,
                Subtitle = tag.SubtitleBlock?.Text ?? string.Empty,
                X = Canvas.GetLeft(node),
                Y = Canvas.GetTop(node),
                IsUserNode = tag.IsUserNode
            });
        }

        var keys = snapshot.Nodes.Select(x => x.NodeKey).ToHashSet(StringComparer.Ordinal);
        foreach (var edge in _connections)
        {
            if (edge.Source.Tag is not CanvasNodeTag sourceTag || edge.Target.Tag is not CanvasNodeTag targetTag)
            {
                continue;
            }

            if (!keys.Contains(sourceTag.NodeKey) || !keys.Contains(targetTag.NodeKey))
            {
                continue;
            }

            snapshot.Edges.Add(new CanvasEdgeDto
            {
                SourceKey = sourceTag.NodeKey,
                TargetKey = targetTag.NodeKey
            });
        }

        return snapshot;
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

        var map = new Dictionary<string, Border>(StringComparer.Ordinal);
        var maxX = Math.Max(DefaultCanvasWidth, snapshot.CanvasWidth);
        var maxY = Math.Max(DefaultCanvasHeight, snapshot.CanvasHeight);
        foreach (var node in snapshot.Nodes)
        {
            if (string.IsNullOrWhiteSpace(node.NodeKey))
            {
                continue;
            }

            var x = Math.Max(0, node.X);
            var y = Math.Max(0, node.Y);
            maxX = Math.Max(maxX, x + 320);
            maxY = Math.Max(maxY, y + 220);
            var border = AddCanvasNode(
                node.NodeKey,
                string.IsNullOrWhiteSpace(node.Title) ? "节点" : node.Title,
                node.Subtitle ?? string.Empty,
                x,
                y,
                isUserNode: node.IsUserNode);
            map[node.NodeKey] = border;
        }

        _canvasWidth = Math.Ceiling(Math.Max(DefaultCanvasWidth, maxX) / CanvasGridSize) * CanvasGridSize;
        _canvasHeight = Math.Ceiling(Math.Max(DefaultCanvasHeight, maxY) / CanvasGridSize) * CanvasGridSize;
        BuildCanvasGrid();

        foreach (var edge in snapshot.Edges)
        {
            if (string.IsNullOrWhiteSpace(edge.SourceKey) || string.IsNullOrWhiteSpace(edge.TargetKey))
            {
                continue;
            }

            if (!map.TryGetValue(edge.SourceKey, out var source) || !map.TryGetValue(edge.TargetKey, out var target))
            {
                continue;
            }

            AddConnection(source, target, select: false);
        }

        var userNodes = snapshot.Nodes.Count(x => x.IsUserNode);
        _customNodeCounter = Math.Max(1, userNodes + 1);
        UpdateAllConnections();
        CanvasTransform.ScaleX = Math.Clamp(snapshot.ViewScale, CanvasMinScale, CanvasMaxScale);
        CanvasTransform.ScaleY = Math.Clamp(snapshot.ViewScale, CanvasMinScale, CanvasMaxScale);
        CanvasTransform.TranslateX = snapshot.ViewTranslateX;
        CanvasTransform.TranslateY = snapshot.ViewTranslateY;
        ClampCanvasTransform();
        UpdateNodePropertyPanel();
        EnsureCanvasExtentForViewportAndNodes();
    }

    private void ClearCanvasWorkspaceState()
    {
        CancelConnectionPreview();
        _connections.Clear();
        _artifactNodes.Clear();
        _selectedConnection = null;
        _selectedNode = null;
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
