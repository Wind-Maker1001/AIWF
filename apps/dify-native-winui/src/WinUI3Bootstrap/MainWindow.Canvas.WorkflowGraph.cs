using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using AIWF.Native.Runtime;
using Microsoft.UI.Xaml;
using Microsoft.UI.Xaml.Controls;

namespace AIWF.Native;

public sealed partial class MainWindow
{
    private static readonly string WorkflowGraphStateFilePath = Path.Combine(
        Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
        "AIWF",
        "canvas-workflow.graph.json");

    private readonly Dictionary<string, Control> _nodeConfigInputControls = new(StringComparer.Ordinal);
    private string _canvasWorkflowId = "native_sql_workflow";
    private string _canvasWorkflowVersion = "1.0.0";
    private static readonly string[] SupportedTypedNodeTypes =
    [
        "load_rows_v3",
        "columnar_eval_v1",
        "join_rows_v2",
        "aggregate_rows_v2",
        "query_lang_v1",
        "sql_chart_v1"
    ];

    private Dictionary<string, JsonObject> _lastNodeOutputs = new(StringComparer.Ordinal);

    private async void OnRunCanvasWorkflowClick(object sender, RoutedEventArgs e)
    {
        var document = BuildWorkflowGraphDocumentFromCanvas();
        if (document is null || document.Nodes.Count == 0)
        {
            SetInlineStatus("当前画布没有可执行的 SQL workflow 节点。", InlineStatusTone.Error);
            return;
        }

        try
        {
            var accelUrl = _sqlConnectionProfile.ResolveAccelUrl(BridgeUrlTextBox.Text);
            var response = await _runnerAdapter.PostJsonAsync(
                accelUrl,
                ApiKeyTextBox.Text.Trim(),
                "/operators/workflow_draft_run_v1",
                new JsonObject
                {
                    ["workflow_definition"] = SerializeWorkflowGraphDocument(document),
                    ["job_id"] = string.IsNullOrWhiteSpace(JobIdTextBox.Text) ? document.WorkflowId : JobIdTextBox.Text.Trim(),
                    ["run_id"] = Guid.NewGuid().ToString("N"),
                    ["job_context"] = new JsonObject(),
                    ["params"] = new JsonObject(),
                });

            var responseJson = response.ToJsonString(new JsonSerializerOptions { WriteIndented = true });
            RawResponseTextBox.Text = responseJson;
            if (!TryBindRunResult(responseJson, "Not retried"))
            {
                SetInlineStatus("画布 workflow 已运行，但结果只保留在原始 JSON 中。", InlineStatusTone.Neutral);
            }
            else
            {
                SetInlineStatus("画布 workflow 运行完成。", InlineStatusTone.Success);
            }

            if (response["final_output"] is JsonObject finalOutput && finalOutput["rows"] is JsonArray)
            {
                ApplySqlPreviewState(SqlStudioResultMapper.FromLoadRowsResponse(finalOutput, string.Empty));
            }

            CaptureNodeOutputs(response);
            ApplyNodeRunStatusToCanvas(response);
            TryRenderChartFromWorkflowResult(response);
            SetActiveSection(NavSection.Results);
        }
        catch (Exception ex)
        {
            SetInlineStatus($"运行画布 workflow 失败: {ex.Message}", InlineStatusTone.Error);
        }
    }

    private void ApplyWorkflowGraphDocumentToCanvas(WorkflowGraphDocument document)
    {
        if (!EnsureCanvasWorkspaceInitialized())
        {
            return;
        }

        _canvasWorkflowId = string.IsNullOrWhiteSpace(document.WorkflowId) ? "native_sql_workflow" : document.WorkflowId;
        _canvasWorkflowVersion = string.IsNullOrWhiteSpace(document.Version) ? "1.0.0" : document.Version;
        CreateNewCanvas("已加载 SQL workflow 画布。");

        var map = new Dictionary<string, Border>(StringComparer.Ordinal);
        foreach (var node in document.Nodes)
        {
            var border = AddCanvasNode(
                node.Id,
                node.Title,
                node.Subtitle,
                node.X,
                node.Y,
                workflowNodeType: node.Type,
                workflowConfig: CloneJsonObject(node.Config),
                isUserNode: true,
                requestAutosave: false);
            map[node.Id] = border;
        }

        foreach (var edge in document.Edges)
        {
            if (map.TryGetValue(edge.From, out var source) && map.TryGetValue(edge.To, out var target))
            {
                AddConnection(source, target, select: false, requestAutosave: false);
            }
        }

        SaveWorkflowGraphDocumentSidecar();
        RequestCanvasAutosave();
    }

    private WorkflowGraphDocument? BuildWorkflowGraphDocumentFromCanvas()
    {
        var borders = GetCanvasNodeBorders()
            .Where(node => node.Tag is CanvasNodeTag tag
                && tag.IsUserNode
                && !string.IsNullOrWhiteSpace(tag.WorkflowNodeType))
            .ToList();
        if (borders.Count == 0)
        {
            return null;
        }

        var allowedIds = borders
            .Select(node => ((CanvasNodeTag)node.Tag).NodeKey)
            .ToHashSet(StringComparer.Ordinal);
        var edges = _connections
            .Where(edge =>
                edge.Source.Tag is CanvasNodeTag sourceTag
                && edge.Target.Tag is CanvasNodeTag targetTag
                && allowedIds.Contains(sourceTag.NodeKey)
                && allowedIds.Contains(targetTag.NodeKey))
            .Select(edge => new WorkflowGraphEdgeDocument(
                ((CanvasNodeTag)edge.Source.Tag).NodeKey,
                ((CanvasNodeTag)edge.Target.Tag).NodeKey))
            .ToArray();
        var incoming = edges
            .GroupBy(edge => edge.To, StringComparer.Ordinal)
            .ToDictionary(group => group.Key, group => (IReadOnlyList<string>)group.Select(item => item.From).ToArray(), StringComparer.Ordinal);

        var nodes = borders.Select(border =>
        {
            var tag = (CanvasNodeTag)border.Tag;
            var config = CloneJsonObject(tag.WorkflowConfig) ?? SqlCanvasNodeDefaults.Create(tag.WorkflowNodeType);
            var predecessors = incoming.TryGetValue(tag.NodeKey, out var values) ? values : Array.Empty<string>();
            ApplyRuntimeWorkflowInputs(tag.WorkflowNodeType, config, predecessors);
            return new WorkflowGraphNodeDocument(
                tag.NodeKey,
                tag.WorkflowNodeType ?? "unknown",
                tag.TitleBlock?.Text ?? tag.NodeKey,
                tag.SubtitleBlock?.Text ?? string.Empty,
                Canvas.GetLeft(border),
                Canvas.GetTop(border),
                config);
        }).ToArray();

        return new WorkflowGraphDocument(
            WorkflowId: _canvasWorkflowId,
            Version: _canvasWorkflowVersion,
            Nodes: nodes,
            Edges: edges,
            Viewport: new WorkflowGraphViewportDocument(
                CanvasTransform.ScaleX,
                CanvasTransform.TranslateX,
                CanvasTransform.TranslateY),
            Selection: new WorkflowGraphSelectionDocument(
                _multiSelectedNodes
                    .Select(node => (node.Tag as CanvasNodeTag)?.NodeKey ?? string.Empty)
                    .Where(static key => !string.IsNullOrWhiteSpace(key))
                    .ToArray()));
    }

    private static void ApplyRuntimeWorkflowInputs(string? workflowNodeType, JsonObject config, IReadOnlyList<string> predecessors)
    {
        if (string.IsNullOrWhiteSpace(workflowNodeType))
        {
            return;
        }

        switch (workflowNodeType)
        {
            case "columnar_eval_v1":
            case "aggregate_rows_v2":
            case "query_lang_v1":
            case "sql_chart_v1":
                if (predecessors.Count > 0)
                {
                    config["rows"] = new JsonArray();
                    config["input_map"] = new JsonObject
                    {
                        ["rows"] = new JsonObject { ["from"] = predecessors[0], ["path"] = "rows" }
                    };
                }
                break;
            case "join_rows_v2":
                if (predecessors.Count > 0)
                {
                    config["left_rows"] = new JsonArray();
                }
                if (predecessors.Count > 1)
                {
                    config["right_rows"] = new JsonArray();
                }
                if (predecessors.Count > 0)
                {
                    config["input_map"] = new JsonObject
                    {
                        ["left_rows"] = new JsonObject { ["from"] = predecessors[0], ["path"] = "rows" },
                        ["right_rows"] = new JsonObject
                        {
                            ["from"] = predecessors.Count > 1 ? predecessors[1] : predecessors[0],
                            ["path"] = "rows"
                        }
                    };
                }
                break;
        }
    }

    private static JsonObject SerializeWorkflowGraphDocument(WorkflowGraphDocument document)
    {
        var nodes = new JsonArray();
        foreach (var node in document.Nodes)
        {
            nodes.Add(new JsonObject
            {
                ["id"] = node.Id,
                ["type"] = node.Type,
                ["x"] = node.X,
                ["y"] = node.Y,
                ["config"] = CloneJsonObject(node.Config),
            });
        }

        var edges = new JsonArray();
        foreach (var edge in document.Edges)
        {
            edges.Add(new JsonObject
            {
                ["from"] = edge.From,
                ["to"] = edge.To,
            });
        }

        return new JsonObject
        {
            ["workflow_id"] = document.WorkflowId,
            ["version"] = document.Version,
            ["nodes"] = nodes,
            ["edges"] = edges,
        };
    }

    private void SaveWorkflowGraphDocumentSidecar()
    {
        var document = BuildWorkflowGraphDocumentFromCanvas();
        var dir = Path.GetDirectoryName(WorkflowGraphStateFilePath) ?? ".";
        Directory.CreateDirectory(dir);
        if (document is null)
        {
            if (File.Exists(WorkflowGraphStateFilePath))
            {
                File.Delete(WorkflowGraphStateFilePath);
            }
            return;
        }

        File.WriteAllText(
            WorkflowGraphStateFilePath,
            JsonSerializer.Serialize(document, CanvasSnapshotJsonOptions),
            Encoding.UTF8);
    }

    private void TryRestoreWorkflowGraphDocumentSidecar()
    {
        if (!File.Exists(WorkflowGraphStateFilePath))
        {
            return;
        }

        try
        {
            var document = JsonSerializer.Deserialize<WorkflowGraphDocument>(
                File.ReadAllText(WorkflowGraphStateFilePath, Encoding.UTF8));
            if (document is null)
            {
                return;
            }

            _canvasWorkflowId = document.WorkflowId;
            _canvasWorkflowVersion = document.Version;
            var byId = GetCanvasNodeBorders()
                .Where(node => node.Tag is CanvasNodeTag)
                .ToDictionary(node => ((CanvasNodeTag)node.Tag).NodeKey, node => node, StringComparer.Ordinal);
            foreach (var node in document.Nodes)
            {
                if (!byId.TryGetValue(node.Id, out var border) || border.Tag is not CanvasNodeTag tag)
                {
                    continue;
                }

                tag.WorkflowNodeType = node.Type;
                tag.WorkflowConfig = CloneJsonObject(node.Config);
                if (tag.TitleBlock is not null)
                {
                    tag.TitleBlock.Text = node.Title;
                }
                if (tag.SubtitleBlock is not null)
                {
                    tag.SubtitleBlock.Text = node.Subtitle;
                }
            }
        }
        catch
        {
        }
    }

    private void RenderSelectedNodeWorkflowEditor(CanvasNodeTag? tag)
    {
        _nodeConfigInputControls.Clear();
        NodeConfigEditorHost.Children.Clear();
        NodeConfigJsonTextBox.Visibility = Visibility.Collapsed;
        NodeConfigJsonTextBox.Text = string.Empty;
        ApplyNodeConfigButton.IsEnabled = false;
        OpenNodeInSqlStudioButton.IsEnabled = false;
        NodeTypeInfoTextBlock.Text = "类型: -";

        if (tag is null || string.IsNullOrWhiteSpace(tag.WorkflowNodeType))
        {
            return;
        }

        NodeTypeInfoTextBlock.Text = $"类型: {tag.WorkflowNodeType}";
        OpenNodeInSqlStudioButton.IsEnabled = string.Equals(tag.WorkflowNodeType, "load_rows_v3", StringComparison.Ordinal);
        ApplyNodeConfigButton.IsEnabled = true;
        var config = CloneJsonObject(tag.WorkflowConfig) ?? SqlCanvasNodeDefaults.Create(tag.WorkflowNodeType);
        NodeConfigJsonTextBox.Text = PrettyJson(config.ToJsonString());

        if (!SupportedTypedNodeTypes.Contains(tag.WorkflowNodeType, StringComparer.Ordinal))
        {
            NodeConfigJsonTextBox.Visibility = Visibility.Visible;
            return;
        }

        switch (tag.WorkflowNodeType)
        {
            case "load_rows_v3":
                AddEditorComboBox("source_type", "Source Type", config["source_type"]?.GetValue<string>() ?? SqlConnectionProfile.Sqlite, SqlConnectionProfile.Sqlite, SqlConnectionProfile.SqlServer);
                AddEditorTextBox("source", "Source", config["source"]?.GetValue<string>() ?? string.Empty);
                AddEditorTextBox("query", "Query", config["query"]?.GetValue<string>() ?? string.Empty, multiline: true, height: 120);
                AddEditorTextBox("limit", "Limit", config["limit"]?.ToJsonString() ?? "200");
                break;
            case "columnar_eval_v1":
                AddEditorTextBox("select_fields", "Select Fields (csv)", string.Join(", ", ReadJsonArrayStrings(config["select_fields"] as JsonArray)));
                AddEditorTextBox("filter_eq", "Filter Eq (json)", PrettyJson((config["filter_eq"] as JsonObject ?? new JsonObject()).ToJsonString()), multiline: true, height: 100);
                AddEditorTextBox("limit", "Limit", config["limit"]?.ToJsonString() ?? "200");
                break;
            case "join_rows_v2":
                AddEditorTextBox("left_on", "Left Keys (csv)", string.Join(", ", ReadJsonArrayStrings(config["left_on"] as JsonArray)));
                AddEditorTextBox("right_on", "Right Keys (csv)", string.Join(", ", ReadJsonArrayStrings(config["right_on"] as JsonArray)));
                AddEditorComboBox("join_type", "Join Type", config["join_type"]?.GetValue<string>() ?? "inner", "inner", "left", "right", "full", "semi", "anti");
                break;
            case "aggregate_rows_v2":
                AddEditorTextBox("group_by", "Group By (csv)", string.Join(", ", ReadJsonArrayStrings(config["group_by"] as JsonArray)));
                AddEditorTextBox("aggregates", "Aggregates (json)", PrettyJson((config["aggregates"] as JsonArray ?? new JsonArray()).ToJsonString()), multiline: true, height: 100);
                break;
            case "query_lang_v1":
                AddEditorTextBox("query", "Query DSL", config["query"]?.GetValue<string>() ?? "limit 100", multiline: true, height: 100);
                break;
            case "sql_chart_v1":
                AddEditorTextBox("chart_type", "Chart Type", config["chart_type"]?.GetValue<string>() ?? "bar");
                AddEditorTextBox("category_field", "Category Field", config["category_field"]?.GetValue<string>() ?? "category");
                AddEditorTextBox("value_field", "Value Field", config["value_field"]?.GetValue<string>() ?? "value");
                AddEditorTextBox("series_field", "Series Field", config["series_field"]?.GetValue<string>() ?? "series");
                AddEditorTextBox("top_n", "Top N", config["top_n"]?.ToJsonString() ?? "20");
                break;
        }
    }

    private void AddEditorTextBox(string key, string label, string value, bool multiline = false, double height = 44)
    {
        NodeConfigEditorHost.Children.Add(new TextBlock { Text = label });
        var box = new TextBox
        {
            Text = value,
            AcceptsReturn = multiline,
            TextWrapping = multiline ? TextWrapping.Wrap : TextWrapping.NoWrap,
            MinHeight = height,
            Height = multiline ? height : double.NaN
        };
        if (ResolveStyleResource("TouchTextBoxStyle") is Style style)
        {
            box.Style = style;
        }
        NodeConfigEditorHost.Children.Add(box);
        _nodeConfigInputControls[key] = box;
    }

    private void AddEditorComboBox(string key, string label, string selected, params string[] items)
    {
        NodeConfigEditorHost.Children.Add(new TextBlock { Text = label });
        var combo = new ComboBox
        {
        };
        if (ResolveStyleResource("TouchComboBoxStyle") is Style style)
        {
            combo.Style = style;
        }
        foreach (var item in items)
        {
            combo.Items.Add(new ComboBoxItem { Content = item });
        }
        SetComboByText(combo, selected);
        NodeConfigEditorHost.Children.Add(combo);
        _nodeConfigInputControls[key] = combo;
    }

    private void OnApplyNodeConfigClick(object sender, RoutedEventArgs e)
    {
        if (_selectedNode?.Tag is not CanvasNodeTag tag || string.IsNullOrWhiteSpace(tag.WorkflowNodeType))
        {
            return;
        }

        tag.WorkflowConfig = BuildConfigFromEditor(tag.WorkflowNodeType, tag.WorkflowConfig);
        NodeConfigJsonTextBox.Text = PrettyJson((tag.WorkflowConfig ?? new JsonObject()).ToJsonString());
        RequestCanvasAutosave();
        SetInlineStatus("节点配置已更新。", InlineStatusTone.Success);
    }

    private JsonObject BuildConfigFromEditor(string workflowNodeType, JsonObject? existing)
    {
        if (!SupportedTypedNodeTypes.Contains(workflowNodeType, StringComparer.Ordinal))
        {
            try
            {
                return JsonNode.Parse(NodeConfigJsonTextBox.Text) as JsonObject ?? new JsonObject();
            }
            catch
            {
                return CloneJsonObject(existing) ?? new JsonObject();
            }
        }

        var config = CloneJsonObject(existing) ?? SqlCanvasNodeDefaults.Create(workflowNodeType);
        switch (workflowNodeType)
        {
            case "load_rows_v3":
                config["source_type"] = ReadEditorComboText("source_type");
                config["source"] = ReadEditorText("source");
                config["query"] = ReadEditorText("query");
                config["limit"] = ReadEditorInt("limit", 200);
                config["max_retries"] = 2;
                config["retry_backoff_ms"] = 150;
                break;
            case "columnar_eval_v1":
                config["select_fields"] = new JsonArray(SplitCsv(ReadEditorText("select_fields")).Select(static item => (JsonNode?)JsonValue.Create(item)).ToArray());
                config["filter_eq"] = ParseEditorJsonObject("filter_eq");
                config["limit"] = ReadEditorInt("limit", 200);
                break;
            case "join_rows_v2":
                config["left_on"] = new JsonArray(SplitCsv(ReadEditorText("left_on")).Select(static item => (JsonNode?)JsonValue.Create(item)).ToArray());
                config["right_on"] = new JsonArray(SplitCsv(ReadEditorText("right_on")).Select(static item => (JsonNode?)JsonValue.Create(item)).ToArray());
                config["join_type"] = ReadEditorComboText("join_type");
                break;
            case "aggregate_rows_v2":
                config["group_by"] = new JsonArray(SplitCsv(ReadEditorText("group_by")).Select(static item => (JsonNode?)JsonValue.Create(item)).ToArray());
                config["aggregates"] = ParseEditorJsonArray("aggregates");
                break;
            case "query_lang_v1":
                config["query"] = ReadEditorText("query");
                break;
            case "sql_chart_v1":
                config["chart_type"] = ReadEditorText("chart_type");
                config["category_field"] = ReadEditorText("category_field");
                config["value_field"] = ReadEditorText("value_field");
                config["series_field"] = ReadEditorText("series_field");
                config["top_n"] = ReadEditorInt("top_n", 20);
                break;
        }

        return config;
    }

    private async void OnOpenNodeInSqlStudioClick(object sender, RoutedEventArgs e)
    {
        if (_selectedNode?.Tag is not CanvasNodeTag tag
            || !string.Equals(tag.WorkflowNodeType, "load_rows_v3", StringComparison.Ordinal))
        {
            return;
        }

        var config = tag.WorkflowConfig ?? new JsonObject();
        _sqlConnectionProfile = ProfileFromLoadNodeConfig(config);
        _sqlBuilderDraft = SqlBuilderDraft.Empty;
        ApplySqlConnectionProfileToControls(_sqlConnectionProfile);
        ApplySqlBuilderDraftToControls(_sqlBuilderDraft);
        SetSqlTextDraft(new SqlTextDraft(config["query"]?.GetValue<string>() ?? "SELECT * FROM data", true));
        SetActiveSection(NavSection.SqlStudio);
        await Task.CompletedTask;
    }

    private static SqlConnectionProfile ProfileFromLoadNodeConfig(JsonObject config)
    {
        var sourceType = config["source_type"]?.GetValue<string>() ?? SqlConnectionProfile.Sqlite;
        var source = config["source"]?.GetValue<string>() ?? string.Empty;
        if (string.Equals(sourceType, SqlConnectionProfile.Sqlite, StringComparison.OrdinalIgnoreCase))
        {
            return SqlConnectionProfile.Default with
            {
                SourceType = SqlConnectionProfile.Sqlite,
                SQLitePath = source
            };
        }

        var normalized = source.Contains("://", StringComparison.Ordinal) ? source : $"sqlserver://{source}";
        if (Uri.TryCreate(normalized, UriKind.Absolute, out var uri))
        {
            var user = string.Empty;
            var password = string.Empty;
            var windowsAuth = false;
            foreach (var part in uri.Query.TrimStart('?').Split('&', StringSplitOptions.RemoveEmptyEntries))
            {
                var kv = part.Split('=', 2);
                if (kv.Length != 2)
                {
                    continue;
                }

                var key = kv[0];
                var value = Uri.UnescapeDataString(kv[1]);
                switch (key)
                {
                    case "user":
                        user = value;
                        break;
                    case "password":
                        password = value;
                        break;
                    case "windows_auth":
                        windowsAuth = value.Equals("true", StringComparison.OrdinalIgnoreCase)
                            || value.Equals("1", StringComparison.OrdinalIgnoreCase)
                            || value.Equals("yes", StringComparison.OrdinalIgnoreCase);
                        break;
                }
            }

            return SqlConnectionProfile.Default with
            {
                SourceType = SqlConnectionProfile.SqlServer,
                SqlServerHost = uri.Host,
                SqlServerPort = uri.IsDefaultPort ? "1433" : uri.Port.ToString(),
                Database = uri.AbsolutePath.Trim('/'),
                User = user,
                Password = password,
                UseWindowsAuth = windowsAuth,
            };
        }

        return SqlConnectionProfile.Default with
        {
            SourceType = SqlConnectionProfile.SqlServer
        };
    }

    private string ReadEditorText(string key)
    {
        return _nodeConfigInputControls.TryGetValue(key, out var control) && control is TextBox box
            ? box.Text.Trim()
            : string.Empty;
    }

    private string ReadEditorComboText(string key)
    {
        return _nodeConfigInputControls.TryGetValue(key, out var control) && control is ComboBox combo
            ? ReadComboText(combo)
            : string.Empty;
    }

    private int ReadEditorInt(string key, int fallback)
    {
        return int.TryParse(ReadEditorText(key), out var parsed) && parsed > 0 ? parsed : fallback;
    }

    private JsonObject ParseEditorJsonObject(string key)
    {
        try
        {
            return JsonNode.Parse(ReadEditorText(key)) as JsonObject ?? new JsonObject();
        }
        catch
        {
            return new JsonObject();
        }
    }

    private JsonArray ParseEditorJsonArray(string key)
    {
        try
        {
            return JsonNode.Parse(ReadEditorText(key)) as JsonArray ?? new JsonArray();
        }
        catch
        {
            return new JsonArray();
        }
    }

    private static IReadOnlyList<string> ReadJsonArrayStrings(JsonArray? array)
    {
        return array?.Select(item => item?.GetValue<string>() ?? string.Empty)
            .Where(static item => !string.IsNullOrWhiteSpace(item))
            .ToArray()
            ?? Array.Empty<string>();
    }

    private Style? ResolveStyleResource(string key)
    {
        return Content is FrameworkElement root
            && root.Resources.TryGetValue(key, out var value)
            && value is Style style
            ? style
            : null;
    }

    private void CaptureNodeOutputs(JsonObject response)
    {
        _lastNodeOutputs.Clear();
        if (response["node_outputs"] is JsonObject outputs)
        {
            foreach (var kv in outputs)
            {
                if (kv.Value is JsonObject nodeOutput)
                {
                    _lastNodeOutputs[kv.Key] = nodeOutput;
                }
            }
        }
    }

    private void ApplyNodeRunStatusToCanvas(JsonObject response)
    {
        var borders = GetCanvasNodeBorders().ToList();
        foreach (var border in borders)
        {
            if (border.Tag is not CanvasNodeTag tag)
            {
                continue;
            }

            if (_lastNodeOutputs.TryGetValue(tag.NodeKey, out var output))
            {
                var ok = output["ok"]?.GetValue<bool?>() == true;
                border.BorderBrush = new Microsoft.UI.Xaml.Media.SolidColorBrush(
                    ok ? Windows.UI.Color.FromArgb(255, 34, 197, 94)     // green
                       : Windows.UI.Color.FromArgb(255, 239, 68, 68));   // red
                border.BorderThickness = new Thickness(2);

                if (tag.SubtitleBlock is not null)
                {
                    var status = output["status"]?.GetValue<string>() ?? (ok ? "done" : "error");
                    var rowCount = output["rows"] is JsonArray rows ? rows.Count : 0;
                    tag.SubtitleBlock.Text = ok ? $"{status} ({rowCount} rows)" : $"ERR: {status}";
                }
            }
        }
    }

    public void ShowNodeOutput(string nodeKey)
    {
        if (!_lastNodeOutputs.TryGetValue(nodeKey, out var output))
        {
            SetInlineStatus($"节点 {nodeKey} 没有输出数据。", InlineStatusTone.Neutral);
            return;
        }

        if (output["rows"] is JsonArray)
        {
            ApplySqlPreviewState(SqlStudioResultMapper.FromLoadRowsResponse(output, string.Empty));
        }
        else
        {
            RawResponseTextBox.Text = output.ToJsonString(new JsonSerializerOptions { WriteIndented = true });
        }

        if (output["chart_type"] is not null)
        {
            SqlChartRenderer.Render(SqlChartCanvas, SqlChartData.FromJson(output), 700, 480);
        }

        SetActiveSection(NavSection.Results);
    }
}
