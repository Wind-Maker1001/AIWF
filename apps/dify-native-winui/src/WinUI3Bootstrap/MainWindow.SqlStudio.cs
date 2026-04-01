using System.Text.Json;
using System.Text.Json.Nodes;
using AIWF.Native.Runtime;
using Microsoft.UI.Xaml;
using Microsoft.UI.Xaml.Controls;

namespace AIWF.Native;

public sealed partial class MainWindow
{
    private SqlConnectionProfile _sqlConnectionProfile = SqlConnectionProfile.Default;
    private SqlBuilderDraft _sqlBuilderDraft = SqlBuilderDraft.Empty;
    private SqlTextDraft _sqlTextDraft = SqlTextDraft.Empty;
    private SchemaBrowserState _schemaBrowserState = SchemaBrowserState.Empty;
    private SqlPreviewState _sqlPreviewState = SqlPreviewState.Empty;
    private bool _suppressSqlTextEditorChange;

    private int _sortColumnIndex = -1;
    private bool _sortDescending;
    private readonly SqlQueryHistoryService _queryHistory = new();

    private void InitializeSqlStudioState()
    {
        ApplySqlConnectionProfileToControls(_sqlConnectionProfile);
        ApplySqlBuilderDraftToControls(_sqlBuilderDraft);
        SetSqlTextDraft(_sqlTextDraft);
        ApplySqlPreviewState(SqlPreviewState.Empty);
        UpdateSqlSourcePanels();
        UpdateSqlDraftModeText();
        _queryHistory.Load();
        RenderQueryHistoryList();
    }

    private async void OnSqlValidateConnectionClick(object sender, RoutedEventArgs e)
    {
        _sqlConnectionProfile = CollectSqlConnectionProfileFromControls();
        try
        {
            var accelUrl = _sqlConnectionProfile.ResolveAccelUrl(BridgeUrlTextBox.Text);
            await _runnerAdapter.PostJsonAsync(
                accelUrl,
                ApiKeyTextBox.Text.Trim(),
                "/operators/data_source_browser_v1",
                new JsonObject
                {
                    ["source_type"] = _sqlConnectionProfile.NormalizedSourceType,
                    ["source"] = _sqlConnectionProfile.BuildRuntimeSource(),
                    ["op"] = "validate_connection",
                });

            var schemasPayload = await _runnerAdapter.PostJsonAsync(
                accelUrl,
                ApiKeyTextBox.Text.Trim(),
                "/operators/data_source_browser_v1",
                new JsonObject
                {
                    ["source_type"] = _sqlConnectionProfile.NormalizedSourceType,
                    ["source"] = _sqlConnectionProfile.BuildRuntimeSource(),
                    ["op"] = "list_schemas",
                });
            _schemaBrowserState = SqlStudioResultMapper.MergeBrowseResponse(SchemaBrowserState.Empty, schemasPayload, "list_schemas");
            RenderSchemaBrowserState();
            SqlPreviewStatusTextBlock.Text = "Connection verified. Schema list loaded.";
            SetInlineStatus("SQL Studio 连接验证通过。", InlineStatusTone.Success);
        }
        catch (Exception ex)
        {
            SqlPreviewStatusTextBlock.Text = $"Connection failed: {ex.Message}";
            SetInlineStatus($"SQL Studio 连接失败: {ex.Message}", InlineStatusTone.Error);
        }
    }

    private async void OnSqlSchemaSelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (SqlSchemaListView.SelectedItem is not SqlSchemaItem item)
        {
            return;
        }

        _schemaBrowserState = _schemaBrowserState with
        {
            SelectedSchema = item.Name,
            Tables = Array.Empty<SqlSchemaItem>(),
            Columns = Array.Empty<SqlColumnMetadata>(),
        };
        RenderSchemaBrowserState();

        try
        {
            var payload = await _runnerAdapter.PostJsonAsync(
                _sqlConnectionProfile.ResolveAccelUrl(BridgeUrlTextBox.Text),
                ApiKeyTextBox.Text.Trim(),
                "/operators/data_source_browser_v1",
                new JsonObject
                {
                    ["source_type"] = _sqlConnectionProfile.NormalizedSourceType,
                    ["source"] = _sqlConnectionProfile.BuildRuntimeSource(),
                    ["op"] = "list_tables",
                    ["schema"] = item.Name,
                });
            _schemaBrowserState = SqlStudioResultMapper.MergeBrowseResponse(_schemaBrowserState, payload, "list_tables");
            RenderSchemaBrowserState();
        }
        catch (Exception ex)
        {
            SqlPreviewStatusTextBlock.Text = $"Load tables failed: {ex.Message}";
        }
    }

    private async void OnSqlTableSelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (SqlTableListView.SelectedItem is not SqlSchemaItem item)
        {
            return;
        }

        var schema = string.IsNullOrWhiteSpace(item.Schema)
            ? _schemaBrowserState.SelectedSchema
            : item.Schema ?? string.Empty;
        _schemaBrowserState = _schemaBrowserState with
        {
            SelectedSchema = schema,
            SelectedTable = item.Name,
            Columns = Array.Empty<SqlColumnMetadata>(),
        };
        RenderSchemaBrowserState();

        try
        {
            var payload = await _runnerAdapter.PostJsonAsync(
                _sqlConnectionProfile.ResolveAccelUrl(BridgeUrlTextBox.Text),
                ApiKeyTextBox.Text.Trim(),
                "/operators/data_source_browser_v1",
                new JsonObject
                {
                    ["source_type"] = _sqlConnectionProfile.NormalizedSourceType,
                    ["source"] = _sqlConnectionProfile.BuildRuntimeSource(),
                    ["op"] = "describe_table",
                    ["schema"] = schema,
                    ["table"] = item.Name,
                });
            _schemaBrowserState = SqlStudioResultMapper.MergeBrowseResponse(_schemaBrowserState, payload, "describe_table");
            RenderSchemaBrowserState();
        }
        catch (Exception ex)
        {
            SqlPreviewStatusTextBlock.Text = $"Describe table failed: {ex.Message}";
        }
    }

    private void OnSqlUseSelectedTableClick(object sender, RoutedEventArgs e)
    {
        if (SqlTableListView.SelectedItem is not SqlSchemaItem item)
        {
            return;
        }

        SqlSelectedSchemaTextBox.Text = string.IsNullOrWhiteSpace(item.Schema)
            ? _schemaBrowserState.SelectedSchema
            : item.Schema ?? string.Empty;
        SqlSelectedTableTextBox.Text = item.Name;
        OnSqlGenerateSqlClick(sender, e);
    }

    private void OnSqlSourceTypeSelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        UpdateSqlSourcePanels();
    }

    private void OnSqlGenerateSqlClick(object sender, RoutedEventArgs e)
    {
        _sqlConnectionProfile = CollectSqlConnectionProfileFromControls();
        _sqlBuilderDraft = CollectSqlBuilderDraftFromControls();
        SetSqlTextDraft(SqlStudioDraftController.ResetToBuilder(_sqlBuilderDraft, _sqlConnectionProfile));
        ApplySqlPreviewState(_sqlPreviewState with { GeneratedSql = _sqlTextDraft.Text });
    }

    private void OnSqlResetBuilderClick(object sender, RoutedEventArgs e)
    {
        _sqlConnectionProfile = CollectSqlConnectionProfileFromControls();
        _sqlBuilderDraft = CollectSqlBuilderDraftFromControls();
        SetSqlTextDraft(SqlStudioDraftController.ResetToBuilder(_sqlBuilderDraft, _sqlConnectionProfile));
        UpdateSqlDraftModeText();
    }

    private void OnSqlTextEditorTextChanged(object sender, TextChangedEventArgs e)
    {
        if (_suppressSqlTextEditorChange)
        {
            return;
        }

        _sqlTextDraft = SqlStudioDraftController.MarkTextOwned(SqlTextEditorTextBox.Text);
        UpdateSqlDraftModeText();
    }

    private async void OnSqlPreviewClick(object sender, RoutedEventArgs e)
    {
        _sqlConnectionProfile = CollectSqlConnectionProfileFromControls();
        _sqlBuilderDraft = CollectSqlBuilderDraftFromControls();
        if (!_sqlTextDraft.IsTextOwned)
        {
            SetSqlTextDraft(SqlStudioDraftController.SyncGeneratedSql(_sqlBuilderDraft, _sqlConnectionProfile, _sqlTextDraft));
        }

        try
        {
            var payload = await _runnerAdapter.PostJsonAsync(
                _sqlConnectionProfile.ResolveAccelUrl(BridgeUrlTextBox.Text),
                ApiKeyTextBox.Text.Trim(),
                "/operators/load_rows_v3",
                new JsonObject
                {
                    ["source_type"] = _sqlConnectionProfile.NormalizedSourceType,
                    ["source"] = _sqlConnectionProfile.BuildRuntimeSource(),
                    ["query"] = _sqlTextDraft.Text,
                    ["limit"] = Math.Max(1, _sqlBuilderDraft.Limit),
                    ["max_retries"] = 2,
                    ["retry_backoff_ms"] = 150,
                });

            ApplySqlPreviewState(SqlStudioResultMapper.FromLoadRowsResponse(payload, _sqlTextDraft.Text));
            RawResponseTextBox.Text = _sqlPreviewState.RawJson;
            RenderChartFromPreviewData();
            _queryHistory.AddHistoryEntry(_sqlTextDraft.Text, _sqlConnectionProfile.NormalizedSourceType,
                _sqlConnectionProfile.Database, _sqlPreviewState.GridRows.Count, true);
            RenderQueryHistoryList();
        }
        catch (Exception ex)
        {
            ApplySqlPreviewState(new SqlPreviewState(
                Ok: false,
                StatusText: $"Preview failed: {ex.Message}",
                GeneratedSql: _sqlTextDraft.Text,
                RawJson: string.Empty,
                Diagnostics: ex.Message,
                RowDisplayItems: Array.Empty<string>(),
                ColumnHeaders: Array.Empty<string>(),
                GridRows: Array.Empty<IReadOnlyList<string>>()));
            _queryHistory.AddHistoryEntry(_sqlTextDraft.Text, _sqlConnectionProfile.NormalizedSourceType,
                _sqlConnectionProfile.Database, 0, false);
            RenderQueryHistoryList();
        }
    }

    private void OnSqlSendToCanvasClick(object sender, RoutedEventArgs e)
    {
        _sqlConnectionProfile = CollectSqlConnectionProfileFromControls();
        _sqlBuilderDraft = CollectSqlBuilderDraftFromControls();
        if (!_sqlTextDraft.IsTextOwned)
        {
            SetSqlTextDraft(SqlStudioDraftController.SyncGeneratedSql(_sqlBuilderDraft, _sqlConnectionProfile, _sqlTextDraft));
        }

        var workflowId = string.IsNullOrWhiteSpace(SqlSelectedTableTextBox.Text)
            ? "native_sql_workflow"
            : $"sql_{SqlSelectedTableTextBox.Text.Trim().Replace(' ', '_')}";
        var document = SqlStudioWorkflowMapper.Build(_sqlConnectionProfile, _sqlBuilderDraft, _sqlTextDraft, workflowId);
        ApplyWorkflowGraphDocumentToCanvas(document);
        SetInlineStatus("SQL 草稿已发送到画布。", InlineStatusTone.Success);
        SetActiveSection(NavSection.Canvas);
    }

    private SqlConnectionProfile CollectSqlConnectionProfileFromControls()
    {
        return new SqlConnectionProfile(
            AccelUrl: SqlAccelUrlTextBox.Text.Trim(),
            SourceType: ReadSelectedSqlSourceType(),
            SQLitePath: SqlitePathTextBox.Text.Trim(),
            SqlServerHost: SqlServerHostTextBox.Text.Trim(),
            SqlServerPort: SqlServerPortTextBox.Text.Trim(),
            Database: SqlServerDatabaseTextBox.Text.Trim(),
            User: SqlServerUserTextBox.Text.Trim(),
            Password: SqlServerPasswordBox.Password?.Trim() ?? string.Empty,
            UseWindowsAuth: SqlWindowsAuthCheckBox.IsChecked == true);
    }

    private void ApplySqlConnectionProfileToControls(SqlConnectionProfile profile)
    {
        SqlAccelUrlTextBox.Text = profile.AccelUrl;
        SqlSourceTypeComboBox.SelectedIndex = string.Equals(profile.SourceType, SqlConnectionProfile.SqlServer, StringComparison.OrdinalIgnoreCase) ? 1 : 0;
        SqlitePathTextBox.Text = profile.SQLitePath;
        SqlServerHostTextBox.Text = profile.SqlServerHost;
        SqlServerPortTextBox.Text = profile.SqlServerPort;
        SqlServerDatabaseTextBox.Text = profile.Database;
        SqlServerUserTextBox.Text = profile.User;
        SqlServerPasswordBox.Password = profile.Password;
        SqlWindowsAuthCheckBox.IsChecked = profile.UseWindowsAuth;
        UpdateSqlSourcePanels();
    }

    private SqlBuilderDraft CollectSqlBuilderDraftFromControls()
    {
        var joins = new List<SqlJoinClause>
        {
            new(
                SqlJoin1EnabledCheckBox.IsChecked == true,
                SqlJoin1SchemaTextBox.Text.Trim(),
                SqlJoin1TableTextBox.Text.Trim(),
                ReadComboText(SqlJoin1TypeComboBox),
                SqlJoin1LeftFieldTextBox.Text.Trim(),
                SqlJoin1RightFieldTextBox.Text.Trim()),
            new(
                SqlJoin2EnabledCheckBox.IsChecked == true,
                SqlJoin2SchemaTextBox.Text.Trim(),
                SqlJoin2TableTextBox.Text.Trim(),
                ReadComboText(SqlJoin2TypeComboBox),
                SqlJoin2LeftFieldTextBox.Text.Trim(),
                SqlJoin2RightFieldTextBox.Text.Trim())
        };

        var filters = new List<SqlFilterClause>();
        if (!string.IsNullOrWhiteSpace(SqlFilterFieldTextBox.Text))
        {
            filters.Add(new SqlFilterClause(
                SqlFilterFieldTextBox.Text.Trim(),
                ReadComboText(SqlFilterOperatorComboBox),
                SqlFilterValueTextBox.Text.Trim()));
        }
        if (!string.IsNullOrWhiteSpace(SqlFilter2FieldTextBox.Text))
        {
            filters.Add(new SqlFilterClause(
                SqlFilter2FieldTextBox.Text.Trim(),
                ReadComboText(SqlFilter2OperatorComboBox),
                SqlFilter2ValueTextBox.Text.Trim()));
        }
        if (!string.IsNullOrWhiteSpace(SqlFilter3FieldTextBox.Text))
        {
            filters.Add(new SqlFilterClause(
                SqlFilter3FieldTextBox.Text.Trim(),
                ReadComboText(SqlFilter3OperatorComboBox),
                SqlFilter3ValueTextBox.Text.Trim()));
        }

        var aggregates = new List<SqlAggregateClause>();
        if (!string.IsNullOrWhiteSpace(SqlAggregateFieldTextBox.Text)
            || !string.IsNullOrWhiteSpace(SqlAggregateAliasTextBox.Text)
            || !string.IsNullOrWhiteSpace(SqlGroupByTextBox.Text))
        {
            aggregates.Add(new SqlAggregateClause(
                ReadComboText(SqlAggregateFunctionComboBox),
                SqlAggregateFieldTextBox.Text.Trim(),
                SqlAggregateAliasTextBox.Text.Trim()));
        }
        if (!string.IsNullOrWhiteSpace(SqlAggregate2FieldTextBox.Text)
            || !string.IsNullOrWhiteSpace(SqlAggregate2AliasTextBox.Text))
        {
            aggregates.Add(new SqlAggregateClause(
                ReadComboText(SqlAggregate2FunctionComboBox),
                SqlAggregate2FieldTextBox.Text.Trim(),
                SqlAggregate2AliasTextBox.Text.Trim()));
        }

        return new SqlBuilderDraft(
            Schema: SqlSelectedSchemaTextBox.Text.Trim(),
            Table: SqlSelectedTableTextBox.Text.Trim(),
            SelectFields: SplitCsv(SqlSelectFieldsTextBox.Text),
            Filters: filters,
            Joins: joins,
            GroupByFields: SplitCsv(SqlGroupByTextBox.Text),
            Aggregates: aggregates,
            OrderByField: SqlOrderByFieldTextBox.Text.Trim(),
            OrderByDescending: SqlOrderByDescCheckBox.IsChecked == true,
            Limit: int.TryParse(SqlLimitTextBox.Text.Trim(), out var parsedLimit) && parsedLimit > 0 ? parsedLimit : 100,
            Chart: new SqlChartDraft(
                Enabled: SqlChartEnabledCheckBox.IsChecked == true,
                ChartType: ReadComboText(SqlChartTypeComboBox),
                CategoryField: SqlChartCategoryFieldTextBox.Text.Trim(),
                ValueField: SqlChartValueFieldTextBox.Text.Trim(),
                SeriesField: SqlChartSeriesFieldTextBox.Text.Trim(),
                TopN: int.TryParse(SqlChartTopNTextBox.Text.Trim(), out var chartTopN) && chartTopN > 0 ? chartTopN : 20),
            Having: new SqlHavingClause(SqlHavingTextBox.Text.Trim()));
    }

    private void ApplySqlBuilderDraftToControls(SqlBuilderDraft draft)
    {
        SqlSelectedSchemaTextBox.Text = draft.Schema;
        SqlSelectedTableTextBox.Text = draft.Table;
        SqlSelectFieldsTextBox.Text = string.Join(", ", draft.SelectFields);
        var filter = draft.Filters.FirstOrDefault();
        SqlFilterFieldTextBox.Text = filter?.Field ?? string.Empty;
        SetComboByText(SqlFilterOperatorComboBox, filter?.Operator ?? "=");
        SqlFilterValueTextBox.Text = filter?.Value ?? string.Empty;
        SqlGroupByTextBox.Text = string.Join(", ", draft.GroupByFields);
        var aggregate = draft.Aggregates.FirstOrDefault();
        SetComboByText(SqlAggregateFunctionComboBox, aggregate?.Function ?? "count");
        SqlAggregateFieldTextBox.Text = aggregate?.Field ?? string.Empty;
        SqlAggregateAliasTextBox.Text = aggregate?.Alias ?? string.Empty;
        var join1 = draft.Joins.ElementAtOrDefault(0);
        SqlJoin1EnabledCheckBox.IsChecked = join1?.Enabled == true;
        SqlJoin1SchemaTextBox.Text = join1?.Schema ?? "dbo";
        SqlJoin1TableTextBox.Text = join1?.Table ?? string.Empty;
        SetComboByText(SqlJoin1TypeComboBox, join1?.JoinType ?? "inner");
        SqlJoin1LeftFieldTextBox.Text = join1?.LeftField ?? string.Empty;
        SqlJoin1RightFieldTextBox.Text = join1?.RightField ?? string.Empty;
        var join2 = draft.Joins.ElementAtOrDefault(1);
        SqlJoin2EnabledCheckBox.IsChecked = join2?.Enabled == true;
        SqlJoin2SchemaTextBox.Text = join2?.Schema ?? "dbo";
        SqlJoin2TableTextBox.Text = join2?.Table ?? string.Empty;
        SetComboByText(SqlJoin2TypeComboBox, join2?.JoinType ?? "inner");
        SqlJoin2LeftFieldTextBox.Text = join2?.LeftField ?? string.Empty;
        SqlJoin2RightFieldTextBox.Text = join2?.RightField ?? string.Empty;
        SqlOrderByFieldTextBox.Text = draft.OrderByField;
        SqlOrderByDescCheckBox.IsChecked = draft.OrderByDescending;
        SqlLimitTextBox.Text = draft.Limit.ToString();
        SqlChartEnabledCheckBox.IsChecked = draft.Chart.Enabled;
        SetComboByText(SqlChartTypeComboBox, draft.Chart.ChartType);
        SqlChartCategoryFieldTextBox.Text = draft.Chart.CategoryField;
        SqlChartValueFieldTextBox.Text = draft.Chart.ValueField;
        SqlChartSeriesFieldTextBox.Text = draft.Chart.SeriesField;
        SqlChartTopNTextBox.Text = draft.Chart.TopN.ToString();
    }

    private void ApplySqlPreviewState(SqlPreviewState state)
    {
        _sqlPreviewState = state;
        _sortColumnIndex = -1;
        _sortDescending = false;
        SqlPreviewStatusTextBlock.Text = state.StatusText;
        SqlGeneratedSqlTextBox.Text = state.GeneratedSql;
        SqlRawJsonTextBox.Text = state.RawJson;
        SqlDiagnosticsTextBox.Text = state.Diagnostics;
        RenderDataGrid(state.ColumnHeaders, state.GridRows);
    }

    private void RenderDataGrid(IReadOnlyList<string> columns, IReadOnlyList<IReadOnlyList<string>> rows)
    {
        SqlDataGridHost.Children.Clear();
        if (columns.Count == 0)
        {
            SqlDataGridHost.Children.Add(new TextBlock
            {
                Text = "No data.",
                Foreground = new Microsoft.UI.Xaml.Media.SolidColorBrush(Microsoft.UI.Colors.Gray)
            });
            return;
        }

        var headerGrid = new Grid { Background = new Microsoft.UI.Xaml.Media.SolidColorBrush(Windows.UI.Color.FromArgb(255, 243, 244, 246)) };
        for (var i = 0; i < columns.Count; i++)
        {
            headerGrid.ColumnDefinitions.Add(new ColumnDefinition { Width = new GridLength(1, GridUnitType.Auto), MinWidth = 80 });
        }

        for (var i = 0; i < columns.Count; i++)
        {
            var colIndex = i;
            var sortIndicator = _sortColumnIndex == i ? (_sortDescending ? " ▼" : " ▲") : "";
            var headerButton = new Button
            {
                Content = columns[i] + sortIndicator,
                HorizontalAlignment = HorizontalAlignment.Stretch,
                HorizontalContentAlignment = HorizontalAlignment.Left,
                FontWeight = Microsoft.UI.Text.FontWeights.SemiBold,
                FontSize = 12,
                Padding = new Thickness(8, 6, 8, 6),
                Background = new Microsoft.UI.Xaml.Media.SolidColorBrush(Windows.UI.Color.FromArgb(0, 0, 0, 0)),
                BorderThickness = new Thickness(0),
            };
            headerButton.Click += (_, _) => OnDataGridHeaderClick(colIndex);
            Grid.SetColumn(headerButton, i);
            headerGrid.Children.Add(headerButton);
        }

        SqlDataGridHost.Children.Add(headerGrid);
        SqlDataGridHost.Children.Add(new Border
        {
            Height = 1,
            Background = new Microsoft.UI.Xaml.Media.SolidColorBrush(Windows.UI.Color.FromArgb(255, 209, 213, 219))
        });

        for (var rowIndex = 0; rowIndex < rows.Count; rowIndex++)
        {
            var row = rows[rowIndex];
            var rowGrid = new Grid();
            if (rowIndex % 2 == 1)
            {
                rowGrid.Background = new Microsoft.UI.Xaml.Media.SolidColorBrush(Windows.UI.Color.FromArgb(255, 249, 250, 251));
            }

            for (var i = 0; i < columns.Count; i++)
            {
                rowGrid.ColumnDefinitions.Add(new ColumnDefinition { Width = new GridLength(1, GridUnitType.Auto), MinWidth = 80 });
            }

            for (var i = 0; i < columns.Count; i++)
            {
                var cellText = i < row.Count ? row[i] : string.Empty;
                var cell = new TextBlock
                {
                    Text = cellText,
                    Padding = new Thickness(8, 4, 8, 4),
                    FontSize = 12,
                    TextTrimming = TextTrimming.CharacterEllipsis,
                    MaxWidth = 300,
                    IsTextSelectionEnabled = true,
                };
                Grid.SetColumn(cell, i);
                rowGrid.Children.Add(cell);
            }

            SqlDataGridHost.Children.Add(rowGrid);
        }
    }

    private void OnDataGridHeaderClick(int columnIndex)
    {
        if (_sqlPreviewState.GridRows.Count == 0 || _sqlPreviewState.ColumnHeaders.Count == 0)
        {
            return;
        }

        if (_sortColumnIndex == columnIndex)
        {
            _sortDescending = !_sortDescending;
        }
        else
        {
            _sortColumnIndex = columnIndex;
            _sortDescending = false;
        }

        var sorted = _sqlPreviewState.GridRows
            .OrderBy(row => columnIndex < row.Count ? row[columnIndex] : string.Empty,
                _sortDescending ? StringComparer.OrdinalIgnoreCase : StringComparer.OrdinalIgnoreCase)
            .ToArray();
        if (_sortDescending)
        {
            sorted = _sqlPreviewState.GridRows
                .OrderByDescending(row => columnIndex < row.Count ? row[columnIndex] : string.Empty,
                    StringComparer.OrdinalIgnoreCase)
                .ToArray();
        }

        RenderDataGrid(_sqlPreviewState.ColumnHeaders, sorted);
    }

    private void RenderChartFromPreviewData()
    {
        if (_sqlPreviewState.ColumnHeaders.Count == 0 || _sqlPreviewState.GridRows.Count == 0)
        {
            SqlChartRenderer.Render(SqlChartCanvas, SqlChartData.Empty, 700, 480);
            return;
        }

        var chartType = ReadComboText(SqlChartTypeComboBox);
        var categoryField = SqlChartCategoryFieldTextBox.Text.Trim();
        var valueField = SqlChartValueFieldTextBox.Text.Trim();
        var seriesField = SqlChartSeriesFieldTextBox.Text.Trim();

        var catIdx = FindColumnIndex(categoryField);
        var valIdx = FindColumnIndex(valueField);
        var serIdx = FindColumnIndex(seriesField);

        if (catIdx < 0 && _sqlPreviewState.ColumnHeaders.Count > 0) catIdx = 0;
        if (valIdx < 0 && _sqlPreviewState.ColumnHeaders.Count > 1) valIdx = 1;

        var categories = new List<string>();
        var grouped = new Dictionary<string, Dictionary<string, double>>(StringComparer.Ordinal);
        var seriesNames = new List<string>();

        foreach (var row in _sqlPreviewState.GridRows)
        {
            var cat = catIdx >= 0 && catIdx < row.Count ? row[catIdx] : string.Empty;
            var ser = serIdx >= 0 && serIdx < row.Count ? row[serIdx] : "value";
            var valStr = valIdx >= 0 && valIdx < row.Count ? row[valIdx] : "0";
            if (!double.TryParse(valStr, System.Globalization.NumberStyles.Float,
                System.Globalization.CultureInfo.InvariantCulture, out var val))
            {
                val = 0;
            }

            if (!grouped.ContainsKey(cat))
            {
                categories.Add(cat);
                grouped[cat] = new Dictionary<string, double>(StringComparer.Ordinal);
            }

            if (!seriesNames.Contains(ser, StringComparer.Ordinal))
            {
                seriesNames.Add(ser);
            }

            if (!grouped[cat].ContainsKey(ser)) grouped[cat][ser] = 0;
            grouped[cat][ser] += val;
        }

        var topN = int.TryParse(SqlChartTopNTextBox.Text.Trim(), out var n) && n > 0 ? n : 20;
        if (categories.Count > topN) categories = categories.Take(topN).ToList();

        var series = seriesNames.Select(name => new SqlChartSeries(
            name,
            categories.Select(cat => grouped.TryGetValue(cat, out var bucket) && bucket.TryGetValue(name, out var v) ? v : 0).ToArray()
        )).ToArray();

        var data = new SqlChartData(chartType, categories, series);
        SqlChartRenderer.Render(SqlChartCanvas, data, 700, 480);
    }

    private void TryRenderChartFromWorkflowResult(JsonObject? response)
    {
        if (response is null)
        {
            return;
        }

        // Check node_outputs for sql_chart_v1 results
        if (response["node_outputs"] is JsonObject nodeOutputs)
        {
            foreach (var kv in nodeOutputs)
            {
                if (kv.Value is JsonObject nodeOut && nodeOut["chart_type"] is not null)
                {
                    var chartData = SqlChartData.FromJson(nodeOut);
                    SqlChartRenderer.Render(SqlChartCanvas, chartData, 700, 480);
                    return;
                }
            }
        }

        // Fallback: check final_output
        if (response["final_output"] is JsonObject final && final["chart_type"] is not null)
        {
            var chartData = SqlChartData.FromJson(final);
            SqlChartRenderer.Render(SqlChartCanvas, chartData, 700, 480);
            return;
        }

        // No chart data — render from preview rows
        RenderChartFromPreviewData();
    }

    private int FindColumnIndex(string fieldName)
    {
        for (var i = 0; i < _sqlPreviewState.ColumnHeaders.Count; i++)
        {
            if (string.Equals(_sqlPreviewState.ColumnHeaders[i], fieldName, StringComparison.OrdinalIgnoreCase))
            {
                return i;
            }
        }

        return -1;
    }

    private void RenderSchemaBrowserState()
    {
        SqlSchemaListView.Items.Clear();
        foreach (var item in _schemaBrowserState.Schemas)
        {
            SqlSchemaListView.Items.Add(item);
        }
        SqlTableListView.Items.Clear();
        foreach (var item in _schemaBrowserState.Tables)
        {
            SqlTableListView.Items.Add(item);
        }
        SqlColumnsListView.Items.Clear();
        foreach (var item in _schemaBrowserState.Columns)
        {
            SqlColumnsListView.Items.Add(item);
        }
        SqlDiagnosticsTextBox.Text = _schemaBrowserState.Diagnostics;
    }

    private void UpdateSqlSourcePanels()
    {
        var isSqlServer = string.Equals(ReadSelectedSqlSourceType(), SqlConnectionProfile.SqlServer, StringComparison.OrdinalIgnoreCase);
        SqliteSourcePanel.Visibility = isSqlServer ? Visibility.Collapsed : Visibility.Visible;
        SqlServerSourcePanel.Visibility = isSqlServer ? Visibility.Visible : Visibility.Collapsed;
    }

    private void UpdateSqlDraftModeText()
    {
        SqlDraftModeTextBlock.Text = _sqlTextDraft.IsTextOwned ? "Mode: text-owned" : "Mode: builder-owned";
    }

    private void SetSqlTextDraft(SqlTextDraft draft)
    {
        _sqlTextDraft = draft;
        _suppressSqlTextEditorChange = true;
        try
        {
            SqlTextEditorTextBox.Text = draft.Text;
        }
        finally
        {
            _suppressSqlTextEditorChange = false;
        }
        UpdateSqlDraftModeText();
    }

    private static IReadOnlyList<string> SplitCsv(string? value)
    {
        return (value ?? string.Empty)
            .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Where(static item => !string.IsNullOrWhiteSpace(item))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();
    }

    private static string ReadComboText(ComboBox comboBox)
    {
        return (comboBox.SelectedItem as ComboBoxItem)?.Content?.ToString()?.Trim()
            ?? comboBox.SelectedValue?.ToString()?.Trim()
            ?? string.Empty;
    }

    private async void OnSqlSaveQueryClick(object sender, RoutedEventArgs e)
    {
        var sqlText = _sqlTextDraft.Text.Trim();
        if (string.IsNullOrWhiteSpace(sqlText))
        {
            SetInlineStatus("没有可保存的查询。", InlineStatusTone.Error);
            return;
        }

        var dialog = new ContentDialog
        {
            Title = "保存查询",
            Content = new TextBox { PlaceholderText = "输入查询名称", Name = "QueryNameInput" },
            PrimaryButtonText = "保存",
            CloseButtonText = "取消",
            XamlRoot = Content.XamlRoot,
        };

        var result = await dialog.ShowAsync();
        if (result == ContentDialogResult.Primary && dialog.Content is TextBox input
            && !string.IsNullOrWhiteSpace(input.Text))
        {
            _queryHistory.SaveQuery(input.Text.Trim(), sqlText,
                _sqlConnectionProfile.NormalizedSourceType, _sqlConnectionProfile.Database);
            RenderQueryHistoryList();
            SetInlineStatus($"查询已保存: {input.Text.Trim()}", InlineStatusTone.Success);
        }
    }

    private void OnSqlHistorySelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (SqlHistoryListView.SelectedItem is not SqlQueryHistoryEntry entry)
        {
            return;
        }

        SetSqlTextDraft(SqlStudioDraftController.MarkTextOwned(entry.SqlText));
        UpdateSqlDraftModeText();
        SqlHistoryListView.SelectedItem = null;
    }

    private void RenderQueryHistoryList()
    {
        SqlHistoryListView.Items.Clear();
        foreach (var entry in _queryHistory.SavedQueries)
        {
            SqlHistoryListView.Items.Add(entry);
        }
        foreach (var entry in _queryHistory.History.Take(20))
        {
            SqlHistoryListView.Items.Add(entry);
        }
    }

    private async void OnSqlExportCsvClick(object sender, RoutedEventArgs e)
    {
        if (_sqlPreviewState.ColumnHeaders.Count == 0)
        {
            SetInlineStatus("没有可导出的数据。", InlineStatusTone.Error);
            return;
        }

        var csv = SqlExportService.ExportCsv(_sqlPreviewState.ColumnHeaders, _sqlPreviewState.GridRows);
        await SaveExportFile(csv, "csv", "CSV Files", "*.csv");
    }

    private async void OnSqlExportJsonClick(object sender, RoutedEventArgs e)
    {
        if (_sqlPreviewState.ColumnHeaders.Count == 0)
        {
            SetInlineStatus("没有可导出的数据。", InlineStatusTone.Error);
            return;
        }

        var json = SqlExportService.ExportJson(_sqlPreviewState.ColumnHeaders, _sqlPreviewState.GridRows);
        await SaveExportFile(json, "json", "JSON Files", "*.json");
    }

    private async Task SaveExportFile(string content, string defaultExt, string filterName, string filterPattern)
    {
        try
        {
            var picker = new Windows.Storage.Pickers.FileSavePicker();
            var hWnd = WinRT.Interop.WindowNative.GetWindowHandle(this);
            WinRT.Interop.InitializeWithWindow.Initialize(picker, hWnd);
            picker.SuggestedStartLocation = Windows.Storage.Pickers.PickerLocationId.DocumentsLibrary;
            picker.SuggestedFileName = $"sql_export_{DateTime.Now:yyyyMMdd_HHmmss}";
            picker.FileTypeChoices.Add(filterName, new List<string> { $".{defaultExt}" });

            var file = await picker.PickSaveFileAsync();
            if (file is null)
            {
                return;
            }

            await Windows.Storage.FileIO.WriteTextAsync(file, content);
            SetInlineStatus($"已导出到 {file.Path}", InlineStatusTone.Success);
        }
        catch (Exception ex)
        {
            SetInlineStatus($"导出失败: {ex.Message}", InlineStatusTone.Error);
        }
    }

    private string ReadSelectedSqlSourceType()
    {
        return ReadComboText(SqlSourceTypeComboBox);
    }

    private static void SetComboByText(ComboBox comboBox, string expected)
    {
        var normalized = expected?.Trim() ?? string.Empty;
        for (var index = 0; index < comboBox.Items.Count; index += 1)
        {
            if (comboBox.Items[index] is ComboBoxItem item
                && string.Equals(item.Content?.ToString(), normalized, StringComparison.OrdinalIgnoreCase))
            {
                comboBox.SelectedIndex = index;
                return;
            }
        }
    }
}
