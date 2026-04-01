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

    private void InitializeSqlStudioState()
    {
        ApplySqlConnectionProfileToControls(_sqlConnectionProfile);
        ApplySqlBuilderDraftToControls(_sqlBuilderDraft);
        SetSqlTextDraft(_sqlTextDraft);
        ApplySqlPreviewState(SqlPreviewState.Empty);
        UpdateSqlSourcePanels();
        UpdateSqlDraftModeText();
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
        }
        catch (Exception ex)
        {
            ApplySqlPreviewState(new SqlPreviewState(
                Ok: false,
                StatusText: $"Preview failed: {ex.Message}",
                GeneratedSql: _sqlTextDraft.Text,
                RawJson: string.Empty,
                Diagnostics: ex.Message,
                RowDisplayItems: Array.Empty<string>()));
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
                ChartType: "bar",
                CategoryField: "category",
                ValueField: "value",
                SeriesField: "series",
                TopN: 20));
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
    }

    private void ApplySqlPreviewState(SqlPreviewState state)
    {
        _sqlPreviewState = state;
        SqlPreviewStatusTextBlock.Text = state.StatusText;
        SqlGeneratedSqlTextBox.Text = state.GeneratedSql;
        SqlRawJsonTextBox.Text = state.RawJson;
        SqlDiagnosticsTextBox.Text = state.Diagnostics;
        SqlPreviewRowsListView.Items.Clear();
        foreach (var item in state.RowDisplayItems)
        {
            SqlPreviewRowsListView.Items.Add(item);
        }
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
