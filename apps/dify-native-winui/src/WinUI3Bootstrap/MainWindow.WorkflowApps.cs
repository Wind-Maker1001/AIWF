using System.Text.Json;
using System.Text.Json.Nodes;
using AIWF.Native.Runtime;
using Microsoft.UI.Xaml;
using Microsoft.UI.Xaml.Controls;
using Microsoft.UI.Xaml.Media;

namespace AIWF.Native;

public sealed partial class MainWindow
{
    private GovernanceWorkflowAppItem? _selectedWorkflowApp;
    private WorkflowAppPublishPreflightReport? _lastWorkflowAppPreflightReport;
    private readonly Dictionary<string, Control> _workflowAppRunParamControls = new(StringComparer.Ordinal);
    private IReadOnlyList<WorkflowAppSchemaField> _workflowAppSchemaFields = Array.Empty<WorkflowAppSchemaField>();

    private async Task RefreshWorkflowAppsAsync()
    {
        try
        {
            var baseUrl = await EnsureGovernanceBoundaryLoadedAsync();
            var items = await _workflowAppPublishCoordinator.RefreshAppsAsync(baseUrl, ApiKeyTextBox.Text.Trim());
            BindWorkflowApps(items, _selectedWorkflowApp?.AppId);
        }
        catch (Exception ex)
        {
            WorkflowAppsListView.Items.Clear();
            _selectedWorkflowApp = null;
            WorkflowAppsListView.SelectedItem = null;
            WorkflowAppPublishedVersionTextBlock.Text = "-";
            SetGovernanceStatus($"Refresh workflow apps failed: {ex.Message}", isError: true);
        }
    }

    private async void OnRefreshWorkflowAppsClick(object sender, RoutedEventArgs e)
    {
        await RefreshWorkflowAppsAsync();
    }

    private void OnWorkflowAppSelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        _selectedWorkflowApp = WorkflowAppsListView.SelectedItem as GovernanceWorkflowAppItem;
        WorkflowAppPublishedVersionTextBlock.Text = _selectedWorkflowApp?.PublishedVersionId ?? "-";
    }

    private void OnWorkflowAppPublishSourceSelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        UpdateWorkflowAppTemplatePolicyPreviewFromCurrentState();
    }

    private void OnWorkflowAppPublishPolicyChanged(object sender, RoutedEventArgs e)
    {
        UpdateWorkflowAppTemplatePolicyPreviewFromCurrentState();
    }

    private void OnApplyWorkflowAppSchemaJsonClick(object sender, RoutedEventArgs e)
    {
        try
        {
            var schema = WorkflowAppSchemaSupport.ParseSchemaJson(WorkflowAppSchemaJsonTextBox.Text);
            var preferred = WorkflowAppRunParamsSupport.ParseRunParamsJson(WorkflowAppRunParamsJsonTextBox.Text);
            _workflowAppSchemaFields = WorkflowAppSchemaSupport.EnumerateFields(schema);
            if (_workflowAppSchemaFields.Count > 0)
            {
                RenderWorkflowAppRunParamsForm(_workflowAppSchemaFields, WorkflowAppRunParamsSupport.BuildDefaultsFromSchema(_workflowAppSchemaFields, preferred));
                SyncWorkflowAppRunParamsJsonFromForm();
            }
            else
            {
                RenderWorkflowAppRunParamsForm(Array.Empty<WorkflowAppSchemaField>(), new JsonObject());
                WorkflowAppRunParamsJsonTextBox.Text = PrettyJson(preferred.ToJsonString());
                UpdateWorkflowAppTemplatePolicyPreviewFromCurrentState();
            }
            SetGovernanceStatus("Workflow app schema applied.", isError: false);
        }
        catch (Exception ex)
        {
            SetGovernanceStatus(WorkflowAppSchemaSupport.BuildSchemaErrorText(ex.Message), isError: true);
        }
    }

    private void OnSyncWorkflowAppRunParamsJsonClick(object sender, RoutedEventArgs e)
    {
        SyncWorkflowAppRunParamsJsonFromForm();
        SetGovernanceStatus("Workflow app run params synchronized to JSON.", isError: false);
    }

    private void OnLoadWorkflowAppRunParamsJsonClick(object sender, RoutedEventArgs e)
    {
        try
        {
            var preferred = WorkflowAppRunParamsSupport.ParseRunParamsJson(WorkflowAppRunParamsJsonTextBox.Text);
            if (_workflowAppSchemaFields.Count > 0)
            {
                RenderWorkflowAppRunParamsForm(_workflowAppSchemaFields, WorkflowAppRunParamsSupport.BuildDefaultsFromSchema(_workflowAppSchemaFields, preferred));
                SyncWorkflowAppRunParamsJsonFromForm();
            }
            else
            {
                WorkflowAppRunParamsJsonTextBox.Text = PrettyJson(preferred.ToJsonString());
                UpdateWorkflowAppTemplatePolicyPreviewFromCurrentState();
            }
            SetGovernanceStatus("Workflow app run params loaded from JSON.", isError: false);
        }
        catch (Exception ex)
        {
            SetGovernanceStatus(ex.Message, isError: true);
        }
    }

    private async void OnRunWorkflowAppPublishPreflightClick(object sender, RoutedEventArgs e)
    {
        if (!TryBuildWorkflowAppPublishSourceContext(out var sourceContext, out var error))
        {
            var failure = WorkflowAppPublishPreflightPresenter.CreateFailure(error);
            ApplyWorkflowAppPreflightViewState(failure);
            _lastWorkflowAppPreflightReport = null;
            SetGovernanceStatus(failure.StatusText, isError: true);
            return;
        }

        try
        {
            var report = await _workflowAppPublishPreflightCoordinator.RunPublishPreflightAsync(
                sourceContext,
                ApiKeyTextBox.Text.Trim());
            _lastWorkflowAppPreflightReport = report;
            var viewState = WorkflowAppPublishPreflightPresenter.Create(report);
            ApplyWorkflowAppPreflightViewState(viewState);
            SetGovernanceStatus(viewState.StatusText, isError: !viewState.Ok);
        }
        catch (Exception ex)
        {
            var failure = WorkflowAppPublishPreflightPresenter.CreateFailure(ex.Message);
            ApplyWorkflowAppPreflightViewState(failure);
            _lastWorkflowAppPreflightReport = null;
            SetGovernanceStatus(failure.StatusText, isError: true);
        }
    }

    private async void OnPublishWorkflowAppClick(object sender, RoutedEventArgs e)
    {
        if (!TryBuildWorkflowAppPublishSourceContext(out var sourceContext, out var error))
        {
            var failure = WorkflowAppPublishPresenter.CreateFailure(error);
            WorkflowAppPublishedVersionTextBlock.Text = failure.PublishedVersionText;
            SetGovernanceStatus(failure.StatusText, isError: true);
            return;
        }

        JsonObject paramsSchema;
        try
        {
            paramsSchema = WorkflowAppSchemaSupport.ParseSchemaJson(WorkflowAppSchemaJsonTextBox.Text);
        }
        catch (Exception ex)
        {
            SetGovernanceStatus(WorkflowAppSchemaSupport.BuildSchemaErrorText(ex.Message), isError: true);
            return;
        }

        if (_workflowAppSchemaFields.Count == 0)
        {
            _workflowAppSchemaFields = WorkflowAppSchemaSupport.EnumerateFields(paramsSchema);
        }

        JsonObject runtimeDefaults;
        try
        {
            runtimeDefaults = CollectWorkflowAppRunParamsFromControls();
        }
        catch (Exception ex)
        {
            SetGovernanceStatus(ex.Message, isError: true);
            return;
        }

        var templatePolicyPreview = WorkflowAppRunParamsSupport.BuildTemplatePolicyPreview(
            PublishRequirePreflightCheckBox.IsChecked == true,
            runtimeDefaults);
        WorkflowAppTemplatePolicyPreviewTextBox.Text = PrettyJson(templatePolicyPreview.ToJsonString());

        var formState = new WorkflowAppPublishFormState(
            Name: (WorkflowAppPublishNameTextBox.Text ?? string.Empty).Trim(),
            AppId: (WorkflowAppIdTextBox.Text ?? string.Empty).Trim(),
            RequirePreflight: PublishRequirePreflightCheckBox.IsChecked == true,
            ParamsSchema: paramsSchema,
            RuntimeDefaults: runtimeDefaults,
            TemplatePolicyPreview: templatePolicyPreview);

        var baseUrl = await EnsureGovernanceBoundaryLoadedAsync();
        var result = await _workflowAppPublishCoordinator.PublishAsync(
            baseUrl,
            ApiKeyTextBox.Text.Trim(),
            sourceContext,
            formState);

        if (result.PreflightReport is not null)
        {
            _lastWorkflowAppPreflightReport = result.PreflightReport;
            ApplyWorkflowAppPreflightViewState(WorkflowAppPublishPreflightPresenter.Create(result.PreflightReport));
        }

        if (result.Ok && result.PublishedApp is not null)
        {
            BindWorkflowApps(result.Items, result.PublishedApp.AppId);
            var viewState = WorkflowAppPublishPresenter.CreateSuccess(result.PublishedApp);
            WorkflowAppPublishedVersionTextBlock.Text = viewState.PublishedVersionText;
            SetGovernanceStatus(viewState.StatusText, isError: false);
            return;
        }

        var failureState = WorkflowAppPublishPresenter.CreateFailure(result.ErrorMessage, result.ErrorCode, result.ErrorItems);
        WorkflowAppPublishedVersionTextBlock.Text = failureState.PublishedVersionText;
        SetGovernanceStatus(failureState.StatusText, isError: true);
    }

    private void BindWorkflowApps(IReadOnlyList<GovernanceWorkflowAppItem> items, string? selectedAppId)
    {
        WorkflowAppsListView.Items.Clear();
        GovernanceWorkflowAppItem? selected = null;
        foreach (var item in items)
        {
            WorkflowAppsListView.Items.Add(item);
            if (!string.IsNullOrWhiteSpace(selectedAppId)
                && string.Equals(selectedAppId, item.AppId, StringComparison.Ordinal))
            {
                selected = item;
            }
        }

        _selectedWorkflowApp = selected;
        WorkflowAppsListView.SelectedItem = selected;
        WorkflowAppPublishedVersionTextBlock.Text = selected?.PublishedVersionId ?? "-";
    }

    private void RenderWorkflowAppRunParamsForm(IReadOnlyList<WorkflowAppSchemaField> fields, JsonObject values)
    {
        _workflowAppRunParamControls.Clear();
        WorkflowAppRunParamsFormHost.Children.Clear();
        if (fields.Count == 0)
        {
            WorkflowAppRunParamsFormHost.Children.Add(new TextBlock
            {
                Text = "No app params schema defined.",
                Foreground = new SolidColorBrush(Windows.UI.Color.FromArgb(0xFF, 0x6B, 0x72, 0x80))
            });
            return;
        }

        foreach (var field in fields)
        {
            WorkflowAppRunParamsFormHost.Children.Add(new TextBlock
            {
                Text = field.Required ? $"{field.Key} *" : field.Key,
                Foreground = new SolidColorBrush(Windows.UI.Color.FromArgb(0xFF, 0x37, 0x47, 0x51))
            });

            if (!string.IsNullOrWhiteSpace(field.Description))
            {
                WorkflowAppRunParamsFormHost.Children.Add(new TextBlock
                {
                    Text = field.Description,
                    Foreground = new SolidColorBrush(Windows.UI.Color.FromArgb(0xFF, 0x6B, 0x72, 0x80)),
                    TextWrapping = TextWrapping.Wrap
                });
            }

            var control = CreateWorkflowAppRunParamControl(field, values[field.Key]);
            WorkflowAppRunParamsFormHost.Children.Add(control);
            _workflowAppRunParamControls[field.Key] = control;
        }
    }

    private Control CreateWorkflowAppRunParamControl(WorkflowAppSchemaField field, JsonNode? value)
    {
        if (field.EnumValues is { Count: > 0 })
        {
            var combo = new ComboBox
            {
                Style = ResolveStyleResource("TouchComboBoxStyle"),
                MinHeight = 44,
                Tag = field.Type,
            };
            foreach (var enumValue in field.EnumValues)
            {
                combo.Items.Add(new ComboBoxItem { Content = enumValue });
            }
            SetComboByText(combo, value?.GetValue<string>() ?? field.EnumValues[0]);
            return combo;
        }

        if (string.Equals(field.Type, "boolean", StringComparison.Ordinal))
        {
            var combo = new ComboBox
            {
                Style = ResolveStyleResource("TouchComboBoxStyle"),
                MinHeight = 44,
                Tag = field.Type,
            };
            combo.Items.Add(new ComboBoxItem { Content = "true" });
            combo.Items.Add(new ComboBoxItem { Content = "false" });
            SetComboByText(combo, (value?.GetValue<bool?>() ?? false) ? "true" : "false");
            return combo;
        }

        var textBox = new TextBox
        {
            Style = ResolveStyleResource("TouchTextBoxStyle"),
            MinHeight = field.Type is "object" or "array" ? 88 : 44,
            AcceptsReturn = field.Type is "object" or "array",
            TextWrapping = field.Type is "object" or "array" ? TextWrapping.Wrap : TextWrapping.NoWrap,
            Tag = field.Type,
            Text = value is null
                ? string.Empty
                : field.Type is "object" or "array"
                    ? JsonSerializer.Serialize(value, new JsonSerializerOptions { WriteIndented = true })
                    : value.ToString(),
        };
        return textBox;
    }

    private JsonObject CollectWorkflowAppRunParamsFromControls()
    {
        if (_workflowAppSchemaFields.Count == 0)
        {
            return WorkflowAppRunParamsSupport.ParseRunParamsJson(WorkflowAppRunParamsJsonTextBox.Text);
        }

        var raw = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var field in _workflowAppSchemaFields)
        {
            if (!_workflowAppRunParamControls.TryGetValue(field.Key, out var control))
            {
                continue;
            }

            raw[field.Key] = control switch
            {
                ComboBox combo => ReadComboText(combo),
                TextBox box => box.Text,
                _ => string.Empty,
            };
        }

        return WorkflowAppRunParamsSupport.CollectRunParamsFromRawValues(_workflowAppSchemaFields, raw);
    }

    private void UpdateWorkflowAppTemplatePolicyPreviewFromCurrentState()
    {
        try
        {
            var runtimeDefaults = _workflowAppSchemaFields.Count > 0
                ? CollectWorkflowAppRunParamsFromControls()
                : WorkflowAppRunParamsSupport.ParseRunParamsJson(WorkflowAppRunParamsJsonTextBox.Text);
            var preview = WorkflowAppRunParamsSupport.BuildTemplatePolicyPreview(
                PublishRequirePreflightCheckBox.IsChecked == true,
                runtimeDefaults);
            WorkflowAppTemplatePolicyPreviewTextBox.Text = PrettyJson(preview.ToJsonString());
        }
        catch
        {
            WorkflowAppTemplatePolicyPreviewTextBox.Text = "{}";
        }
    }

    private void SyncWorkflowAppRunParamsJsonFromForm()
    {
        var runtimeDefaults = CollectWorkflowAppRunParamsFromControls();
        WorkflowAppRunParamsJsonTextBox.Text = PrettyJson(runtimeDefaults.ToJsonString());
        UpdateWorkflowAppTemplatePolicyPreviewFromCurrentState();
    }

    private bool TryBuildWorkflowAppPublishSourceContext(out WorkflowAppPublishSourceContext context, out string error)
    {
        var source = string.Equals(ReadComboText(WorkflowAppPublishSourceComboBox), "sql_studio", StringComparison.OrdinalIgnoreCase)
            ? WorkflowAppPublishSourceKind.SqlStudio
            : WorkflowAppPublishSourceKind.Canvas;

        if (source == WorkflowAppPublishSourceKind.Canvas)
        {
            var document = BuildWorkflowGraphDocumentFromCanvas();
            if (document is null || document.Nodes.Count == 0)
            {
                context = default!;
                error = "Current Canvas does not contain a publishable workflow.";
                return false;
            }

            context = new WorkflowAppPublishSourceContext(
                source,
                _sqlConnectionProfile.ResolveAccelUrl(BridgeUrlTextBox.Text),
                document);
            error = string.Empty;
            return true;
        }

        _sqlConnectionProfile = CollectSqlConnectionProfileFromControls();
        _sqlBuilderDraft = CollectSqlBuilderDraftFromControls();
        if (string.IsNullOrWhiteSpace(_sqlTextDraft.Text) && string.IsNullOrWhiteSpace(_sqlBuilderDraft.Table))
        {
            context = default!;
            error = "Current SQL Studio draft cannot build a publishable workflow.";
            return false;
        }

        var sqlResult = _sqlStudioCanvasCoordinator.BuildWorkflowDocument(
            _sqlConnectionProfile,
            _sqlBuilderDraft,
            _sqlTextDraft,
            SqlSelectedTableTextBox.Text);
        SetSqlTextDraft(sqlResult.EffectiveTextDraft);
        context = new WorkflowAppPublishSourceContext(
            source,
            _sqlConnectionProfile.ResolveAccelUrl(BridgeUrlTextBox.Text),
            sqlResult.Document);
        error = string.Empty;
        return true;
    }

    private void ApplyWorkflowAppPreflightViewState(WorkflowAppPublishPreflightViewState state)
    {
        WorkflowAppPublishPreflightSummaryTextBlock.Text = string.IsNullOrWhiteSpace(state.SummaryText) ? "-" : state.SummaryText;
        WorkflowAppPublishPreflightIssuesListView.Items.Clear();
        foreach (var item in state.IssueRows)
        {
            WorkflowAppPublishPreflightIssuesListView.Items.Add(item);
        }
    }
}
