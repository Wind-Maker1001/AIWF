using System.Text.Json.Nodes;
using AIWF.Native.Runtime;
using Microsoft.UI.Xaml;
using Microsoft.UI.Xaml.Controls;
using Microsoft.UI.Xaml.Media;

namespace AIWF.Native;

public sealed partial class MainWindow
{
    private WorkflowTemplateCatalogItem? _selectedWorkflowTemplate;
    private readonly Dictionary<string, Control> _workflowTemplateParamControls = new(StringComparer.Ordinal);
    private IReadOnlyList<WorkflowAppSchemaField> _workflowTemplateSchemaFields = Array.Empty<WorkflowAppSchemaField>();
    private JsonObject _workflowTemplateParamsSchema = new();
    private bool _workflowTemplateSectionInitialized;

    private void InitializeWorkflowTemplateSection()
    {
        if (_workflowTemplateSectionInitialized)
        {
            return;
        }

        _workflowTemplateSectionInitialized = true;
        WorkflowTemplateOriginTextBlock.Text = "-";
        WorkflowTemplateStatusTextBlock.Text = "-";
        WorkflowTemplateParamsJsonTextBox.Text = "{}";
        TemplateRequirePreflightCheckBox.IsChecked = true;
        RenderWorkflowTemplateParamsForm(Array.Empty<WorkflowAppSchemaField>(), new JsonObject());
        UpdateWorkflowTemplateActionState();
        _ = RefreshWorkflowTemplatesAsync();
    }

    private async Task RefreshWorkflowTemplatesAsync()
    {
        try
        {
            var items = await _workflowTemplateAuthoringCoordinator.RefreshTemplatesAsync();
            BindWorkflowTemplates(items, _selectedWorkflowTemplate?.Id);
            WorkflowTemplateStatusTextBlock.Text = $"Workflow templates refreshed: {items.Count}";
            SetInlineStatus("Workflow templates refreshed.", InlineStatusTone.Success);
        }
        catch (Exception ex)
        {
            BindWorkflowTemplates(Array.Empty<WorkflowTemplateCatalogItem>(), null);
            WorkflowTemplateStatusTextBlock.Text = $"Refresh workflow templates failed: {ex.Message}";
            SetInlineStatus($"Refresh workflow templates failed: {ex.Message}", InlineStatusTone.Error);
        }
    }

    private void BindWorkflowTemplates(IReadOnlyList<WorkflowTemplateCatalogItem> items, string? selectedId)
    {
        WorkflowTemplateSelectComboBox.Items.Clear();
        WorkflowTemplateCatalogItem? selected = null;
        foreach (var item in items)
        {
            WorkflowTemplateSelectComboBox.Items.Add(item);
            if (!string.IsNullOrWhiteSpace(selectedId)
                && string.Equals(selectedId, item.Id, StringComparison.Ordinal))
            {
                selected = item;
            }
        }

        if (selected is null && items.Count > 0)
        {
            selected = items[0];
        }

        _selectedWorkflowTemplate = selected;
        WorkflowTemplateSelectComboBox.SelectedItem = selected;
        ApplyWorkflowTemplateSelection();
    }

    private void ApplyWorkflowTemplateSelection()
    {
        if (_selectedWorkflowTemplate is null)
        {
            _workflowTemplateParamsSchema = new JsonObject();
            _workflowTemplateSchemaFields = Array.Empty<WorkflowAppSchemaField>();
            WorkflowTemplateOriginTextBlock.Text = "-";
            WorkflowTemplateParamsJsonTextBox.Text = "{}";
            TemplateRequirePreflightCheckBox.IsChecked = true;
            RenderWorkflowTemplateParamsForm(Array.Empty<WorkflowAppSchemaField>(), new JsonObject());
            UpdateWorkflowTemplateActionState();
            return;
        }

        _workflowTemplateParamsSchema = WorkflowTemplateContractSupport.CloneObject(_selectedWorkflowTemplate.ParamsSchema);
        _workflowTemplateSchemaFields = WorkflowAppSchemaSupport.EnumerateFields(_workflowTemplateParamsSchema);
        var preferred = _workflowTemplateSchemaFields.Count == 0
            ? WorkflowTemplateContractSupport.CloneObject(_selectedWorkflowTemplate.RuntimeDefaults)
            : WorkflowAppRunParamsSupport.BuildDefaultsFromSchema(
                _workflowTemplateSchemaFields,
                _selectedWorkflowTemplate.RuntimeDefaults);
        TemplateRequirePreflightCheckBox.IsChecked = WorkflowAppSchemaSupport.ReadRequirePreflight(_selectedWorkflowTemplate.Governance);
        WorkflowTemplateOriginTextBlock.Text = BuildWorkflowTemplateOriginText(_selectedWorkflowTemplate);
        RenderWorkflowTemplateParamsForm(_workflowTemplateSchemaFields, preferred);
        WorkflowTemplateParamsJsonTextBox.Text = PrettyJson(preferred.ToJsonString());
        UpdateWorkflowTemplateActionState();
    }

    private void RenderWorkflowTemplateParamsForm(IReadOnlyList<WorkflowAppSchemaField> fields, JsonObject values)
    {
        _workflowTemplateParamControls.Clear();
        WorkflowTemplateParamsFormHost.Children.Clear();
        if (fields.Count == 0)
        {
            WorkflowTemplateParamsFormHost.Children.Add(new TextBlock
            {
                Text = "No template params schema defined.",
                Foreground = new SolidColorBrush(Windows.UI.Color.FromArgb(0xFF, 0x6B, 0x72, 0x80)),
                TextWrapping = TextWrapping.Wrap
            });
            return;
        }

        foreach (var field in fields)
        {
            WorkflowTemplateParamsFormHost.Children.Add(new TextBlock
            {
                Text = field.Required ? $"{field.Key} *" : field.Key,
                Foreground = new SolidColorBrush(Windows.UI.Color.FromArgb(0xFF, 0x37, 0x47, 0x51))
            });
            if (!string.IsNullOrWhiteSpace(field.Description))
            {
                WorkflowTemplateParamsFormHost.Children.Add(new TextBlock
                {
                    Text = field.Description,
                    Foreground = new SolidColorBrush(Windows.UI.Color.FromArgb(0xFF, 0x6B, 0x72, 0x80)),
                    TextWrapping = TextWrapping.Wrap
                });
            }

            var control = CreateWorkflowAppRunParamControl(field, values[field.Key]);
            WorkflowTemplateParamsFormHost.Children.Add(control);
            _workflowTemplateParamControls[field.Key] = control;
        }
    }

    private JsonObject CollectWorkflowTemplateParamsFromCurrentState()
    {
        if (_workflowTemplateSchemaFields.Count == 0)
        {
            return WorkflowAppRunParamsSupport.ParseRunParamsJson(WorkflowTemplateParamsJsonTextBox.Text);
        }

        var raw = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var field in _workflowTemplateSchemaFields)
        {
            if (!_workflowTemplateParamControls.TryGetValue(field.Key, out var control))
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

        return WorkflowAppRunParamsSupport.CollectRunParamsFromRawValues(_workflowTemplateSchemaFields, raw);
    }

    private void SyncWorkflowTemplateParamsJsonFromForm()
    {
        var runtimeDefaults = CollectWorkflowTemplateParamsFromCurrentState();
        WorkflowTemplateParamsJsonTextBox.Text = PrettyJson(runtimeDefaults.ToJsonString());
    }

    private void UpdateWorkflowTemplateActionState()
    {
        var hasSelection = _selectedWorkflowTemplate is not null;
        ApplySelectedWorkflowTemplateButton.IsEnabled = hasSelection;
        SaveCurrentWorkflowAsTemplateButton.IsEnabled = BuildWorkflowGraphDocumentFromCanvas() is not null;
        var packSelected = hasSelection
            && string.Equals(_selectedWorkflowTemplate!.Origin, "pack", StringComparison.Ordinal)
            && !string.IsNullOrWhiteSpace(_selectedWorkflowTemplate.PackId);
        RemoveWorkflowTemplatePackButton.IsEnabled = packSelected;
        ExportWorkflowTemplatePackButton.IsEnabled = packSelected;
    }

    private static string BuildWorkflowTemplateOriginText(WorkflowTemplateCatalogItem item)
    {
        return item.Origin switch
        {
            "builtin" => "builtin",
            "local" => "local",
            "pack" => $"pack:{item.PackId}",
            _ => item.Origin,
        };
    }

    private async void OnRefreshWorkflowTemplatesClick(object sender, RoutedEventArgs e)
    {
        await RefreshWorkflowTemplatesAsync();
    }

    private void OnWorkflowTemplateSelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        _selectedWorkflowTemplate = WorkflowTemplateSelectComboBox.SelectedItem as WorkflowTemplateCatalogItem;
        ApplyWorkflowTemplateSelection();
    }

    private void OnSyncWorkflowTemplateParamsJsonClick(object sender, RoutedEventArgs e)
    {
        try
        {
            SyncWorkflowTemplateParamsJsonFromForm();
            WorkflowTemplateStatusTextBlock.Text = "Workflow template params synchronized to JSON.";
            SetInlineStatus("Workflow template params synchronized to JSON.", InlineStatusTone.Success);
        }
        catch (Exception ex)
        {
            WorkflowTemplateStatusTextBlock.Text = ex.Message;
            SetInlineStatus(ex.Message, InlineStatusTone.Error);
        }
    }

    private void OnLoadWorkflowTemplateParamsJsonClick(object sender, RoutedEventArgs e)
    {
        try
        {
            var preferred = WorkflowAppRunParamsSupport.ParseRunParamsJson(WorkflowTemplateParamsJsonTextBox.Text);
            if (_workflowTemplateSchemaFields.Count > 0)
            {
                RenderWorkflowTemplateParamsForm(
                    _workflowTemplateSchemaFields,
                    WorkflowAppRunParamsSupport.BuildDefaultsFromSchema(_workflowTemplateSchemaFields, preferred));
                SyncWorkflowTemplateParamsJsonFromForm();
            }
            else
            {
                WorkflowTemplateParamsJsonTextBox.Text = PrettyJson(preferred.ToJsonString());
            }

            WorkflowTemplateStatusTextBlock.Text = "Workflow template params loaded from JSON.";
            SetInlineStatus("Workflow template params loaded from JSON.", InlineStatusTone.Success);
        }
        catch (Exception ex)
        {
            WorkflowTemplateStatusTextBlock.Text = ex.Message;
            SetInlineStatus(ex.Message, InlineStatusTone.Error);
        }
    }

    private async void OnApplySelectedWorkflowTemplateClick(object sender, RoutedEventArgs e)
    {
        if (_selectedWorkflowTemplate is null)
        {
            WorkflowTemplateStatusTextBlock.Text = "Select a workflow template first.";
            SetInlineStatus("Select a workflow template first.", InlineStatusTone.Error);
            return;
        }

        try
        {
            var templateParams = CollectWorkflowTemplateParamsFromCurrentState();
            var accelUrl = CollectSqlConnectionProfileFromControls().ResolveAccelUrl(BridgeUrlTextBox.Text);
            var result = await _workflowTemplateAuthoringCoordinator.ApplySelectedTemplateAsync(
                _selectedWorkflowTemplate,
                templateParams,
                accelUrl,
                ApiKeyTextBox.Text.Trim());
            if (!result.Ok || result.Document is null)
            {
                WorkflowTemplateStatusTextBlock.Text = result.StatusMessage;
                SetInlineStatus(result.StatusMessage, InlineStatusTone.Error);
                return;
            }

            ApplyWorkflowGraphDocumentToCanvas(result.Document);
            SyncPublishSurfaceFromTemplateDefaults(
                _selectedWorkflowTemplate.ParamsSchema,
                templateParams,
                TemplateRequirePreflightCheckBox.IsChecked == true);
            WorkflowTemplateStatusTextBlock.Text = result.StatusMessage;
            SetInlineStatus(result.StatusMessage, InlineStatusTone.Success);
        }
        catch (Exception ex)
        {
            WorkflowTemplateStatusTextBlock.Text = ex.Message;
            SetInlineStatus(ex.Message, InlineStatusTone.Error);
        }
    }

    private async void OnSaveCurrentWorkflowAsTemplateClick(object sender, RoutedEventArgs e)
    {
        var document = BuildWorkflowGraphDocumentFromCanvas();
        if (document is null || document.Nodes.Count == 0)
        {
            WorkflowTemplateStatusTextBlock.Text = "Current canvas does not contain a template-ready workflow.";
            SetInlineStatus("Current canvas does not contain a template-ready workflow.", InlineStatusTone.Error);
            return;
        }

        var templateName = await PromptForWorkflowTemplateNameAsync(document.WorkflowId);
        if (string.IsNullOrWhiteSpace(templateName))
        {
            return;
        }

        try
        {
            var paramsSchema = ResolveWorkflowTemplateParamsSchemaForSave();
            var runtimeDefaults = CollectWorkflowTemplateParamsFromCurrentState();
            var result = await _workflowTemplateAuthoringCoordinator.SaveCurrentAsTemplateAsync(
                document,
                templateName,
                paramsSchema,
                runtimeDefaults,
                TemplateRequirePreflightCheckBox.IsChecked == true);
            BindWorkflowTemplates(result.Items, result.SelectedTemplateId);
            WorkflowTemplateStatusTextBlock.Text = result.StatusMessage;
            SetInlineStatus(result.StatusMessage, InlineStatusTone.Success);
        }
        catch (Exception ex)
        {
            WorkflowTemplateStatusTextBlock.Text = ex.Message;
            SetInlineStatus(ex.Message, InlineStatusTone.Error);
        }
    }

    private async Task<string?> PromptForWorkflowTemplateNameAsync(string defaultName)
    {
        var input = new TextBox
        {
            Text = defaultName,
            PlaceholderText = "template name",
            MinWidth = 360,
        };
        if (ResolveStyleResource("TouchTextBoxStyle") is Style style)
        {
            input.Style = style;
        }

        var dialog = new ContentDialog
        {
            Title = "Save Workflow Template",
            PrimaryButtonText = "Save",
            CloseButtonText = "Cancel",
            DefaultButton = ContentDialogButton.Primary,
            Content = input,
            XamlRoot = (Content as FrameworkElement)?.XamlRoot
        };

        var result = await dialog.ShowAsync();
        return result == ContentDialogResult.Primary
            ? input.Text.Trim()
            : null;
    }

    private JsonObject ResolveWorkflowTemplateParamsSchemaForSave()
    {
        if (_workflowTemplateParamsSchema.Count > 0)
        {
            return WorkflowAppSchemaSupport.NormalizeSchemaObject(_workflowTemplateParamsSchema);
        }

        if (!string.IsNullOrWhiteSpace(WorkflowAppSchemaJsonTextBox.Text))
        {
            return WorkflowAppSchemaSupport.ParseSchemaJson(WorkflowAppSchemaJsonTextBox.Text);
        }

        return new JsonObject();
    }

    private async void OnInstallWorkflowTemplatePackClick(object sender, RoutedEventArgs e)
    {
        try
        {
            var picker = new Windows.Storage.Pickers.FileOpenPicker();
            var hWnd = WinRT.Interop.WindowNative.GetWindowHandle(this);
            WinRT.Interop.InitializeWithWindow.Initialize(picker, hWnd);
            picker.FileTypeFilter.Add(".json");
            var file = await picker.PickSingleFileAsync();
            if (file is null)
            {
                return;
            }

            var result = await _workflowTemplateAuthoringCoordinator.InstallTemplatePackAsync(file.Path);
            BindWorkflowTemplates(result.Items, result.SelectedTemplateId);
            WorkflowTemplateStatusTextBlock.Text = result.StatusMessage;
            SetInlineStatus(result.StatusMessage, InlineStatusTone.Success);
        }
        catch (Exception ex)
        {
            WorkflowTemplateStatusTextBlock.Text = ex.Message;
            SetInlineStatus(ex.Message, InlineStatusTone.Error);
        }
    }

    private async void OnRemoveWorkflowTemplatePackClick(object sender, RoutedEventArgs e)
    {
        if (_selectedWorkflowTemplate is null
            || !string.Equals(_selectedWorkflowTemplate.Origin, "pack", StringComparison.Ordinal)
            || string.IsNullOrWhiteSpace(_selectedWorkflowTemplate.PackId))
        {
            WorkflowTemplateStatusTextBlock.Text = "Current template is not backed by a template pack.";
            SetInlineStatus("Current template is not backed by a template pack.", InlineStatusTone.Error);
            return;
        }

        try
        {
            var result = await _workflowTemplateAuthoringCoordinator.RemoveTemplatePackAsync(_selectedWorkflowTemplate.PackId);
            BindWorkflowTemplates(result.Items, result.SelectedTemplateId);
            WorkflowTemplateStatusTextBlock.Text = result.StatusMessage;
            SetInlineStatus(result.StatusMessage, InlineStatusTone.Success);
        }
        catch (Exception ex)
        {
            WorkflowTemplateStatusTextBlock.Text = ex.Message;
            SetInlineStatus(ex.Message, InlineStatusTone.Error);
        }
    }

    private async void OnExportWorkflowTemplatePackClick(object sender, RoutedEventArgs e)
    {
        if (_selectedWorkflowTemplate is null
            || !string.Equals(_selectedWorkflowTemplate.Origin, "pack", StringComparison.Ordinal)
            || string.IsNullOrWhiteSpace(_selectedWorkflowTemplate.PackId))
        {
            WorkflowTemplateStatusTextBlock.Text = "Current template is not backed by a template pack.";
            SetInlineStatus("Current template is not backed by a template pack.", InlineStatusTone.Error);
            return;
        }

        try
        {
            var picker = new Windows.Storage.Pickers.FileSavePicker();
            var hWnd = WinRT.Interop.WindowNative.GetWindowHandle(this);
            WinRT.Interop.InitializeWithWindow.Initialize(picker, hWnd);
            picker.SuggestedStartLocation = Windows.Storage.Pickers.PickerLocationId.DocumentsLibrary;
            picker.SuggestedFileName = $"{_selectedWorkflowTemplate.PackId}_export";
            picker.FileTypeChoices.Add("JSON Files", new List<string> { ".json" });
            var file = await picker.PickSaveFileAsync();
            if (file is null)
            {
                return;
            }

            var result = await _workflowTemplateAuthoringCoordinator.ExportTemplatePackAsync(
                _selectedWorkflowTemplate.PackId,
                file.Path);
            WorkflowTemplateStatusTextBlock.Text = result.StatusMessage;
            SetInlineStatus(result.StatusMessage, InlineStatusTone.Success);
        }
        catch (Exception ex)
        {
            WorkflowTemplateStatusTextBlock.Text = ex.Message;
            SetInlineStatus(ex.Message, InlineStatusTone.Error);
        }
    }

    private void SyncPublishSurfaceFromTemplateDefaults(JsonObject paramsSchema, JsonObject runtimeDefaults, bool requirePreflight)
    {
        var normalizedSchema = WorkflowAppSchemaSupport.NormalizeSchemaObject(paramsSchema);
        WorkflowAppSchemaJsonTextBox.Text = PrettyJson(normalizedSchema.ToJsonString());
        _workflowAppSchemaFields = WorkflowAppSchemaSupport.EnumerateFields(normalizedSchema);
        PublishRequirePreflightCheckBox.IsChecked = requirePreflight;
        if (_workflowAppSchemaFields.Count > 0)
        {
            RenderWorkflowAppRunParamsForm(
                _workflowAppSchemaFields,
                WorkflowAppRunParamsSupport.BuildDefaultsFromSchema(_workflowAppSchemaFields, runtimeDefaults));
        }
        else
        {
            RenderWorkflowAppRunParamsForm(Array.Empty<WorkflowAppSchemaField>(), new JsonObject());
        }

        WorkflowAppRunParamsJsonTextBox.Text = PrettyJson(runtimeDefaults.ToJsonString());
        UpdateWorkflowAppTemplatePolicyPreviewFromCurrentState();
    }
}
