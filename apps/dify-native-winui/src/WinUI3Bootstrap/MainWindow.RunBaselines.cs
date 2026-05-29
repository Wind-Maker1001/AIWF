using AIWF.Native.Runtime;
using Microsoft.UI.Xaml;
using Microsoft.UI.Xaml.Controls;

namespace AIWF.Native;

public sealed partial class MainWindow
{
    private GovernanceRunBaselineItem? _selectedRunBaseline;
    private WorkflowRunCompareResult? _lastRunCompareResult;
    private WorkflowRunBaselineRegressionResult? _lastRunBaselineRegressionResult;
    private WorkflowRunLineageResult? _lastRunLineageResult;

    private async Task RefreshRunBaselinesAsync()
    {
        try
        {
            var baseUrl = await EnsureGovernanceBoundaryLoadedAsync();
            var baselines = await _workflowRunBaselineCoordinator.RefreshBaselinesAsync(baseUrl, ApiKeyTextBox.Text.Trim());
            BindRunBaselines(baselines, _selectedRunBaseline?.BaselineId);
        }
        catch (Exception ex)
        {
            RunBaselinesListView.Items.Clear();
            _selectedRunBaseline = null;
            RunBaselinesListView.SelectedItem = null;
            ApplyRunBaselineActionState();
            RunBaselineSummaryTextBlock.Text = "-";
            RunBaselineDetailsListView.Items.Clear();
            RunLineageTextBox.Text = string.Empty;
            SetGovernanceStatus($"Refresh run baselines failed: {ex.Message}", isError: true);
        }
    }

    private void BindRunBaselines(IReadOnlyList<GovernanceRunBaselineItem> items, string? selectedBaselineId)
    {
        RunBaselinesListView.Items.Clear();
        GovernanceRunBaselineItem? selected = null;
        foreach (var item in items)
        {
            RunBaselinesListView.Items.Add(item);
            if (!string.IsNullOrWhiteSpace(selectedBaselineId)
                && string.Equals(selectedBaselineId, item.BaselineId, StringComparison.Ordinal))
            {
                selected = item;
            }
        }

        _selectedRunBaseline = selected;
        RunBaselinesListView.SelectedItem = selected;
        ApplyRunBaselineActionState();
    }

    private GovernanceWorkflowRunItem? SelectedGovernanceRun =>
        GovernanceRecentRunsListView.SelectedItem as GovernanceWorkflowRunItem;

    private void ApplyRunBaselineActionState()
    {
        var hasSelectedRun = SelectedGovernanceRun is not null;
        UseGovernanceRunAsAButton.IsEnabled = hasSelectedRun;
        UseGovernanceRunAsBButton.IsEnabled = hasSelectedRun;
        SaveRunBaselineButton.IsEnabled = !string.IsNullOrWhiteSpace(RunBaselineRunATextBox.Text);
        CompareRunsButton.IsEnabled =
            !string.IsNullOrWhiteSpace(RunBaselineRunATextBox.Text)
            && !string.IsNullOrWhiteSpace(RunBaselineRunBTextBox.Text);
        CompareWithBaselineButton.IsEnabled =
            !string.IsNullOrWhiteSpace(RunBaselineRunBTextBox.Text)
            && (_selectedRunBaseline is not null || RunBaselinesListView.Items.Count > 0);
        LoadRunLineageButton.IsEnabled = !string.IsNullOrWhiteSpace(RunBaselineRunATextBox.Text);
        ExportRunCompareReportButton.IsEnabled = _lastRunCompareResult is not null || _lastRunBaselineRegressionResult is not null;
        ExportRunLineageButton.IsEnabled = _lastRunLineageResult?.Ok == true;
    }

    private async void OnRefreshRunBaselinesClick(object sender, RoutedEventArgs e)
    {
        await RefreshRunBaselinesAsync();
    }

    private void OnRunBaselineSelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        _selectedRunBaseline = RunBaselinesListView.SelectedItem as GovernanceRunBaselineItem;
        ApplyRunBaselineActionState();
    }

    private void OnRunBaselineInputChanged(object sender, TextChangedEventArgs e)
    {
        ApplyRunBaselineActionState();
    }

    private void OnUseGovernanceRunAsAClick(object sender, RoutedEventArgs e)
    {
        if (SelectedGovernanceRun is null)
        {
            return;
        }

        RunBaselineRunATextBox.Text = SelectedGovernanceRun.RunId;
        if (string.IsNullOrWhiteSpace(RunBaselineNameTextBox.Text))
        {
            RunBaselineNameTextBox.Text = $"baseline_{SelectedGovernanceRun.RunId[..Math.Min(8, SelectedGovernanceRun.RunId.Length)]}";
        }
        ApplyRunBaselineActionState();
    }

    private void OnUseGovernanceRunAsBClick(object sender, RoutedEventArgs e)
    {
        if (SelectedGovernanceRun is null)
        {
            return;
        }

        RunBaselineRunBTextBox.Text = SelectedGovernanceRun.RunId;
        ApplyRunBaselineActionState();
    }

    private async void OnSaveRunBaselineClick(object sender, RoutedEventArgs e)
    {
        var runId = RunBaselineRunATextBox.Text.Trim();
        if (string.IsNullOrWhiteSpace(runId))
        {
            SetGovernanceStatus("Run A is required to save a baseline.", isError: true);
            return;
        }

        try
        {
            var baseUrl = await EnsureGovernanceBoundaryLoadedAsync();
            var saved = await _workflowRunBaselineCoordinator.SaveCurrentRunAsBaselineAsync(
                baseUrl,
                ApiKeyTextBox.Text.Trim(),
                runId,
                RunBaselineNameTextBox.Text.Trim());
            await RefreshRunBaselinesAsync();
            _selectedRunBaseline = RunBaselinesListView.Items.OfType<GovernanceRunBaselineItem>()
                .FirstOrDefault(item => string.Equals(item.BaselineId, saved.BaselineId, StringComparison.Ordinal));
            RunBaselinesListView.SelectedItem = _selectedRunBaseline;
            ApplyRunBaselineActionState();
            SetGovernanceStatus($"Run baseline saved: {saved.BaselineId}", isError: false);
        }
        catch (Exception ex)
        {
            SetGovernanceStatus($"Save run baseline failed: {ex.Message}", isError: true);
        }
    }

    private async void OnCompareRunsClick(object sender, RoutedEventArgs e)
    {
        var runA = RunBaselineRunATextBox.Text.Trim();
        var runB = RunBaselineRunBTextBox.Text.Trim();
        if (string.IsNullOrWhiteSpace(runA) || string.IsNullOrWhiteSpace(runB))
        {
            SetGovernanceStatus("Run A and Run B are required for compare.", isError: true);
            return;
        }

        try
        {
            var baseUrl = await EnsureGovernanceBoundaryLoadedAsync();
            var result = await _workflowRunBaselineCoordinator.CompareRunsAsync(
                baseUrl,
                ApiKeyTextBox.Text.Trim(),
                runA,
                runB);
            _lastRunCompareResult = result;
            _lastRunBaselineRegressionResult = null;
            ApplyRunBaselineCompareResult(result);
            ApplyRunBaselineActionState();
            SetGovernanceStatus("Run compare completed.", isError: false);
        }
        catch (Exception ex)
        {
            SetGovernanceStatus($"Run compare failed: {ex.Message}", isError: true);
        }
    }

    private async void OnCompareWithBaselineClick(object sender, RoutedEventArgs e)
    {
        var runId = RunBaselineRunBTextBox.Text.Trim();
        if (string.IsNullOrWhiteSpace(runId))
        {
            SetGovernanceStatus("Run B is required to compare with baseline.", isError: true);
            return;
        }

        try
        {
            var baseUrl = await EnsureGovernanceBoundaryLoadedAsync();
            var result = await _workflowRunBaselineCoordinator.CompareRunWithBaselineAsync(
                baseUrl,
                ApiKeyTextBox.Text.Trim(),
                runId,
                _selectedRunBaseline?.BaselineId);
            _lastRunBaselineRegressionResult = result;
            _lastRunCompareResult = result.Compare;
            ApplyRunBaselineRegressionResult(result);
            ApplyRunBaselineActionState();
            SetGovernanceStatus("Baseline compare completed.", isError: false);
        }
        catch (Exception ex)
        {
            SetGovernanceStatus($"Baseline compare failed: {ex.Message}", isError: true);
        }
    }

    private async void OnLoadRunLineageClick(object sender, RoutedEventArgs e)
    {
        var runId = RunBaselineRunATextBox.Text.Trim();
        if (string.IsNullOrWhiteSpace(runId))
        {
            SetGovernanceStatus("Run A is required to load lineage.", isError: true);
            return;
        }

        try
        {
            var baseUrl = await EnsureGovernanceBoundaryLoadedAsync();
            var result = await _workflowRunBaselineCoordinator.LoadLineageAsync(
                baseUrl,
                ApiKeyTextBox.Text.Trim(),
                runId,
                RawResponseTextBox.Text,
                JobIdTextBlock.Text);
            _lastRunLineageResult = result;
            RunLineageTextBox.Text = result.RawJson;
            RunBaselineSummaryTextBlock.Text = result.StatusMessage;
            ApplyRunBaselineActionState();
            SetGovernanceStatus(result.StatusMessage, isError: !result.Ok);
        }
        catch (Exception ex)
        {
            SetGovernanceStatus($"Load lineage failed: {ex.Message}", isError: true);
        }
    }

    private async void OnExportRunCompareReportClick(object sender, RoutedEventArgs e)
    {
        var compare = _lastRunCompareResult ?? _lastRunBaselineRegressionResult?.Compare;
        if (compare is null)
        {
            SetGovernanceStatus("No compare result available to export.", isError: true);
            return;
        }

        try
        {
            var picker = new Windows.Storage.Pickers.FileSavePicker();
            var hWnd = WinRT.Interop.WindowNative.GetWindowHandle(this);
            WinRT.Interop.InitializeWithWindow.Initialize(picker, hWnd);
            picker.SuggestedStartLocation = Windows.Storage.Pickers.PickerLocationId.DocumentsLibrary;
            picker.SuggestedFileName = $"run_compare_{DateTime.Now:yyyyMMdd_HHmmss}";
            picker.FileTypeChoices.Add("Markdown", new List<string> { ".md" });
            picker.FileTypeChoices.Add("HTML", new List<string> { ".html" });

            var file = await picker.PickSaveFileAsync();
            if (file is null)
            {
                return;
            }

            var extension = Path.GetExtension(file.Path).ToLowerInvariant();
            var content = extension == ".html"
                ? WorkflowRunBaselineExportSupport.RenderCompareHtml(compare)
                : WorkflowRunBaselineExportSupport.RenderCompareMarkdown(compare);
            await Windows.Storage.FileIO.WriteTextAsync(file, content);
            SetGovernanceStatus($"Run compare report exported: {file.Path}", isError: false);
        }
        catch (Exception ex)
        {
            SetGovernanceStatus($"Export run compare report failed: {ex.Message}", isError: true);
        }
    }

    private async void OnExportRunLineageClick(object sender, RoutedEventArgs e)
    {
        if (_lastRunLineageResult?.Ok != true)
        {
            SetGovernanceStatus("No lineage payload available to export.", isError: true);
            return;
        }

        try
        {
            var picker = new Windows.Storage.Pickers.FileSavePicker();
            var hWnd = WinRT.Interop.WindowNative.GetWindowHandle(this);
            WinRT.Interop.InitializeWithWindow.Initialize(picker, hWnd);
            picker.SuggestedStartLocation = Windows.Storage.Pickers.PickerLocationId.DocumentsLibrary;
            picker.SuggestedFileName = $"run_lineage_{DateTime.Now:yyyyMMdd_HHmmss}";
            picker.FileTypeChoices.Add("JSON", new List<string> { ".json" });

            var file = await picker.PickSaveFileAsync();
            if (file is null)
            {
                return;
            }

            await Windows.Storage.FileIO.WriteTextAsync(file, _lastRunLineageResult.RawJson);
            SetGovernanceStatus($"Run lineage exported: {file.Path}", isError: false);
        }
        catch (Exception ex)
        {
            SetGovernanceStatus($"Export run lineage failed: {ex.Message}", isError: true);
        }
    }

    private void ApplyRunBaselineCompareResult(WorkflowRunCompareResult result)
    {
        RunBaselineSummaryTextBlock.Text = WorkflowRunBaselinePresenter.BuildCompareSummary(result);
        RunBaselineDetailsListView.Items.Clear();
        foreach (var row in WorkflowRunBaselinePresenter.BuildCompareRows(result))
        {
            RunBaselineDetailsListView.Items.Add(row);
        }
    }

    private void ApplyRunBaselineRegressionResult(WorkflowRunBaselineRegressionResult result)
    {
        RunBaselineSummaryTextBlock.Text = WorkflowRunBaselinePresenter.BuildRegressionSummary(result);
        RunBaselineDetailsListView.Items.Clear();
        foreach (var row in WorkflowRunBaselinePresenter.BuildCompareRows(result.Compare))
        {
            RunBaselineDetailsListView.Items.Add(row);
        }
    }
}
