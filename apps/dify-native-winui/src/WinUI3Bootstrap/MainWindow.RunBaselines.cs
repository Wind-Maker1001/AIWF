using AIWF.Native.Runtime;
using Microsoft.UI.Xaml;
using Microsoft.UI.Xaml.Controls;

namespace AIWF.Native;

public sealed partial class MainWindow
{
    private GovernanceRunBaselineItem? _selectedRunBaseline;

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
            ApplyRunBaselineCompareResult(result);
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
            ApplyRunBaselineRegressionResult(result);
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
            RunLineageTextBox.Text = result.RawJson;
            RunBaselineSummaryTextBlock.Text = result.StatusMessage;
            SetGovernanceStatus(result.StatusMessage, isError: !result.Ok);
        }
        catch (Exception ex)
        {
            SetGovernanceStatus($"Load lineage failed: {ex.Message}", isError: true);
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
