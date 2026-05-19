using AIWF.Native.Runtime;
using Microsoft.UI.Xaml;
using Microsoft.UI.Xaml.Controls;

namespace AIWF.Native;

public sealed partial class MainWindow
{
    private GovernanceWorkflowVersionItem? _selectedWorkflowVersion;

    private async Task RefreshWorkflowVersionsAdminAsync()
    {
        try
        {
            var baseUrl = await EnsureGovernanceBoundaryLoadedAsync();
            var result = await _workflowVersionAdminCoordinator.RefreshVersionsAsync(baseUrl, ApiKeyTextBox.Text.Trim());
            var selectedVersionId = _selectedWorkflowVersion?.VersionId ?? string.Empty;
            WorkflowVersionsListView.Items.Clear();
            GovernanceWorkflowVersionItem? selected = null;
            foreach (var item in result.Items)
            {
                WorkflowVersionsListView.Items.Add(item);
                if (!string.IsNullOrWhiteSpace(selectedVersionId)
                    && string.Equals(selectedVersionId, item.VersionId, StringComparison.Ordinal))
                {
                    selected = item;
                }
            }

            _selectedWorkflowVersion = selected;
            WorkflowVersionsListView.SelectedItem = selected;
            ApplyWorkflowVersionSelection();
            WorkflowVersionCacheStatsTextBlock.Text = WorkflowVersionCachePresenter.BuildStatsText(result.Stats);
            SetGovernanceStatus("Workflow versions refreshed.", isError: false);
        }
        catch (Exception ex)
        {
            WorkflowVersionsListView.Items.Clear();
            _selectedWorkflowVersion = null;
            WorkflowVersionsListView.SelectedItem = null;
            ApplyWorkflowVersionSelection();
            WorkflowVersionCacheStatsTextBlock.Text = WorkflowVersionCachePresenter.BuildStatsText(await _workflowVersionAdminCoordinator.GetCacheStatsAsync());
            WorkflowVersionCompareSummaryTextBlock.Text = "-";
            WorkflowVersionCompareDetailsListView.Items.Clear();
            SetGovernanceStatus($"Refresh workflow versions failed: {ex.Message}", isError: true);
        }
    }

    private async void OnRefreshWorkflowVersionsClick(object sender, RoutedEventArgs e)
    {
        await RefreshWorkflowVersionsAdminAsync();
    }

    private void OnWorkflowVersionSelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        _selectedWorkflowVersion = WorkflowVersionsListView.SelectedItem as GovernanceWorkflowVersionItem;
        ApplyWorkflowVersionSelection();
    }

    private void ApplyWorkflowVersionSelection()
    {
        var hasSelection = _selectedWorkflowVersion is not null;
        UseWorkflowVersionAsAButton.IsEnabled = hasSelection;
        UseWorkflowVersionAsBButton.IsEnabled = hasSelection;
        ApplyWorkflowVersionActionState();
    }

    private void OnWorkflowVersionCompareInputChanged(object sender, TextChangedEventArgs e)
    {
        ApplyWorkflowVersionActionState();
    }

    private void ApplyWorkflowVersionActionState()
    {
        CompareWorkflowVersionsButton.IsEnabled =
            !string.IsNullOrWhiteSpace(WorkflowVersionCompareATextBox.Text)
            && !string.IsNullOrWhiteSpace(WorkflowVersionCompareBTextBox.Text);
    }

    private void OnUseWorkflowVersionAsAClick(object sender, RoutedEventArgs e)
    {
        if (_selectedWorkflowVersion is null)
        {
            return;
        }

        WorkflowVersionCompareATextBox.Text = _selectedWorkflowVersion.VersionId;
        ApplyWorkflowVersionActionState();
    }

    private void OnUseWorkflowVersionAsBClick(object sender, RoutedEventArgs e)
    {
        if (_selectedWorkflowVersion is null)
        {
            return;
        }

        WorkflowVersionCompareBTextBox.Text = _selectedWorkflowVersion.VersionId;
        ApplyWorkflowVersionActionState();
    }

    private async void OnCompareWorkflowVersionsClick(object sender, RoutedEventArgs e)
    {
        var versionA = WorkflowVersionCompareATextBox.Text.Trim();
        var versionB = WorkflowVersionCompareBTextBox.Text.Trim();
        if (string.IsNullOrWhiteSpace(versionA) || string.IsNullOrWhiteSpace(versionB))
        {
            ApplyWorkflowVersionCompareViewState(
                WorkflowVersionComparePresenter.CreateValidationFailure("Workflow version compare requires version A and B."));
            SetGovernanceStatus("Workflow version compare requires version A and B.", isError: true);
            return;
        }
        try
        {
            var baseUrl = await EnsureGovernanceBoundaryLoadedAsync();
            var result = await _workflowVersionAdminCoordinator.CompareVersionsAsync(
                baseUrl,
                ApiKeyTextBox.Text.Trim(),
                versionA,
                versionB);
            WorkflowVersionCacheStatsTextBlock.Text = WorkflowVersionCachePresenter.BuildStatsText(result.Stats);
            var viewState = result.Result is not null && string.IsNullOrWhiteSpace(result.ErrorMessage)
                ? WorkflowVersionComparePresenter.CreateSuccess(result.Result)
                : WorkflowVersionComparePresenter.CreateFailure(result.ErrorMessage, result.ErrorCode, result.ErrorItems);
            ApplyWorkflowVersionCompareViewState(viewState);
            SetGovernanceStatus(viewState.StatusText, isError: !viewState.Ok);
        }
        catch (Exception ex)
        {
            var viewState = WorkflowVersionComparePresenter.CreateFailure(ex.Message);
            ApplyWorkflowVersionCompareViewState(viewState);
            SetGovernanceStatus(viewState.StatusText, isError: true);
        }
    }

    private async void OnRefreshWorkflowVersionCacheStatsClick(object sender, RoutedEventArgs e)
    {
        var stats = await _workflowVersionAdminCoordinator.GetCacheStatsAsync();
        WorkflowVersionCacheStatsTextBlock.Text = WorkflowVersionCachePresenter.BuildStatsText(stats);
        SetGovernanceStatus("Workflow version cache stats refreshed.", isError: false);
    }

    private async void OnClearWorkflowVersionCacheClick(object sender, RoutedEventArgs e)
    {
        var stats = await _workflowVersionAdminCoordinator.ClearCacheAsync();
        WorkflowVersionCacheStatsTextBlock.Text = WorkflowVersionCachePresenter.BuildStatsText(stats);
        ApplyWorkflowVersionCompareViewState(WorkflowVersionComparePresenter.CreateEmpty());
        SetGovernanceStatus(WorkflowVersionCachePresenter.BuildClearStatusText(true), isError: false);
    }

    private void ApplyWorkflowVersionCompareViewState(WorkflowVersionCompareViewState state)
    {
        WorkflowVersionCompareSummaryTextBlock.Text = string.IsNullOrWhiteSpace(state.SummaryText) ? "-" : state.SummaryText;
        WorkflowVersionCompareDetailsListView.Items.Clear();
        foreach (var item in state.DetailRows)
        {
            WorkflowVersionCompareDetailsListView.Items.Add(item);
        }
    }
}
