using AIWF.Native.Runtime;
using Microsoft.UI.Xaml;

namespace AIWF.Native;

public sealed partial class MainWindow
{
    private async void OnRefreshWorkflowDiagnosticsClick(object sender, RoutedEventArgs e)
    {
        await RefreshWorkflowDiagnosticsAsync(showStatus: true);
    }

    private async Task RefreshWorkflowDiagnosticsAsync(bool showStatus)
    {
        try
        {
            var baseUrl = GetBridgeBaseUrlOrThrow();
            _sqlConnectionProfile = CollectSqlConnectionProfileFromControls();
            var runId = ResolveWorkflowDiagnosticsRunId();
            var result = await _workflowDiagnosticsCoordinator.RefreshAsync(
                baseUrl,
                ApiKeyTextBox.Text.Trim(),
                _sqlConnectionProfile.ResolveAccelUrl(BridgeUrlTextBox.Text),
                runId);
            ApplyWorkflowDiagnosticsState(result);
            if (showStatus)
            {
                SetInlineStatus(result.StatusText, InlineStatusTone.Success);
            }
        }
        catch (Exception ex)
        {
            ClearWorkflowDiagnosticsState();
            WorkflowDiagnosticsSummaryTextBlock.Text = $"Workflow diagnostics unavailable: {ex.Message}";
            if (showStatus)
            {
                SetInlineStatus($"Workflow diagnostics unavailable: {ex.Message}", InlineStatusTone.Error);
            }
        }
    }

    private void ApplyWorkflowDiagnosticsState(WorkflowDiagnosticsRefreshResult state)
    {
        WorkflowDiagnosticsSummaryTextBlock.Text = string.IsNullOrWhiteSpace(state.SummaryText) ? "-" : state.SummaryText;

        WorkflowCurrentRunDiagnosticsListView.Items.Clear();
        if (state.CurrentRunTimeline.Count == 0)
        {
            WorkflowCurrentRunDiagnosticsListView.Items.Add("No current run timeline.");
        }
        else
        {
            foreach (var row in state.CurrentRunTimeline)
            {
                WorkflowCurrentRunDiagnosticsListView.Items.Add(row);
            }
        }

        WorkflowDiagnosticsListView.Items.Clear();
        if (state.AggregateItems.Count == 0)
        {
            WorkflowDiagnosticsListView.Items.Add("No diagnostics yet.");
        }
        else
        {
            foreach (var row in state.AggregateItems)
            {
                WorkflowDiagnosticsListView.Items.Add(row);
            }
        }
    }

    private void ClearWorkflowDiagnosticsState()
    {
        WorkflowCurrentRunDiagnosticsListView.Items.Clear();
        WorkflowDiagnosticsListView.Items.Clear();
    }

    private string ResolveWorkflowDiagnosticsRunId()
    {
        var explicitRunId = (WorkflowDiagnosticsRunIdTextBox.Text ?? string.Empty).Trim();
        if (!string.IsNullOrWhiteSpace(explicitRunId))
        {
            return explicitRunId;
        }

        var resultRunId = (JobIdTextBlock.Text ?? string.Empty).Trim();
        return string.Equals(resultRunId, "-", StringComparison.Ordinal) ? string.Empty : resultRunId;
    }

    private void SyncWorkflowDiagnosticsRunIdFromResultState(ResultPanelState state)
    {
        var runId = (state.JobIdText ?? string.Empty).Trim();
        if (!string.IsNullOrWhiteSpace(runId) && !string.Equals(runId, "-", StringComparison.Ordinal))
        {
            WorkflowDiagnosticsRunIdTextBox.Text = runId;
        }
    }
}
