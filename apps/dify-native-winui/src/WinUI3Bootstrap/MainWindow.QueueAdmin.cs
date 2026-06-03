using System.Text.Json;
using System.Text.Json.Nodes;
using AIWF.Native.Runtime;
using Microsoft.UI.Xaml;
using Microsoft.UI.Xaml.Controls;

namespace AIWF.Native;

public sealed partial class MainWindow
{
    private WorkflowQueueTaskItem? _selectedQueueTask;
    private GovernanceWorkflowRunRecordDetail? _selectedQueueRunHistory;
    private WorkflowQueueAdminRefreshResult? _currentQueueAdminState;

    private Task RefreshQueueAdminAsync()
    {
        try
        {
            var state = _workflowQueueAdminCoordinator.Refresh();
            _currentQueueAdminState = state;
            BindQueueAdminState(state);
        }
        catch (Exception ex)
        {
            _currentQueueAdminState = null;
            _selectedQueueTask = null;
            _selectedQueueRunHistory = null;
            WorkflowQueueListView.Items.Clear();
            WorkflowRunHistoryListView.Items.Clear();
            WorkflowQueueSummaryTextBlock.Text = "-";
            WorkflowRunHistorySummaryTextBlock.Text = "-";
            WorkflowQueueTaskDetailTextBox.Text = string.Empty;
            WorkflowRunHistoryDetailTextBox.Text = string.Empty;
            ApplyQueueAdminSelectionState();
            SetGovernanceStatus($"Refresh queue admin failed: {ex.Message}", isError: true);
        }

        return Task.CompletedTask;
    }

    private void BindQueueAdminState(WorkflowQueueAdminRefreshResult state)
    {
        var selectedTaskId = _selectedQueueTask?.TaskId;
        var selectedRunId = _selectedQueueRunHistory?.RunId;

        WorkflowQueueListView.Items.Clear();
        _selectedQueueTask = null;
        foreach (var item in state.QueueItems)
        {
            WorkflowQueueListView.Items.Add(item);
            if (!string.IsNullOrWhiteSpace(selectedTaskId)
                && string.Equals(item.TaskId, selectedTaskId, StringComparison.Ordinal))
            {
                _selectedQueueTask = item;
            }
        }

        WorkflowQueueListView.SelectedItem = _selectedQueueTask;

        WorkflowRunHistoryListView.Items.Clear();
        _selectedQueueRunHistory = null;
        foreach (var item in state.RunHistory)
        {
            WorkflowRunHistoryListView.Items.Add(item);
            if (!string.IsNullOrWhiteSpace(selectedRunId)
                && string.Equals(item.RunId, selectedRunId, StringComparison.Ordinal))
            {
                _selectedQueueRunHistory = item;
            }
        }

        WorkflowRunHistoryListView.SelectedItem = _selectedQueueRunHistory;
        WorkflowQueueSummaryTextBlock.Text = BuildQueueSummaryText(state);
        ApplyQueueAdminSelectionState();
    }

    private void ApplyQueueAdminSelectionState()
    {
        var control = _currentQueueAdminState?.Control ?? new WorkflowQueueControlState(false, new Dictionary<string, int>());
        PauseWorkflowQueueButton.IsEnabled = !control.Paused;
        ResumeWorkflowQueueButton.IsEnabled = control.Paused;

        var queueStatus = (_selectedQueueTask?.Status ?? string.Empty).Trim().ToLowerInvariant();
        CancelWorkflowQueueTaskButton.IsEnabled = _selectedQueueTask is not null && string.Equals(queueStatus, "queued", StringComparison.Ordinal);
        RetryWorkflowQueueTaskButton.IsEnabled = _selectedQueueTask is not null
            && !string.Equals(queueStatus, "queued", StringComparison.Ordinal)
            && !string.Equals(queueStatus, "running", StringComparison.Ordinal);
        WorkflowQueueTaskDetailTextBox.Text = _selectedQueueTask is null
            ? string.Empty
            : SerializeQueueTask(_selectedQueueTask);

        ReplayWorkflowRunButton.IsEnabled = _selectedQueueRunHistory is not null;
        ReplayWorkflowRunButton.Content = ResolveReplayActionText(_selectedQueueRunHistory);
        RetryFailedWorkflowRunButton.IsEnabled = CanRetryFailedStep(_selectedQueueRunHistory);
        UseQueueRunAsAButton.IsEnabled = _selectedQueueRunHistory is not null;
        UseQueueRunAsBButton.IsEnabled = _selectedQueueRunHistory is not null;
        WorkflowRunHistorySummaryTextBlock.Text = BuildRunHistorySummaryText(_currentQueueAdminState, _selectedQueueRunHistory);
        WorkflowRunHistoryDetailTextBox.Text = _selectedQueueRunHistory is null
            ? string.Empty
            : SerializeRunHistory(_selectedQueueRunHistory);
    }

    private async void OnRefreshQueueAdminClick(object sender, RoutedEventArgs e)
    {
        await RefreshQueueAdminAsync();
    }

    private async void OnQueueCurrentFlowClick(object sender, RoutedEventArgs e)
    {
        if (!ValidateRunInputs(out var validationMessage))
        {
            SetGovernanceStatus(validationMessage, isError: true);
            return;
        }

        try
        {
            var input = CollectRunRequestInput();
            var state = await _workflowQueueAdminCoordinator.EnqueueLegacyFlowAsync(
                input.BaseUrl,
                input.ApiKey,
                input.Owner,
                input.JobId,
                input.Flow,
                input.Payload,
                string.IsNullOrWhiteSpace(ReportTitleTextBox.Text) ? input.Flow : ReportTitleTextBox.Text.Trim());
            _currentQueueAdminState = state;
            BindQueueAdminState(state);
            SetGovernanceStatus(
                state.Control.Paused
                    ? "Workflow task queued while the queue is paused."
                    : "Workflow task queued.",
                isError: false);
        }
        catch (Exception ex)
        {
            SetGovernanceStatus($"Queue current flow failed: {ex.Message}", isError: true);
        }
    }

    private async void OnQueueCurrentCanvasClick(object sender, RoutedEventArgs e)
    {
        var document = BuildWorkflowGraphDocumentFromCanvas();
        if (document is null || document.Nodes.Count == 0)
        {
            SetGovernanceStatus("Current canvas does not contain a runnable workflow.", isError: true);
            return;
        }

        try
        {
            var accelUrl = _sqlConnectionProfile.ResolveAccelUrl(BridgeUrlTextBox.Text);
            var state = await _workflowQueueAdminCoordinator.EnqueueCanvasDraftAsync(
                accelUrl,
                ApiKeyTextBox.Text.Trim(),
                string.IsNullOrWhiteSpace(JobIdTextBox.Text) ? document.WorkflowId : JobIdTextBox.Text.Trim(),
                document,
                document.WorkflowId);
            _currentQueueAdminState = state;
            BindQueueAdminState(state);
            SetGovernanceStatus(
                state.Control.Paused
                    ? "Canvas workflow queued while the queue is paused."
                    : "Canvas workflow queued.",
                isError: false);
        }
        catch (Exception ex)
        {
            SetGovernanceStatus($"Queue current canvas failed: {ex.Message}", isError: true);
        }
    }

    private void OnWorkflowQueueSelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        _selectedQueueTask = WorkflowQueueListView.SelectedItem as WorkflowQueueTaskItem;
        ApplyQueueAdminSelectionState();
    }

    private async void OnPauseWorkflowQueueClick(object sender, RoutedEventArgs e)
    {
        try
        {
            var state = _workflowQueueAdminCoordinator.SetPaused(true);
            _currentQueueAdminState = state;
            BindQueueAdminState(state);
            SetGovernanceStatus("Workflow queue paused.", isError: false);
        }
        catch (Exception ex)
        {
            SetGovernanceStatus($"Pause queue failed: {ex.Message}", isError: true);
        }
    }

    private async void OnResumeWorkflowQueueClick(object sender, RoutedEventArgs e)
    {
        try
        {
            var state = await _workflowQueueAdminCoordinator.ResumeAndProcessAsync(ApiKeyTextBox.Text.Trim());
            _currentQueueAdminState = state;
            BindQueueAdminState(state);
            SetGovernanceStatus("Workflow queue resumed.", isError: false);
        }
        catch (Exception ex)
        {
            SetGovernanceStatus($"Resume queue failed: {ex.Message}", isError: true);
        }
    }

    private void OnCancelWorkflowQueueTaskClick(object sender, RoutedEventArgs e)
    {
        if (_selectedQueueTask is null)
        {
            SetGovernanceStatus("Select a queued task first.", isError: true);
            return;
        }

        try
        {
            var state = _workflowQueueAdminCoordinator.CancelTask(_selectedQueueTask.TaskId);
            _currentQueueAdminState = state;
            BindQueueAdminState(state);
            SetGovernanceStatus($"Queue task canceled: {_selectedQueueTask.TaskId}", isError: false);
        }
        catch (Exception ex)
        {
            SetGovernanceStatus($"Cancel queue task failed: {ex.Message}", isError: true);
        }
    }

    private async void OnRetryWorkflowQueueTaskClick(object sender, RoutedEventArgs e)
    {
        if (_selectedQueueTask is null)
        {
            SetGovernanceStatus("Select a queue task first.", isError: true);
            return;
        }

        try
        {
            var state = await _workflowQueueAdminCoordinator.RetryTaskAsync(_selectedQueueTask.TaskId, ApiKeyTextBox.Text.Trim());
            _currentQueueAdminState = state;
            BindQueueAdminState(state);
            SetGovernanceStatus($"Queue task retried: {_selectedQueueTask.TaskId}", isError: false);
        }
        catch (Exception ex)
        {
            SetGovernanceStatus($"Retry queue task failed: {ex.Message}", isError: true);
        }
    }

    private void OnWorkflowRunHistorySelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        _selectedQueueRunHistory = WorkflowRunHistoryListView.SelectedItem as GovernanceWorkflowRunRecordDetail;
        ApplyQueueAdminSelectionState();
    }

    private async void OnReplayWorkflowRunClick(object sender, RoutedEventArgs e)
    {
        if (_selectedQueueRunHistory is null)
        {
            SetGovernanceStatus("Select a run history item first.", isError: true);
            return;
        }

        try
        {
            var status = (_selectedQueueRunHistory.Status ?? string.Empty).Trim().ToLowerInvariant();
            if (string.Equals(status, "pending_review", StringComparison.Ordinal))
            {
                await RefreshPendingReviewsAsync();
                TrySelectPendingReviewForRun(_selectedQueueRunHistory.RunId);
                SetGovernanceStatus("Pending review queue refreshed for the selected run.", isError: false);
                return;
            }

            if (string.Equals(status, "quality_blocked", StringComparison.Ordinal))
            {
                GovernanceTimelineRunIdTextBox.Text = _selectedQueueRunHistory.RunId;
                await RefreshGovernanceAuditAsync();
                SetGovernanceStatus("Governance audit loaded for the quality-blocked run.", isError: false);
                return;
            }

            await ReplaySelectedRunAsync(_selectedQueueRunHistory, null);
        }
        catch (Exception ex)
        {
            SetGovernanceStatus($"Replay run failed: {ex.Message}", isError: true);
        }
    }

    private async void OnRetryFailedWorkflowRunClick(object sender, RoutedEventArgs e)
    {
        if (_selectedQueueRunHistory is null)
        {
            SetGovernanceStatus("Select a run history item first.", isError: true);
            return;
        }

        var failedStepId = _selectedQueueRunHistory.Steps
            .FirstOrDefault(step => string.Equals(step.Status, "failed", StringComparison.OrdinalIgnoreCase))
            ?.StepId;
        if (string.IsNullOrWhiteSpace(failedStepId))
        {
            SetGovernanceStatus("The selected run has no failed step to retry.", isError: true);
            return;
        }

        try
        {
            await ReplaySelectedRunAsync(_selectedQueueRunHistory, failedStepId);
        }
        catch (Exception ex)
        {
            SetGovernanceStatus($"Retry failed step failed: {ex.Message}", isError: true);
        }
    }

    private void OnUseQueueRunAsAClick(object sender, RoutedEventArgs e)
    {
        if (_selectedQueueRunHistory is null)
        {
            return;
        }

        RunBaselineRunATextBox.Text = _selectedQueueRunHistory.RunId;
        if (string.IsNullOrWhiteSpace(RunBaselineNameTextBox.Text))
        {
            RunBaselineNameTextBox.Text = $"baseline_{_selectedQueueRunHistory.RunId[..Math.Min(8, _selectedQueueRunHistory.RunId.Length)]}";
        }

        ApplyRunBaselineActionState();
    }

    private void OnUseQueueRunAsBClick(object sender, RoutedEventArgs e)
    {
        if (_selectedQueueRunHistory is null)
        {
            return;
        }

        RunBaselineRunBTextBox.Text = _selectedQueueRunHistory.RunId;
        ApplyRunBaselineActionState();
    }

    private async Task ReplaySelectedRunAsync(GovernanceWorkflowRunRecordDetail record, string? nodeId)
    {
        var baseUrl = GetBridgeBaseUrlOrThrow();
        var accelUrl = _sqlConnectionProfile.ResolveAccelUrl(BridgeUrlTextBox.Text);
        var result = await _workflowQueueAdminCoordinator.ReplayRunAsync(
            baseUrl,
            accelUrl,
            ApiKeyTextBox.Text.Trim(),
            record,
            nodeId);
        RawResponseTextBox.Text = PrettyJson(result.Response.Body);
        if (result.Ok && TryBindRunResult(result.Response.Body))
        {
            SetActiveSection(NavSection.Results);
        }

        await RefreshQueueAdminAsync();
        SetGovernanceStatus(result.StatusText, isError: !result.Ok);
    }

    private void TrySelectPendingReviewForRun(string runId)
    {
        var entry = PendingReviewsListView.Items
            .OfType<GovernanceReviewListEntry>()
            .FirstOrDefault(item => string.Equals(item.Item.RunId, runId, StringComparison.Ordinal));
        if (entry is null)
        {
            return;
        }

        PendingReviewsListView.SelectedItem = entry;
        _selectedGovernanceReview = entry;
        ApplyGovernanceSelection();
    }

    private static bool CanRetryFailedStep(GovernanceWorkflowRunRecordDetail? record)
    {
        if (record is null)
        {
            return false;
        }

        var status = (record.Status ?? string.Empty).Trim().ToLowerInvariant();
        if (string.Equals(status, "pending_review", StringComparison.Ordinal)
            || string.Equals(status, "quality_blocked", StringComparison.Ordinal))
        {
            return false;
        }

        return record.Steps.Any(step => string.Equals(step.Status, "failed", StringComparison.OrdinalIgnoreCase));
    }

    private static string ResolveReplayActionText(GovernanceWorkflowRunRecordDetail? record)
    {
        if (record is null)
        {
            return "Replay Run";
        }

        var status = (record.Status ?? string.Empty).Trim().ToLowerInvariant();
        if (string.Equals(status, "pending_review", StringComparison.Ordinal))
        {
            return "Open Review Queue";
        }

        return string.Equals(status, "quality_blocked", StringComparison.Ordinal)
            ? "Open Audit"
            : "Replay Run";
    }

    private static string BuildQueueSummaryText(WorkflowQueueAdminRefreshResult state)
    {
        var queued = state.QueueItems.Count(item => string.Equals(item.Status, "queued", StringComparison.Ordinal));
        var running = state.QueueItems.Count(item => string.Equals(item.Status, "running", StringComparison.Ordinal));
        var failed = state.QueueItems.Count(item => string.Equals(item.Status, "failed", StringComparison.Ordinal));
        var done = state.QueueItems.Count(item => string.Equals(item.Status, "done", StringComparison.Ordinal));
        return $"{state.Control.DisplayText} | tasks: {state.QueueItems.Count} | queued={queued} running={running} done={done} failed={failed}";
    }

    private static string BuildRunHistorySummaryText(WorkflowQueueAdminRefreshResult? state, GovernanceWorkflowRunRecordDetail? selected)
    {
        if (selected is not null)
        {
            return $"{selected.RunId} | {selected.Status} | {selected.RunRequestKind} | steps={selected.Steps.Count}";
        }

        return state is null
            ? "-"
            : $"Local runs: {state.RunHistory.Count}";
    }

    private static string SerializeQueueTask(WorkflowQueueTaskItem item)
    {
        var root = new JsonObject
        {
            ["task_id"] = item.TaskId,
            ["label"] = item.Label,
            ["dispatch_kind"] = item.DispatchKind,
            ["dispatch_base_url"] = item.DispatchBaseUrl,
            ["workflow_id"] = item.WorkflowId,
            ["owner"] = item.Owner,
            ["requested_job_id"] = item.RequestedJobId,
            ["flow"] = item.Flow,
            ["priority"] = item.Priority,
            ["status"] = item.Status,
            ["created_at"] = item.CreatedAt,
            ["started_at"] = item.StartedAt,
            ["finished_at"] = item.FinishedAt,
            ["run_id"] = item.RunId,
            ["error"] = item.Error,
            ["payload"] = CloneQueueAdminJsonObject(item.Payload),
            ["result"] = CloneQueueAdminJsonObject(item.ResultPayload),
        };
        return root.ToJsonString(new JsonSerializerOptions { WriteIndented = true });
    }

    private static string SerializeRunHistory(GovernanceWorkflowRunRecordDetail record)
    {
        var steps = new JsonArray(record.Steps.Select(step => (JsonNode)new JsonObject
        {
            ["step_id"] = step.StepId,
            ["status"] = step.Status,
            ["started_at"] = step.StartedAt,
            ["ended_at"] = step.EndedAt,
            ["seconds"] = step.Seconds,
            ["error"] = step.Error,
        }).ToArray());
        var root = new JsonObject
        {
            ["run_id"] = record.RunId,
            ["workflow_id"] = record.WorkflowId,
            ["status"] = record.Status,
            ["ok"] = record.Ok,
            ["timestamp"] = record.Timestamp,
            ["run_request_kind"] = record.RunRequestKind,
            ["version_id"] = record.VersionId,
            ["published_version_id"] = record.PublishedVersionId,
            ["workflow_definition_source"] = record.WorkflowDefinitionSource,
            ["payload"] = CloneQueueAdminJsonObject(record.Payload),
            ["steps"] = steps,
            ["result"] = CloneQueueAdminJsonObject(record.ResultPayload),
        };
        return root.ToJsonString(new JsonSerializerOptions { WriteIndented = true });
    }

    private static JsonObject CloneQueueAdminJsonObject(JsonObject? source)
    {
        return source is null
            ? new JsonObject()
            : JsonNode.Parse(source.ToJsonString()) as JsonObject ?? new JsonObject();
    }
}
