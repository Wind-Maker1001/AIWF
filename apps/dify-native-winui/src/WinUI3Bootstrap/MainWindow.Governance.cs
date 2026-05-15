using AIWF.Native.Runtime;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using Microsoft.UI.Xaml;
using Microsoft.UI.Xaml.Controls;
using Microsoft.UI.Xaml.Media;

namespace AIWF.Native;

public sealed partial class MainWindow
{
    private GovernanceReviewListEntry? _selectedGovernanceReview;
    private GovernanceQualityRuleSetItem? _selectedQualityRuleSet;
    private GovernanceSandboxRuleVersionItem? _selectedSandboxRuleVersion;
    private GovernanceControlPlaneBoundary? _currentGovernanceBoundary;
    private string _currentGovernanceBoundaryBaseUrl = string.Empty;
    private GovernanceSandboxAutoFixState _currentSandboxAutoFixState =
        new(string.Empty, string.Empty, 0, new JsonArray(), new JsonArray());

    private async Task RefreshGovernanceAsync()
    {
        await RefreshGovernanceBoundaryAsync();
        await RefreshPendingReviewsAsync();
        await RefreshReviewHistoryAsync();
        await RefreshGovernanceAuditAsync();
        await RefreshQualityRuleSetsAsync();
        await RefreshSandboxGovernanceAsync();
    }

    private async void OnRefreshPendingReviewsClick(object sender, RoutedEventArgs e)
    {
        await RefreshPendingReviewsAsync();
    }

    private async void OnRefreshReviewHistoryClick(object sender, RoutedEventArgs e)
    {
        await RefreshReviewHistoryAsync();
    }

    private async void OnRefreshGovernanceAuditClick(object sender, RoutedEventArgs e)
    {
        await RefreshGovernanceAuditAsync();
    }

    private async void OnRefreshQualityRuleSetsClick(object sender, RoutedEventArgs e)
    {
        await RefreshQualityRuleSetsAsync();
    }

    private async void OnSaveQualityRuleSetClick(object sender, RoutedEventArgs e)
    {
        await SaveQualityRuleSetAsync();
    }

    private async void OnDeleteQualityRuleSetClick(object sender, RoutedEventArgs e)
    {
        await DeleteQualityRuleSetAsync();
    }

    private async void OnRefreshSandboxGovernanceClick(object sender, RoutedEventArgs e)
    {
        await RefreshSandboxGovernanceAsync();
    }

    private async void OnSaveSandboxRulesClick(object sender, RoutedEventArgs e)
    {
        await SaveSandboxRulesAsync();
    }

    private async void OnRollbackSandboxRuleVersionClick(object sender, RoutedEventArgs e)
    {
        await RollbackSandboxRuleVersionAsync();
    }

    private async void OnApplySandboxMuteClick(object sender, RoutedEventArgs e)
    {
        await ApplySandboxMuteAsync();
    }

    private async void OnSaveSandboxAutoFixStateClick(object sender, RoutedEventArgs e)
    {
        await SaveSandboxAutoFixStateAsync();
    }

    private async void OnClearSandboxAutoFixOverrideClick(object sender, RoutedEventArgs e)
    {
        await ClearSandboxAutoFixOverrideAsync();
    }

    private async void OnApproveReviewClick(object sender, RoutedEventArgs e)
    {
        await SubmitGovernanceReviewDecisionAsync(approved: true);
    }

    private async void OnRejectReviewClick(object sender, RoutedEventArgs e)
    {
        await SubmitGovernanceReviewDecisionAsync(approved: false);
    }

    private void OnPendingReviewSelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        _selectedGovernanceReview = PendingReviewsListView.SelectedItem as GovernanceReviewListEntry;
        ApplyGovernanceSelection();
    }

    private void OnQualityRuleSetSelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        _selectedQualityRuleSet = QualityRuleSetListView.SelectedItem as GovernanceQualityRuleSetItem;
        ApplyQualityRuleSetSelection();
    }

    private void OnSandboxRuleVersionSelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        _selectedSandboxRuleVersion = SandboxRuleVersionsListView.SelectedItem as GovernanceSandboxRuleVersionItem;
        if (_selectedSandboxRuleVersion is null)
        {
            return;
        }

        SandboxRulesJsonTextBox.Text = _selectedSandboxRuleVersion.RulesJson;
    }

    private async void OnGovernanceRunSelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (GovernanceRecentRunsListView.SelectedItem is GovernanceWorkflowRunItem item)
        {
            GovernanceTimelineRunIdTextBox.Text = item.RunId;
            await RefreshGovernanceAuditAsync();
        }
    }

    private async Task RefreshPendingReviewsAsync()
    {
        try
        {
            var baseUrl = await EnsureGovernanceBoundaryLoadedAsync();
            var state = await _governanceManualReviewCoordinator.RefreshPendingAsync(baseUrl, ApiKeyTextBox.Text.Trim());
            PendingReviewsListView.Items.Clear();
            foreach (var item in state.Items)
            {
                PendingReviewsListView.Items.Add(new GovernanceReviewListEntry
                {
                    Item = item,
                    IsHistory = false,
                });
            }

            GovernancePendingHintTextBlock.Text = state.HintText;
            if (state.Items.Count == 0)
            {
                _selectedGovernanceReview = null;
                PendingReviewsListView.SelectedItem = null;
                ApplyGovernanceSelection();
            }
            SetGovernanceStatus("Pending review queue refreshed.", isError: false);
        }
        catch (Exception ex)
        {
            PendingReviewsListView.Items.Clear();
            GovernancePendingHintTextBlock.Text = "Pending review queue unavailable.";
            _selectedGovernanceReview = null;
            ApplyGovernanceSelection();
            SetGovernanceStatus($"Refresh pending review queue failed: {ex.Message}", isError: true);
        }
    }

    private async Task RefreshReviewHistoryAsync()
    {
        try
        {
            var baseUrl = await EnsureGovernanceBoundaryLoadedAsync();
            var items = await _governanceManualReviewCoordinator.RefreshHistoryAsync(
                baseUrl,
                ApiKeyTextBox.Text.Trim(),
                GovernanceHistoryRunIdTextBox.Text,
                GovernanceHistoryReviewerTextBox.Text,
                ReadComboValue(GovernanceHistoryStatusComboBox));

            ReviewHistoryListView.Items.Clear();
            foreach (var item in items)
            {
                ReviewHistoryListView.Items.Add(new GovernanceReviewListEntry
                {
                    Item = item,
                    IsHistory = true,
                });
            }
        }
        catch (Exception ex)
        {
            ReviewHistoryListView.Items.Clear();
            SetGovernanceStatus($"Refresh review history failed: {ex.Message}", isError: true);
        }
    }

    private async Task RefreshGovernanceAuditAsync()
    {
        try
        {
            var baseUrl = GetBridgeBaseUrlOrThrow();
            var apiKey = ApiKeyTextBox.Text.Trim();

            var auditState = await _governanceAuditCoordinator.RefreshAsync(
                baseUrl,
                apiKey,
                GovernanceTimelineRunIdTextBox.Text.Trim(),
                GovernanceAuditActionTextBox.Text.Trim());
            GovernanceRecentRunsListView.Items.Clear();
            foreach (var item in auditState.Runs)
            {
                GovernanceRecentRunsListView.Items.Add(item);
            }

            GovernanceTimelineListView.Items.Clear();
            foreach (var item in auditState.Timeline)
            {
                GovernanceTimelineListView.Items.Add(item);
            }

            GovernanceFailureSummaryListView.Items.Clear();
            foreach (var item in auditState.Failures.Take(20))
            {
                GovernanceFailureSummaryListView.Items.Add(item);
            }
            GovernanceAuditLogListView.Items.Clear();
            foreach (var item in auditState.AuditEvents)
            {
                GovernanceAuditLogListView.Items.Add(item);
            }
            GovernanceAuditSummaryTextBlock.Text = auditState.SummaryText;
            SetGovernanceStatus("治理查询已刷新。", isError: false);
        }
        catch (Exception ex)
        {
            GovernanceRecentRunsListView.Items.Clear();
            GovernanceTimelineListView.Items.Clear();
            GovernanceFailureSummaryListView.Items.Clear();
            GovernanceAuditLogListView.Items.Clear();
            GovernanceAuditSummaryTextBlock.Text = "-";
            SetGovernanceStatus($"刷新治理查询失败：{ex.Message}", isError: true);
        }
    }

    private async Task RefreshQualityRuleSetsAsync()
    {
        try
        {
            var baseUrl = await EnsureGovernanceBoundaryLoadedAsync();
            var state = await _governanceQualityRuleSetCoordinator.RefreshAsync(
                baseUrl,
                ApiKeyTextBox.Text.Trim(),
                _selectedQualityRuleSet?.Id);
            QualityRuleSetListView.Items.Clear();
            foreach (var item in state.Items)
            {
                QualityRuleSetListView.Items.Add(item);
            }

            _selectedQualityRuleSet = state.SelectedItem;
            QualityRuleSetListView.SelectedItem = state.SelectedItem;
            ApplyQualityRuleSetSelection();
        }
        catch (Exception ex)
        {
            QualityRuleSetListView.Items.Clear();
            _selectedQualityRuleSet = null;
            ApplyQualityRuleSetSelection();
            SetGovernanceStatus($"Refresh quality rule sets failed: {ex.Message}", isError: true);
        }
    }

    private async Task SaveQualityRuleSetAsync()
    {
        try
        {
            var baseUrl = await EnsureGovernanceBoundaryLoadedAsync();
            var id = QualityRuleSetIdTextBox.Text.Trim();
            if (string.IsNullOrWhiteSpace(id))
            {
                SetGovernanceStatus("Quality rule set id is required.", isError: true);
                return;
            }

            var rules = ParseJsonObjectOrThrow(QualityRuleSetJsonTextBox.Text);
            var item = await _governanceQualityRuleSetCoordinator.SaveAsync(
                baseUrl,
                ApiKeyTextBox.Text.Trim(),
                id,
                QualityRuleSetNameTextBox.Text,
                QualityRuleSetVersionTextBox.Text,
                rules);
            SetGovernanceStatus($"Quality rule set saved: {item.Id}", isError: false);
            _selectedQualityRuleSet = item;
            await RefreshQualityRuleSetsAsync();
        }
        catch (Exception ex)
        {
            SetGovernanceStatus($"Save quality rule set failed: {ex.Message}", isError: true);
        }
    }

    private async Task DeleteQualityRuleSetAsync()
    {
        try
        {
            var baseUrl = await EnsureGovernanceBoundaryLoadedAsync();
            var id = string.IsNullOrWhiteSpace(QualityRuleSetIdTextBox.Text)
                ? _selectedQualityRuleSet?.Id ?? string.Empty
                : QualityRuleSetIdTextBox.Text.Trim();
            if (string.IsNullOrWhiteSpace(id))
            {
                SetGovernanceStatus("Select or enter a quality rule set id first.", isError: true);
                return;
            }

            await _governanceQualityRuleSetCoordinator.DeleteAsync(baseUrl, ApiKeyTextBox.Text.Trim(), id);
            SetGovernanceStatus($"Quality rule set deleted: {id}", isError: false);
            QualityRuleSetIdTextBox.Text = string.Empty;
            QualityRuleSetNameTextBox.Text = string.Empty;
            QualityRuleSetVersionTextBox.Text = "v1";
            QualityRuleSetJsonTextBox.Text = string.Empty;
            _selectedQualityRuleSet = null;
            await RefreshQualityRuleSetsAsync();
        }
        catch (Exception ex)
        {
            SetGovernanceStatus($"Delete quality rule set failed: {ex.Message}", isError: true);
        }
    }

    private void ApplyQualityRuleSetSelection()
    {
        if (_selectedQualityRuleSet is null)
        {
            QualityRuleSetIdTextBox.Text = string.Empty;
            QualityRuleSetNameTextBox.Text = string.Empty;
            QualityRuleSetVersionTextBox.Text = "v1";
            QualityRuleSetJsonTextBox.Text = string.Empty;
            return;
        }

        QualityRuleSetIdTextBox.Text = _selectedQualityRuleSet.Id;
        QualityRuleSetNameTextBox.Text = _selectedQualityRuleSet.Name;
        QualityRuleSetVersionTextBox.Text = string.IsNullOrWhiteSpace(_selectedQualityRuleSet.Version) ? "v1" : _selectedQualityRuleSet.Version;
        QualityRuleSetJsonTextBox.Text = _selectedQualityRuleSet.RulesJson;
    }

    private async Task RefreshSandboxGovernanceAsync()
    {
        try
        {
            var baseUrl = await EnsureGovernanceBoundaryLoadedAsync();
            var apiKey = ApiKeyTextBox.Text.Trim();
            var state = await _governanceSandboxCoordinator.RefreshAsync(baseUrl, apiKey);
            SandboxRulesJsonTextBox.Text = PrettyGovernanceJson(state.Rules.ToJsonString());

            SandboxRuleVersionsListView.Items.Clear();
            foreach (var item in state.Versions)
            {
                SandboxRuleVersionsListView.Items.Add(item);
            }

            var autoFixState = state.AutoFixState;
            _currentSandboxAutoFixState = autoFixState;
            SandboxAutoFixStateTextBlock.Text = autoFixState.DisplayText;
            SandboxAutoFixModeTextBox.Text = autoFixState.ForcedIsolationMode;
            SandboxAutoFixUntilTextBox.Text = autoFixState.ForcedUntil;
            SandboxAutoFixGreenStreakTextBox.Text = autoFixState.GreenStreak.ToString();

            SandboxAutoFixActionsListView.Items.Clear();
            foreach (var item in state.AutoFixActions)
            {
                SandboxAutoFixActionsListView.Items.Add(item);
            }
        }
        catch (Exception ex)
        {
            SandboxRuleVersionsListView.Items.Clear();
            SandboxAutoFixActionsListView.Items.Clear();
            SandboxAutoFixStateTextBlock.Text = "-";
            SandboxAutoFixModeTextBox.Text = string.Empty;
            SandboxAutoFixUntilTextBox.Text = string.Empty;
            SandboxAutoFixGreenStreakTextBox.Text = "0";
            _currentSandboxAutoFixState = new GovernanceSandboxAutoFixState(string.Empty, string.Empty, 0, new JsonArray(), new JsonArray());
            SetGovernanceStatus($"Refresh sandbox governance failed: {ex.Message}", isError: true);
        }
    }

    private async Task SaveSandboxRulesAsync()
    {
        try
        {
            var baseUrl = await EnsureGovernanceBoundaryLoadedAsync();
            var rules = ParseJsonObjectOrThrow(SandboxRulesJsonTextBox.Text);
            var versionId = await _governanceSandboxMutationCoordinator.SaveRulesAsync(
                baseUrl,
                ApiKeyTextBox.Text.Trim(),
                rules);
            SetGovernanceStatus($"Sandbox rules saved: {versionId}", isError: false);
            await RefreshSandboxGovernanceAsync();
        }
        catch (Exception ex)
        {
            SetGovernanceStatus($"Save sandbox rules failed: {ex.Message}", isError: true);
        }
    }

    private async Task RollbackSandboxRuleVersionAsync()
    {
        try
        {
            var versionId = _selectedSandboxRuleVersion?.VersionId ?? string.Empty;
            if (string.IsNullOrWhiteSpace(versionId))
            {
                SetGovernanceStatus("Select a sandbox rule version first.", isError: true);
                return;
            }

            var baseUrl = await EnsureGovernanceBoundaryLoadedAsync();
            var newVersion = await _governanceSandboxMutationCoordinator.RollbackRulesAsync(
                baseUrl,
                ApiKeyTextBox.Text.Trim(),
                versionId);
            SetGovernanceStatus($"Sandbox rules rolled back into new version: {newVersion}", isError: false);
            await RefreshSandboxGovernanceAsync();
        }
        catch (Exception ex)
        {
            SetGovernanceStatus($"Rollback sandbox rules failed: {ex.Message}", isError: true);
        }
    }

    private async Task ApplySandboxMuteAsync()
    {
        try
        {
            var baseUrl = await EnsureGovernanceBoundaryLoadedAsync();
            var currentRules = ParseJsonObjectOrThrow(SandboxRulesJsonTextBox.Text);
            var muteResult = await _governanceSandboxMutationCoordinator.ApplyMuteAsync(
                baseUrl,
                ApiKeyTextBox.Text.Trim(),
                currentRules,
                SandboxMuteNodeTypeTextBox.Text,
                SandboxMuteNodeIdTextBox.Text,
                SandboxMuteCodeTextBox.Text,
                SandboxMuteMinutesTextBox.Text,
                DateTimeOffset.UtcNow);

            SandboxRulesJsonTextBox.Text = PrettyGovernanceJson(muteResult.Rules.ToJsonString());
            SetGovernanceStatus($"Sandbox mute applied: {muteResult.VersionId}", isError: false);
            await RefreshSandboxGovernanceAsync();
        }
        catch (Exception ex)
        {
            SetGovernanceStatus($"Apply sandbox mute failed: {ex.Message}", isError: true);
        }
    }

    private async Task SaveSandboxAutoFixStateAsync()
    {
        try
        {
            var baseUrl = await EnsureGovernanceBoundaryLoadedAsync();
            _currentSandboxAutoFixState = await _governanceSandboxMutationCoordinator.SaveAutoFixStateAsync(
                baseUrl,
                ApiKeyTextBox.Text.Trim(),
                _currentSandboxAutoFixState,
                SandboxAutoFixModeTextBox.Text,
                SandboxAutoFixUntilTextBox.Text,
                SandboxAutoFixGreenStreakTextBox.Text);
            SetGovernanceStatus("Sandbox AutoFix override saved.", isError: false);
            await RefreshSandboxGovernanceAsync();
        }
        catch (Exception ex)
        {
            SetGovernanceStatus($"Save Sandbox AutoFix override failed: {ex.Message}", isError: true);
        }
    }

    private async Task ClearSandboxAutoFixOverrideAsync()
    {
        SandboxAutoFixModeTextBox.Text = string.Empty;
        SandboxAutoFixUntilTextBox.Text = string.Empty;
        await SaveSandboxAutoFixStateAsync();
    }

    private async Task SubmitGovernanceReviewDecisionAsync(bool approved)
    {
        if (_selectedGovernanceReview?.Item is null)
        {
            SetGovernanceStatus("Select a pending review item first.", isError: true);
            return;
        }

        try
        {
            var baseUrl = await EnsureGovernanceBoundaryLoadedAsync();
            var selected = _selectedGovernanceReview.Item;
            var item = await _governanceManualReviewCoordinator.SubmitDecisionAsync(
                baseUrl,
                ApiKeyTextBox.Text.Trim(),
                selected,
                approved,
                GovernanceReviewerTextBox.Text,
                GovernanceCommentTextBox.Text);

            SetGovernanceStatus(
                approved
                    ? $"Review approved: {item.RunId} / {item.ReviewKey}"
                    : $"Review rejected: {item.RunId} / {item.ReviewKey}",
                isError: false);
            GovernanceCommentTextBox.Text = string.Empty;
            _selectedGovernanceReview = null;
            PendingReviewsListView.SelectedItem = null;
            ApplyGovernanceSelection();
            await RefreshGovernanceAsync();
        }
        catch (Exception ex)
        {
            SetGovernanceStatus($"Submit review decision failed: {ex.Message}", isError: true);
        }
    }

    private void ApplyGovernanceSelection()
    {
        var selected = _selectedGovernanceReview?.Item;
        if (selected is null)
        {
            SelectedReviewSummaryTextBlock.Text = "未选中待审核项";
            SelectedReviewDetailTextBlock.Text = "-";
            ApproveReviewButton.IsEnabled = false;
            RejectReviewButton.IsEnabled = false;
            return;
        }

        SelectedReviewSummaryTextBlock.Text = $"{selected.RunId} / {selected.ReviewKey}";
        SelectedReviewDetailTextBlock.Text =
            $"workflow={selected.WorkflowId} | node={selected.NodeId} | status={selected.Status}";
        ApproveReviewButton.IsEnabled = true;
        RejectReviewButton.IsEnabled = true;
    }

    private void SetGovernanceStatus(string message, bool isError)
    {
        GovernanceStatusTextBlock.Text = string.IsNullOrWhiteSpace(message) ? "-" : message;
        GovernanceStatusTextBlock.Foreground = new SolidColorBrush(
            isError
                ? Windows.UI.Color.FromArgb(0xFF, 0xC6, 0x28, 0x28)
                : Windows.UI.Color.FromArgb(0xFF, 0x37, 0x47, 0x51));
        SetInlineStatus(message, isError ? InlineStatusTone.Error : InlineStatusTone.Success);
    }

    private async Task RefreshGovernanceBoundaryAsync()
    {
        try
        {
            var baseUrl = GetBridgeBaseUrlOrThrow();
            _currentGovernanceBoundary = await _governanceClient.GetGovernanceControlPlaneBoundaryAsync(baseUrl, ApiKeyTextBox.Text.Trim());
            _currentGovernanceBoundaryBaseUrl = baseUrl;
            ApplyGovernanceBoundarySummary(_currentGovernanceBoundary);
        }
        catch (Exception ex)
        {
            _currentGovernanceBoundary = null;
            _currentGovernanceBoundaryBaseUrl = string.Empty;
            GovernanceBoundaryHintTextBlock.Text = "治理控制面边界不可用。";
            GovernanceBoundaryRoutesTextBlock.Text = ex.Message;
            throw;
        }
    }

    private async Task<string> EnsureGovernanceBoundaryLoadedAsync()
    {
        var baseUrl = GetBridgeBaseUrlOrThrow();
        if (_currentGovernanceBoundary is null || !string.Equals(_currentGovernanceBoundaryBaseUrl, baseUrl, StringComparison.OrdinalIgnoreCase))
        {
            _currentGovernanceBoundary = await _governanceClient.GetGovernanceControlPlaneBoundaryAsync(baseUrl, ApiKeyTextBox.Text.Trim());
            _currentGovernanceBoundaryBaseUrl = baseUrl;
        }
        ApplyGovernanceBoundarySummary(_currentGovernanceBoundary);
        return baseUrl;
    }

    private void ApplyGovernanceBoundarySummary(GovernanceControlPlaneBoundary? boundary)
    {
        if (boundary is null)
        {
            GovernanceBoundaryHintTextBlock.Text = "治理控制面边界不可用。";
            GovernanceBoundaryRoutesTextBlock.Text = "-";
            return;
        }

        GovernanceBoundaryHintTextBlock.Text =
            $"治理状态由 {boundary.GovernanceStateControlPlaneOwner} 承载；job lifecycle 仍由 {boundary.JobLifecycleControlPlaneOwner} 负责。";
        GovernanceBoundaryRoutesTextBlock.Text =
            $"role={boundary.ControlPlaneRole} | surfaces={boundary.GovernanceSurfaces.Count} | meta={boundary.MetaRoute}";
    }

    private static JsonObject ParseJsonObjectOrThrow(string raw)
    {
        var text = string.IsNullOrWhiteSpace(raw) ? "{}" : raw;
        var node = JsonNode.Parse(text);
        if (node is JsonObject obj)
        {
            return obj;
        }

        throw new InvalidOperationException("需要 JSON 对象。");
    }

    private static string PrettyGovernanceJson(string raw)
    {
        try
        {
            var node = JsonNode.Parse(raw);
            return node?.ToJsonString(new JsonSerializerOptions { WriteIndented = true }) ?? raw;
        }
        catch
        {
            return raw;
        }
    }
}
