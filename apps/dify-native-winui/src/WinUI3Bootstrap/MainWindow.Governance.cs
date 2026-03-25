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
        if (_selectedQualityRuleSet is null)
        {
            return;
        }

        QualityRuleSetIdTextBox.Text = _selectedQualityRuleSet.Id;
        QualityRuleSetNameTextBox.Text = _selectedQualityRuleSet.Name;
        QualityRuleSetVersionTextBox.Text = string.IsNullOrWhiteSpace(_selectedQualityRuleSet.Version) ? "v1" : _selectedQualityRuleSet.Version;
        QualityRuleSetJsonTextBox.Text = _selectedQualityRuleSet.RulesJson;
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
            var items = await _governanceClient.ListManualReviewsAsync(baseUrl, ApiKeyTextBox.Text.Trim());
            PendingReviewsListView.Items.Clear();
            foreach (var item in items)
            {
                PendingReviewsListView.Items.Add(new GovernanceReviewListEntry
                {
                    Item = item,
                    IsHistory = false,
                });
            }

            GovernancePendingHintTextBlock.Text = items.Count == 0
                ? "当前没有待审核项。"
                : $"当前待审核项: {items.Count}";
            if (items.Count == 0)
            {
                _selectedGovernanceReview = null;
                PendingReviewsListView.SelectedItem = null;
                ApplyGovernanceSelection();
            }
            SetGovernanceStatus("待审核队列已刷新。", isError: false);
        }
        catch (Exception ex)
        {
            PendingReviewsListView.Items.Clear();
            GovernancePendingHintTextBlock.Text = "待审核队列不可用。";
            _selectedGovernanceReview = null;
            ApplyGovernanceSelection();
            SetGovernanceStatus($"刷新待审核队列失败：{ex.Message}", isError: true);
        }
    }

    private async Task RefreshReviewHistoryAsync()
    {
        try
        {
            var baseUrl = await EnsureGovernanceBoundaryLoadedAsync();
            var status = ReadComboValue(GovernanceHistoryStatusComboBox);
            var items = await _governanceClient.ListManualReviewHistoryAsync(
                baseUrl,
                ApiKeyTextBox.Text.Trim(),
                runId: GovernanceHistoryRunIdTextBox.Text.Trim(),
                reviewer: GovernanceHistoryReviewerTextBox.Text.Trim(),
                status: status);

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
            SetGovernanceStatus($"刷新审核历史失败：{ex.Message}", isError: true);
        }
    }

    private async Task RefreshGovernanceAuditAsync()
    {
        try
        {
            var baseUrl = await EnsureGovernanceBoundaryLoadedAsync();
            var apiKey = ApiKeyTextBox.Text.Trim();

            var runs = await _governanceClient.ListWorkflowRunsAsync(baseUrl, apiKey, limit: 40);
            GovernanceRecentRunsListView.Items.Clear();
            foreach (var item in runs)
            {
                GovernanceRecentRunsListView.Items.Add(item);
            }

            var runId = GovernanceTimelineRunIdTextBox.Text.Trim();
            if (string.IsNullOrWhiteSpace(runId))
            {
                GovernanceTimelineListView.Items.Clear();
            }
            else
            {
                var timeline = await _governanceClient.GetWorkflowRunTimelineAsync(baseUrl, apiKey, runId);
                GovernanceTimelineListView.Items.Clear();
                foreach (var item in timeline)
                {
                    GovernanceTimelineListView.Items.Add(item);
                }
            }

            var failures = await _governanceClient.GetWorkflowFailureSummaryAsync(baseUrl, apiKey, limit: 80);
            GovernanceFailureSummaryListView.Items.Clear();
            foreach (var item in failures.Take(20))
            {
                GovernanceFailureSummaryListView.Items.Add(item);
            }

            var audits = await _governanceClient.ListWorkflowAuditEventsAsync(
                baseUrl,
                apiKey,
                limit: 60,
                action: GovernanceAuditActionTextBox.Text.Trim());
            GovernanceAuditLogListView.Items.Clear();
            foreach (var item in audits)
            {
                GovernanceAuditLogListView.Items.Add(item);
            }

            GovernanceAuditSummaryTextBlock.Text =
                $"runs={runs.Count}, failure_types={failures.Count}, audit_events={audits.Count}";
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
            var items = await _governanceClient.ListQualityRuleSetsAsync(baseUrl, ApiKeyTextBox.Text.Trim(), 80);
            QualityRuleSetListView.Items.Clear();
            foreach (var item in items)
            {
                QualityRuleSetListView.Items.Add(item);
            }

            if (_selectedQualityRuleSet is null && items.Count > 0)
            {
                QualityRuleSetListView.SelectedItem = items[0];
            }
            else if (items.Count == 0)
            {
                _selectedQualityRuleSet = null;
            }
        }
        catch (Exception ex)
        {
            QualityRuleSetListView.Items.Clear();
            SetGovernanceStatus($"刷新质量规则集失败：{ex.Message}", isError: true);
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
                SetGovernanceStatus("请先填写质量规则集 ID。", isError: true);
                return;
            }

            var rules = ParseJsonObjectOrThrow(QualityRuleSetJsonTextBox.Text);
            var item = await _governanceClient.SaveQualityRuleSetAsync(
                baseUrl,
                ApiKeyTextBox.Text.Trim(),
                id,
                string.IsNullOrWhiteSpace(QualityRuleSetNameTextBox.Text) ? id : QualityRuleSetNameTextBox.Text.Trim(),
                string.IsNullOrWhiteSpace(QualityRuleSetVersionTextBox.Text) ? "v1" : QualityRuleSetVersionTextBox.Text.Trim(),
                "workflow",
                rules);
            SetGovernanceStatus($"已保存质量规则集：{item.Id}", isError: false);
            await RefreshQualityRuleSetsAsync();
        }
        catch (Exception ex)
        {
            SetGovernanceStatus($"保存质量规则集失败：{ex.Message}", isError: true);
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
                SetGovernanceStatus("请先选择或填写一个质量规则集。", isError: true);
                return;
            }

            await _governanceClient.DeleteQualityRuleSetAsync(baseUrl, ApiKeyTextBox.Text.Trim(), id);
            SetGovernanceStatus($"已删除质量规则集：{id}", isError: false);
            QualityRuleSetIdTextBox.Text = string.Empty;
            QualityRuleSetNameTextBox.Text = string.Empty;
            QualityRuleSetVersionTextBox.Text = "v1";
            QualityRuleSetJsonTextBox.Text = string.Empty;
            _selectedQualityRuleSet = null;
            await RefreshQualityRuleSetsAsync();
        }
        catch (Exception ex)
        {
            SetGovernanceStatus($"删除质量规则集失败：{ex.Message}", isError: true);
        }
    }

    private async Task RefreshSandboxGovernanceAsync()
    {
        try
        {
            var baseUrl = await EnsureGovernanceBoundaryLoadedAsync();
            var apiKey = ApiKeyTextBox.Text.Trim();
            var rules = await _governanceClient.GetWorkflowSandboxRulesAsync(baseUrl, apiKey);
            SandboxRulesJsonTextBox.Text = PrettyGovernanceJson(rules.ToJsonString());

            var versions = await _governanceClient.ListWorkflowSandboxRuleVersionsAsync(baseUrl, apiKey, 80);
            SandboxRuleVersionsListView.Items.Clear();
            foreach (var item in versions)
            {
                SandboxRuleVersionsListView.Items.Add(item);
            }

            var autofixState = await _governanceClient.GetWorkflowSandboxAutoFixStateAsync(baseUrl, apiKey);
            _currentSandboxAutoFixState = autofixState;
            SandboxAutoFixStateTextBlock.Text = autofixState.DisplayText;
            SandboxAutoFixModeTextBox.Text = autofixState.ForcedIsolationMode;
            SandboxAutoFixUntilTextBox.Text = autofixState.ForcedUntil;
            SandboxAutoFixGreenStreakTextBox.Text = autofixState.GreenStreak.ToString();

            var actions = await _governanceClient.ListWorkflowSandboxAutoFixActionsAsync(baseUrl, apiKey, 60);
            SandboxAutoFixActionsListView.Items.Clear();
            foreach (var item in actions)
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
            SetGovernanceStatus($"刷新 Sandbox 治理失败：{ex.Message}", isError: true);
        }
    }

    private async Task SaveSandboxRulesAsync()
    {
        try
        {
            var baseUrl = await EnsureGovernanceBoundaryLoadedAsync();
            var rules = ParseJsonObjectOrThrow(SandboxRulesJsonTextBox.Text);
            var versionId = await _governanceClient.SaveWorkflowSandboxRulesAsync(baseUrl, ApiKeyTextBox.Text.Trim(), rules);
            SetGovernanceStatus($"已保存 Sandbox 规则：{versionId}", isError: false);
            await RefreshSandboxGovernanceAsync();
        }
        catch (Exception ex)
        {
            SetGovernanceStatus($"保存 Sandbox 规则失败：{ex.Message}", isError: true);
        }
    }

    private async Task RollbackSandboxRuleVersionAsync()
    {
        try
        {
            var versionId = _selectedSandboxRuleVersion?.VersionId ?? string.Empty;
            if (string.IsNullOrWhiteSpace(versionId))
            {
                SetGovernanceStatus("请先选择一个 Sandbox 规则版本。", isError: true);
                return;
            }

            var baseUrl = await EnsureGovernanceBoundaryLoadedAsync();
            var newVersion = await _governanceClient.RollbackWorkflowSandboxRuleVersionAsync(baseUrl, ApiKeyTextBox.Text.Trim(), versionId);
            SetGovernanceStatus($"已回滚 Sandbox 规则，生成新版本：{newVersion}", isError: false);
            await RefreshSandboxGovernanceAsync();
        }
        catch (Exception ex)
        {
            SetGovernanceStatus($"回滚 Sandbox 规则失败：{ex.Message}", isError: true);
        }
    }

    private async Task ApplySandboxMuteAsync()
    {
        try
        {
            var baseUrl = await EnsureGovernanceBoundaryLoadedAsync();
            var currentRules = ParseJsonObjectOrThrow(SandboxRulesJsonTextBox.Text);
            var minutes = int.TryParse(SandboxMuteMinutesTextBox.Text.Trim(), out var parsedMinutes)
                ? parsedMinutes
                : 60;
            var nextRules = GovernanceEditorSupport.ApplySandboxMute(
                currentRules,
                SandboxMuteNodeTypeTextBox.Text,
                SandboxMuteNodeIdTextBox.Text,
                SandboxMuteCodeTextBox.Text,
                minutes,
                DateTimeOffset.UtcNow);

            SandboxRulesJsonTextBox.Text = PrettyGovernanceJson(nextRules.ToJsonString());
            var versionId = await _governanceClient.SaveWorkflowSandboxRulesAsync(
                baseUrl,
                ApiKeyTextBox.Text.Trim(),
                nextRules,
                reason: "winui_governance_mute");
            SetGovernanceStatus($"已应用 Sandbox 静默：{versionId}", isError: false);
            await RefreshSandboxGovernanceAsync();
        }
        catch (Exception ex)
        {
            SetGovernanceStatus($"应用 Sandbox 静默失败：{ex.Message}", isError: true);
        }
    }

    private async Task SaveSandboxAutoFixStateAsync()
    {
        try
        {
            var baseUrl = await EnsureGovernanceBoundaryLoadedAsync();
            var greenStreak = int.TryParse(SandboxAutoFixGreenStreakTextBox.Text.Trim(), out var parsedGreen)
                ? Math.Max(0, parsedGreen)
                : 0;
            var nextState = _currentSandboxAutoFixState with
            {
                ForcedIsolationMode = SandboxAutoFixModeTextBox.Text.Trim(),
                ForcedUntil = SandboxAutoFixUntilTextBox.Text.Trim(),
                GreenStreak = greenStreak,
            };
            _currentSandboxAutoFixState = await _governanceClient.SaveWorkflowSandboxAutoFixStateAsync(
                baseUrl,
                ApiKeyTextBox.Text.Trim(),
                nextState);
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
            SetGovernanceStatus("请先选择一条待审核项。", isError: true);
            return;
        }

        try
        {
            var baseUrl = await EnsureGovernanceBoundaryLoadedAsync();
            var selected = _selectedGovernanceReview.Item;
            var item = await _governanceClient.SubmitManualReviewAsync(
                baseUrl,
                ApiKeyTextBox.Text.Trim(),
                selected.RunId,
                selected.ReviewKey,
                approved,
                GovernanceReviewerTextBox.Text.Trim(),
                GovernanceCommentTextBox.Text.Trim());

            SetGovernanceStatus(
                approved
                    ? $"已记录批准：{item.RunId} / {item.ReviewKey}"
                    : $"已记录拒绝：{item.RunId} / {item.ReviewKey}",
                isError: false);
            GovernanceCommentTextBox.Text = string.Empty;
            _selectedGovernanceReview = null;
            PendingReviewsListView.SelectedItem = null;
            ApplyGovernanceSelection();
            await RefreshGovernanceAsync();
        }
        catch (Exception ex)
        {
            SetGovernanceStatus($"提交审核失败：{ex.Message}", isError: true);
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
