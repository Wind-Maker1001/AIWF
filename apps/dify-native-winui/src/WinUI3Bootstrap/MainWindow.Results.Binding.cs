using System.Text.Json;
using AIWF.Native.Runtime;
using Microsoft.UI.Xaml.Media;

namespace AIWF.Native;

public sealed partial class MainWindow
{
    private bool TryBindRunResult(string json, string retryInfo = "未重试")
    {
        if (!RunResultBindingService.TryCreateFromJson(json, retryInfo, out var state))
        {
            ApplyRunResultBindingState(state);
            return false;
        }

        ApplyRunResultBindingState(state);
        return true;
    }

    private void ApplyRunResultBindingState(RunResultBindingState state)
    {
        ArtifactsListView.Items.Clear();
        foreach (var displayItem in state.ArtifactDisplayItems)
        {
            ArtifactsListView.Items.Add(displayItem);
        }

        var canvasReady = _isCanvasWorkspaceInitialized;
        if (!canvasReady && state.SyncArtifactsToCanvas)
        {
            canvasReady = EnsureCanvasWorkspaceInitialized();
        }

        if (canvasReady)
        {
            ClearCanvasArtifactNodes();
            if (state.SyncArtifactsToCanvas)
            {
                UpdateCanvasArtifactNodes(state.Artifacts);
            }

            SetCanvasNodeSubtitle(_inputNode, state.InputNodeSubtitle);
            SetCanvasNodeSubtitle(_cleanNode, state.CleanNodeSubtitle);
            SetCanvasNodeSubtitle(_outputNode, state.OutputNodeSubtitle);
        }

        ApplyResultPanelState(state.PanelState);
        ApplyMetricVisuals(state.BadgeOk, state.MetricMode, state.MetricDurationMs);
        ApplyRunStatusBadge(state.BadgeOk);
    }

    private void ResetRunResultPresentation()
    {
        ApplyRunResultBindingState(RunResultBindingService.CreateInitialState());
    }

    private void ApplyResultPanelState(ResultPanelState state)
    {
        ArtifactsCountTextBlock.Text = state.ArtifactsCountText;
        JobIdTextBlock.Text = state.JobIdText;
        RetryInfoTextBlock.Text = state.RetryInfoText;
        RunResultTextBlock.Text = state.RunResultText;
        RunModeTextBlock.Text = state.RunModeText;
        DurationTextBlock.Text = state.DurationText;
        OkMetricTextBlock.Text = state.OkMetricText;
        ModeMetricTextBlock.Text = state.ModeMetricText;
        DurationMetricTextBlock.Text = state.DurationMetricText;
    }

    private static string PrettyJson(string text)
    {
        try
        {
            using var doc = JsonDocument.Parse(text);
            return JsonSerializer.Serialize(doc.RootElement, new JsonSerializerOptions
            {
                WriteIndented = true
            });
        }
        catch
        {
            return text;
        }
    }

    private void ApplyMetricVisuals(bool? ok, string mode, int? durationMs)
    {
        var mapped = RunVisualStateMapper.MapMetrics(ok, mode, durationMs);
        OkMetricTextBlock.Foreground = ToMetricBrush(mapped.OkState);
        ModeMetricTextBlock.Foreground = ToMetricBrush(mapped.ModeState);
        DurationMetricTextBlock.Foreground = ToMetricBrush(mapped.DurationState);
    }

    private void ApplyRunStatusBadge(bool? ok)
    {
        var visual = RunBadgePresenter.Resolve(ok);
        RunStatusBadgeText.Text = visual.Text;
        RunStatusBadgeText.Foreground = new SolidColorBrush(visual.Foreground);
        RunStatusBadgeBorder.BorderBrush = new SolidColorBrush(visual.Border);
        RunStatusBadgeBorder.Background = new SolidColorBrush(visual.Background);
    }

    private static SolidColorBrush ToMetricBrush(MetricVisualState state)
    {
        return new SolidColorBrush(
            state switch
            {
                MetricVisualState.Good => Windows.UI.Color.FromArgb(0xFF, 0x11, 0x11, 0x11),
                MetricVisualState.Warning => Windows.UI.Color.FromArgb(0xFF, 0x6B, 0x72, 0x80),
                MetricVisualState.Danger => Windows.UI.Color.FromArgb(0xFF, 0xC6, 0x28, 0x28),
                _ => Windows.UI.Color.FromArgb(0xFF, 0x6B, 0x72, 0x80)
            });
    }
}
