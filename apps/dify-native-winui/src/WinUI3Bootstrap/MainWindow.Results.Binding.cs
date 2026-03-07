using System.Text.Json;
using AIWF.Native.Runtime;
using Microsoft.UI.Xaml.Media;

namespace AIWF.Native;

public sealed partial class MainWindow
{
    private void BindRunResult(string json, string retryInfo = "未重试")
    {
        ResetRunResultPresentation();
        if (!RunResultParser.TryParse(json, out var parsed))
        {
            return;
        }

        var panelState = ResultPanelController.CreateFromResult(parsed);
        ApplyResultPanelState(ResultPanelController.WithRetryInfo(panelState, retryInfo));
        ApplyRunStatusBadge(parsed.Ok);
        ApplyMetricVisuals(parsed.Ok, parsed.RunMode, parsed.DurationMs);

        foreach (var artifact in parsed.Artifacts)
        {
            ArtifactsListView.Items.Add(ArtifactPresentationMapper.FormatListDisplay(artifact));
        }

        ArtifactsCountTextBlock.Text = $"{ArtifactsListView.Items.Count} 项";
        UpdateCanvasArtifactNodes(parsed.Artifacts);
    }

    private void ResetRunResultPresentation()
    {
        ArtifactsListView.Items.Clear();
        ClearCanvasArtifactNodes();
        SetCanvasNodeSubtitle(_inputNode, "源数据准备");
        SetCanvasNodeSubtitle(_cleanNode, "规则处理");
        SetCanvasNodeSubtitle(_outputNode, "等待运行结果");
        ApplyResultPanelState(ResultPanelController.CreateInitialState());
        ApplyMetricVisuals(null, "-", null);
        ApplyRunStatusBadge(null);
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
