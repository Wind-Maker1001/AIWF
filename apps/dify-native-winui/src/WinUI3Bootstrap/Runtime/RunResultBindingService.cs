namespace AIWF.Native.Runtime;

public sealed record RunResultBindingState(
    ResultPanelState PanelState,
    bool? BadgeOk,
    string MetricMode,
    int? MetricDurationMs,
    IReadOnlyList<string> ArtifactDisplayItems,
    IReadOnlyList<RunArtifactItem> Artifacts,
    bool SyncArtifactsToCanvas,
    string InputNodeSubtitle,
    string CleanNodeSubtitle,
    string OutputNodeSubtitle);

public static class RunResultBindingService
{
    public static RunResultBindingState CreateInitialState()
    {
        return new RunResultBindingState(
            ResultPanelController.CreateInitialState(),
            BadgeOk: null,
            MetricMode: "-",
            MetricDurationMs: null,
            ArtifactDisplayItems: [],
            Artifacts: [],
            SyncArtifactsToCanvas: false,
            InputNodeSubtitle: "源数据准备",
            CleanNodeSubtitle: "规则处理",
            OutputNodeSubtitle: "等待运行结果");
    }

    public static RunResultBindingState CreateParseFailureState(string retryInfo = "未重试")
    {
        return new RunResultBindingState(
            ResultPanelController.WithRetryInfo(ResultPanelController.CreateParseFailureState(), retryInfo),
            BadgeOk: false,
            MetricMode: "-",
            MetricDurationMs: null,
            ArtifactDisplayItems: [],
            Artifacts: [],
            SyncArtifactsToCanvas: false,
            InputNodeSubtitle: "源数据准备",
            CleanNodeSubtitle: "结果解析失败",
            OutputNodeSubtitle: "无法识别返回结构");
    }

    public static bool TryCreateFromJson(string json, string retryInfo, out RunResultBindingState state)
    {
        if (!RunResultParser.TryParse(json, out var parsed))
        {
            state = CreateParseFailureState(retryInfo);
            return false;
        }

        state = CreateFromParsedResult(parsed, retryInfo);
        return true;
    }

    public static RunResultBindingState CreateFromParsedResult(RunResultViewData parsed, string retryInfo = "未重试")
    {
        var panelState = ResultPanelController.WithRetryInfo(ResultPanelController.CreateFromResult(parsed), retryInfo);
        var artifactDisplayItems = parsed.Artifacts
            .Select(ArtifactPresentationMapper.FormatListDisplay)
            .ToArray();
        var artifactsCount = parsed.Artifacts.Count;

        return new RunResultBindingState(
            panelState,
            BadgeOk: parsed.Ok,
            MetricMode: parsed.RunMode,
            MetricDurationMs: parsed.DurationMs,
            ArtifactDisplayItems: artifactDisplayItems,
            Artifacts: parsed.Artifacts,
            SyncArtifactsToCanvas: true,
            InputNodeSubtitle: "源数据准备",
            CleanNodeSubtitle: "处理完成",
            OutputNodeSubtitle: artifactsCount > 0 ? $"已生成 {artifactsCount} 个产物" : "无可用产物");
    }
}
