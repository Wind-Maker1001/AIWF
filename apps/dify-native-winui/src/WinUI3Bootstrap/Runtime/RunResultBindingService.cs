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
    public static bool IsBusinessSuccess(RunResultBindingState state)
    {
        return state.BadgeOk == true;
    }

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
            InputNodeSubtitle: "Source ready",
            CleanNodeSubtitle: "Processing rules",
            OutputNodeSubtitle: "Waiting for run result");
    }

    public static RunResultBindingState CreateParseFailureState(string retryInfo = "Not retried")
    {
        return new RunResultBindingState(
            ResultPanelController.WithRetryInfo(ResultPanelController.CreateParseFailureState(), retryInfo),
            BadgeOk: false,
            MetricMode: "-",
            MetricDurationMs: null,
            ArtifactDisplayItems: [],
            Artifacts: [],
            SyncArtifactsToCanvas: false,
            InputNodeSubtitle: "Source ready",
            CleanNodeSubtitle: "Parse failed",
            OutputNodeSubtitle: "Response format not recognized");
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

    public static RunResultBindingState CreateFromParsedResult(RunResultViewData parsed, string retryInfo = "Not retried")
    {
        var panelState = ResultPanelController.WithRetryInfo(ResultPanelController.CreateFromResult(parsed), retryInfo);
        var artifactDisplayItems = parsed.Artifacts
            .Select(ArtifactPresentationMapper.FormatListDisplay)
            .ToArray();
        var artifactsCount = parsed.Artifacts.Count;

        var cleanNodeSubtitle = parsed.Ok switch
        {
            true => "Processing complete",
            false => "Processing failed",
            _ => "Result pending confirmation",
        };

        var outputNodeSubtitle = parsed.Ok switch
        {
            true => artifactsCount > 0 ? $"Generated {artifactsCount} artifacts" : "No usable artifacts",
            false => "Run failed, no usable artifacts generated",
            _ => "Missing ok status, inspect the raw response",
        };

        return new RunResultBindingState(
            panelState,
            BadgeOk: parsed.Ok,
            MetricMode: parsed.RunMode,
            MetricDurationMs: parsed.DurationMs,
            ArtifactDisplayItems: artifactDisplayItems,
            Artifacts: parsed.Artifacts,
            SyncArtifactsToCanvas: parsed.Ok == true,
            InputNodeSubtitle: "Source ready",
            CleanNodeSubtitle: cleanNodeSubtitle,
            OutputNodeSubtitle: outputNodeSubtitle);
    }
}
