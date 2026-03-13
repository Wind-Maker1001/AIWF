namespace AIWF.Native.Runtime;

public static class ResultPanelController
{
    public static ResultPanelState CreateInitialState()
    {
        return ResultPanelState.Empty;
    }

    public static ResultPanelState CreateParseFailureState()
    {
        return ResultPanelState.Empty with
        {
            RunResultText = "Run completed, but the response could not be parsed.",
            OkMetricText = "-",
            ModeMetricText = "-",
            DurationMetricText = "-"
        };
    }

    public static ResultPanelState CreateFromResult(RunResultViewData data)
    {
        var view = RunResultPresentationMapper.Map(data);
        return new ResultPanelState(
            $"{data.Artifacts.Count} items",
            view.JobIdText,
            "Not retried",
            view.SummaryText,
            view.ModeText,
            view.DurationText,
            view.OkMetricText,
            view.ModeText,
            view.DurationMetricText);
    }

    public static ResultPanelState WithRetryInfo(ResultPanelState state, string retryInfo)
    {
        return state with
        {
            RetryInfoText = string.IsNullOrWhiteSpace(retryInfo) ? "Not retried" : retryInfo
        };
    }
}
