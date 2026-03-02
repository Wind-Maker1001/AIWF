namespace AIWF.Native.Runtime;

public static class ResultPanelController
{
    public static ResultPanelState CreateInitialState()
    {
        return ResultPanelState.Empty;
    }

    public static ResultPanelState CreateFromResult(RunResultViewData data)
    {
        var view = RunResultPresentationMapper.Map(data);
        return new ResultPanelState(
            $"{data.Artifacts.Count} 项",
            view.JobIdText,
            "未重试",
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
            RetryInfoText = string.IsNullOrWhiteSpace(retryInfo) ? "未重试" : retryInfo
        };
    }
}
