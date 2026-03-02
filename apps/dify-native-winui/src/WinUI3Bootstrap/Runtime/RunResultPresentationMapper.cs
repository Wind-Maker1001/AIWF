namespace AIWF.Native.Runtime;

public sealed record RunResultPresentation(
    string SummaryText,
    string JobIdText,
    string OkMetricText,
    string ModeText,
    string DurationText,
    string DurationMetricText);

public static class RunResultPresentationMapper
{
    public static RunResultPresentation Map(RunResultViewData data)
    {
        return new RunResultPresentation(
            data.Ok == true ? "执行成功。" : data.Ok == false ? "执行失败。" : "状态未知。",
            data.JobId,
            data.Ok?.ToString() ?? "-",
            data.RunMode,
            data.DurationMs?.ToString() ?? "-",
            data.DurationMs is null ? "-" : $"{data.DurationMs} ms");
    }
}
