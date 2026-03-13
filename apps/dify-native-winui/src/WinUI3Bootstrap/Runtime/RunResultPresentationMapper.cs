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
            data.Ok == true ? "Run succeeded." : data.Ok == false ? "Run failed." : "Run status unknown.",
            data.JobId,
            data.Ok == true ? "Success" : data.Ok == false ? "Failed" : "-",
            data.RunMode,
            data.DurationMs?.ToString() ?? "-",
            data.DurationMs is null ? "-" : $"{data.DurationMs} ms");
    }
}
