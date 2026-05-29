namespace AIWF.Native.Runtime;

public static class WorkflowRunBaselinePresenter
{
    public static string BuildCompareSummary(WorkflowRunCompareResult result)
    {
        var summary = result.Summary;
        return $"Run A: {summary.RunA} | Run B: {summary.RunB} | changed={summary.ChangedNodes} | nodes={summary.NodeCountA}/{summary.NodeCountB}";
    }

    public static IReadOnlyList<string> BuildCompareRows(WorkflowRunCompareResult result)
    {
        return result.NodeDiff
            .Select(item => $"{item.Id} | {item.StatusA} -> {item.StatusB} | {item.SecondsA:0.###}s -> {item.SecondsB:0.###}s | delta={item.SecondsDelta:0.###}s")
            .ToArray();
    }

    public static string BuildRegressionSummary(WorkflowRunBaselineRegressionResult result)
    {
        return $"Baseline: {result.BaselineName} ({result.BaselineId}) | changed={result.ChangedNodes} | status_flip={result.StatusFlipNodes} | perf_hot={result.PerfHotNodes}";
    }
}
