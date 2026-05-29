using AIWF.Native.Runtime;
using Xunit;

namespace AIWF.Native.Tests;

public sealed class WorkflowRunBaselineExportSupportTests
{
    [Fact]
    public void RenderCompareMarkdown_IncludesSummaryAndRows()
    {
        var output = new WorkflowRunCompareResult(
            true,
            new WorkflowRunCompareSummary("run_a", "run_b", "done", "failed", true, false, 2, 2, 1),
            [
                new WorkflowRunCompareNodeDiffItem("clean_md", "clean_md", "DONE", "FAILED", true, 1, 2, 1)
            ]);

        var markdown = WorkflowRunBaselineExportSupport.RenderCompareMarkdown(output);

        Assert.Contains("# AIWF Run Compare Report", markdown, StringComparison.Ordinal);
        Assert.Contains("Run A: run_a", markdown, StringComparison.Ordinal);
        Assert.Contains("| clean_md (clean_md) | DONE | FAILED | 1 | 2 | 1 |", markdown, StringComparison.Ordinal);
    }

    [Fact]
    public void RenderCompareHtml_IncludesSummaryAndTableRows()
    {
        var output = new WorkflowRunCompareResult(
            true,
            new WorkflowRunCompareSummary("run_a", "run_b", "done", "failed", true, false, 2, 2, 1),
            [
                new WorkflowRunCompareNodeDiffItem("clean_md", "clean_md", "DONE", "FAILED", true, 1, 2, 1)
            ]);

        var html = WorkflowRunBaselineExportSupport.RenderCompareHtml(output);

        Assert.Contains("<title>AIWF Run Compare Report</title>", html, StringComparison.Ordinal);
        Assert.Contains("Run A: run_a", html, StringComparison.Ordinal);
        Assert.Contains("<td>clean_md (clean_md)</td>", html, StringComparison.Ordinal);
    }
}
