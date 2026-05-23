using AIWF.Native.Runtime;
using Xunit;

namespace AIWF.Native.Tests;

public sealed class WorkflowVersionCachePresenterTests
{
    [Fact]
    public void BuildStatsText_FormatsAllMetrics()
    {
        var text = WorkflowVersionCachePresenter.BuildStatsText(
            new WorkflowVersionCacheStats(2, 3, 1, 4, 0.75, "2026-05-19T10:30:00Z"));

        Assert.Contains("entries=2", text, StringComparison.Ordinal);
        Assert.Contains("hits=3", text, StringComparison.Ordinal);
        Assert.Contains("misses=1", text, StringComparison.Ordinal);
        Assert.Contains("sets=4", text, StringComparison.Ordinal);
        Assert.Contains("hit_rate=0.75", text, StringComparison.Ordinal);
        Assert.Contains("last_reset_at=2026-05-19T10:30:00Z", text, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildClearStatusText_FormatsSuccessAndFailure()
    {
        Assert.Equal("Workflow version cache cleared.", WorkflowVersionCachePresenter.BuildClearStatusText(true));
        Assert.Equal("Workflow version cache clear failed: locked", WorkflowVersionCachePresenter.BuildClearStatusText(false, "locked"));
    }
}
