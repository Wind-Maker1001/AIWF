using AIWF.Native.Runtime;
using Xunit;

namespace AIWF.Native.Tests;

public sealed class RunResultPresentationMapperTests
{
    [Fact]
    public void Map_PreservesCoreMetricsAndFormatsDuration()
    {
        var result = RunResultPresentationMapper.Map(new RunResultViewData
        {
            Ok = true,
            JobId = "job-123",
            RunMode = "cleaning",
            DurationMs = 912
        });

        Assert.Equal("job-123", result.JobIdText);
        Assert.Equal("True", result.OkMetricText);
        Assert.Equal("cleaning", result.ModeText);
        Assert.Equal("912", result.DurationText);
        Assert.Equal("912 ms", result.DurationMetricText);
        Assert.False(string.IsNullOrWhiteSpace(result.SummaryText));
    }

    [Fact]
    public void Map_UsesDashPlaceholdersWhenMetricsAreMissing()
    {
        var result = RunResultPresentationMapper.Map(new RunResultViewData
        {
            Ok = null,
            JobId = "-",
            RunMode = "-",
            DurationMs = null
        });

        Assert.Equal("-", result.OkMetricText);
        Assert.Equal("-", result.ModeText);
        Assert.Equal("-", result.DurationText);
        Assert.Equal("-", result.DurationMetricText);
        Assert.False(string.IsNullOrWhiteSpace(result.SummaryText));
    }

    [Fact]
    public void Map_ChangesSummaryAcrossSuccessFailureAndUnknownStates()
    {
        var success = RunResultPresentationMapper.Map(new RunResultViewData { Ok = true });
        var failure = RunResultPresentationMapper.Map(new RunResultViewData { Ok = false });
        var unknown = RunResultPresentationMapper.Map(new RunResultViewData { Ok = null });

        Assert.NotEqual(success.SummaryText, failure.SummaryText);
        Assert.NotEqual(success.SummaryText, unknown.SummaryText);
        Assert.NotEqual(failure.SummaryText, unknown.SummaryText);
    }
}
