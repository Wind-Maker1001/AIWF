using AIWF.Native.Runtime;
using Xunit;

namespace AIWF.Native.Tests;

public sealed class WorkflowVersionCacheServiceTests
{
    [Fact]
    public void VersionListAndCompare_RoundTrip_AndTrackStats()
    {
        var root = Path.Combine(Path.GetTempPath(), $"aiwf-workflow-version-cache-{Guid.NewGuid():N}");
        Directory.CreateDirectory(root);
        try
        {
            var service = new WorkflowVersionCacheService(
                Path.Combine(root, "workflow-version-cache.json"),
                Path.Combine(root, "workflow-version-cache-metrics.json"),
                () => "2026-05-19T10:20:00Z");
            service.SaveVersionList([
                new GovernanceWorkflowVersionItem("ver_a", "Workflow A", "wf_a", "2026-05-19T10:00:00Z", "glue-python", "glue-python")
            ]);
            service.SaveCompareResult(
                "ver_a",
                "ver_b",
                new GovernanceWorkflowVersionCompareResult(
                    true,
                    "glue-python",
                    new GovernanceWorkflowVersionCompareSummary("ver_a", "ver_b", 1, 1, 0),
                    [new GovernanceWorkflowVersionCompareNodeDiffItem("n1", "load_rows_v3", "updated", true, false)]));

            Assert.True(service.TryGetCompareResult("ver_a", "ver_b", out var hit));
            Assert.NotNull(hit);
            Assert.False(service.TryGetCompareResult("ver_x", "ver_y", out _));

            var stats = service.GetStats();
            Assert.Equal(2, stats.Entries);
            Assert.Equal(1, stats.Hits);
            Assert.Equal(1, stats.Misses);
            Assert.Equal(2, stats.Sets);
            Assert.Equal(0.5, stats.HitRate, 3);
        }
        finally
        {
            if (Directory.Exists(root))
            {
                Directory.Delete(root, true);
            }
        }
    }

    [Fact]
    public void Clear_ResetsEntriesAndMetrics()
    {
        var root = Path.Combine(Path.GetTempPath(), $"aiwf-workflow-version-cache-{Guid.NewGuid():N}");
        Directory.CreateDirectory(root);
        try
        {
            var service = new WorkflowVersionCacheService(
                Path.Combine(root, "workflow-version-cache.json"),
                Path.Combine(root, "workflow-version-cache-metrics.json"),
                () => "2026-05-19T10:20:00Z");
            service.SaveVersionList([
                new GovernanceWorkflowVersionItem("ver_a", "Workflow A", "wf_a", "2026-05-19T10:00:00Z", "glue-python", "glue-python")
            ]);
            _ = service.GetStats();

            var stats = service.Clear();

            Assert.Equal(0, stats.Entries);
            Assert.Equal(0, stats.Hits);
            Assert.Equal(0, stats.Misses);
            Assert.Equal(0, stats.Sets);
            Assert.Equal("2026-05-19T10:20:00Z", stats.LastResetAt);
        }
        finally
        {
            if (Directory.Exists(root))
            {
                Directory.Delete(root, true);
            }
        }
    }

    [Fact]
    public void CorruptFiles_FailOpenToEmptyCache()
    {
        var root = Path.Combine(Path.GetTempPath(), $"aiwf-workflow-version-cache-{Guid.NewGuid():N}");
        Directory.CreateDirectory(root);
        try
        {
            var cachePath = Path.Combine(root, "workflow-version-cache.json");
            var metricsPath = Path.Combine(root, "workflow-version-cache-metrics.json");
            File.WriteAllText(cachePath, "{ bad json", System.Text.Encoding.UTF8);
            File.WriteAllText(metricsPath, "{ bad json", System.Text.Encoding.UTF8);
            var service = new WorkflowVersionCacheService(cachePath, metricsPath, () => "2026-05-19T10:20:00Z");

            var stats = service.GetStats();

            Assert.Equal(0, stats.Entries);
            Assert.False(service.TryGetCompareResult("ver_a", "ver_b", out _));
        }
        finally
        {
            if (Directory.Exists(root))
            {
                Directory.Delete(root, true);
            }
        }
    }
}
