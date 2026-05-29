using AIWF.Native.Runtime;
using Xunit;

namespace AIWF.Native.Tests;

public sealed class GovernanceAuditExportSupportTests
{
    [Fact]
    public void BuildExportEnvelope_ContainsTimelineFailuresAndAuditRows()
    {
        var state = new GovernanceAuditRefreshResult(
            Runs:
            [
                new GovernanceWorkflowRunItem("run_1", "wf_finance", "done", true, "2026-05-29T00:00:00Z")
            ],
            Timeline:
            [
                new GovernanceTimelineEntry("n1", "clean_md", "DONE", "2026-05-29T00:00:00Z", "2026-05-29T00:00:01Z", 1)
            ],
            Failures:
            [
                new GovernanceFailureSummaryEntry("quality_check_v3", 2, "boom")
            ],
            AuditEvents:
            [
                new GovernanceAuditEventItem("2026-05-29T00:00:02Z", "run_workflow", "{\"run_id\":\"run_1\"}")
            ],
            SummaryText: "runs=1, failure_types=1, audit_events=1");

        var payload = GovernanceAuditExportSupport.BuildExportEnvelope("run_1", "run_workflow", state);

        Assert.Equal("run_1", payload["run_id"]?.GetValue<string>());
        Assert.Equal("run_workflow", payload["action_filter"]?.GetValue<string>());
        Assert.Equal("quality_check_v3", payload["failure_summary"]?[0]?["node_type"]?.GetValue<string>());
        Assert.Equal("run_workflow", payload["audit_events"]?[0]?["action"]?.GetValue<string>());
    }

    [Fact]
    public void RenderMarkdown_FormatsSections()
    {
        var state = new GovernanceAuditRefreshResult(
            Runs: [],
            Timeline:
            [
                new GovernanceTimelineEntry("n1", "clean_md", "DONE", "2026-05-29T00:00:00Z", "2026-05-29T00:00:01Z", 1)
            ],
            Failures:
            [
                new GovernanceFailureSummaryEntry("quality_check_v3", 2, "boom")
            ],
            AuditEvents:
            [
                new GovernanceAuditEventItem("2026-05-29T00:00:02Z", "run_workflow", "{\"run_id\":\"run_1\"}")
            ],
            SummaryText: "runs=1, failure_types=1, audit_events=1");

        var markdown = GovernanceAuditExportSupport.RenderMarkdown("run_1", "run_workflow", state);

        Assert.Contains("# AIWF Governance Audit Export", markdown, StringComparison.Ordinal);
        Assert.Contains("## Timeline", markdown, StringComparison.Ordinal);
        Assert.Contains("| n1 | clean_md | DONE | 1 |", markdown, StringComparison.Ordinal);
        Assert.Contains("## Failure Summary", markdown, StringComparison.Ordinal);
        Assert.Contains("## Audit Events", markdown, StringComparison.Ordinal);
    }
}
