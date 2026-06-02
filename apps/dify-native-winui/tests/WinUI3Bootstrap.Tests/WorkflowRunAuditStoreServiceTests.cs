using System.Text.Json.Nodes;
using AIWF.Native.Runtime;
using Xunit;

namespace AIWF.Native.Tests;

public sealed class WorkflowRunAuditStoreServiceTests
{
    [Fact]
    public void AppendFromResponse_StoresAndReadsLocalRun()
    {
        var tempDir = Path.Combine(Path.GetTempPath(), "aiwf-run-audit-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tempDir);
        try
        {
            var service = new WorkflowRunAuditStoreService(Path.Combine(tempDir, "run_history.jsonl"));
            var appended = service.AppendFromResponse(
                """
                {
                  "ok": false,
                  "status": "failed",
                  "run_id": "run_local_1",
                  "workflow_id": "wf_local",
                  "node_runs": [
                    {
                      "id": "n1",
                      "type": "quality_check_v3",
                      "status": "failed",
                      "error": "sandbox_limit_exceeded:output",
                      "started_at": "2026-06-01T00:00:00Z",
                      "ended_at": "2026-06-01T00:00:02Z",
                      "seconds": 2
                    }
                  ]
                }
                """,
                "run_local_1",
                new JsonObject
                {
                    ["flow"] = "cleaning",
                    ["params"] = new JsonObject
                    {
                        ["region"] = "cn"
                    }
                },
                workflowId: "wf_local",
                runRequestKind: "legacy_flow",
                workflowDefinitionSource: "legacy_flow_dispatch");

            Assert.NotNull(appended);
            var runs = service.ListRuns(20);
            var item = Assert.Single(runs);
            Assert.Equal("run_local_1", item.RunId);
            Assert.Equal("legacy_flow", item.RunRequestKind);
            Assert.Equal("cleaning", item.Payload["flow"]?.GetValue<string>());
            Assert.Single(item.Steps);
            Assert.Equal("sandbox_limit_exceeded:output", item.Steps[0].Error);
        }
        finally
        {
            Directory.Delete(tempDir, recursive: true);
        }
    }
}
