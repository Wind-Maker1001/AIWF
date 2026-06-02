using System.Text.Json.Nodes;
using AIWF.Native.Runtime;
using Xunit;

namespace AIWF.Native.Tests;

public sealed class GovernanceSandboxAuditExportSupportTests
{
    [Fact]
    public void BuildExportEnvelope_ContainsRulesHealthAndAlertRows()
    {
        var state = new GovernanceSandboxAlertRefreshResult(
            Items:
            [
                new JsonObject
                {
                    ["run_id"] = "run_1",
                    ["node_type"] = "clean_md",
                    ["node_id"] = "n1",
                    ["error"] = "sandbox_limit_exceeded:output",
                }
            ],
            ByNode:
            [
                new GovernanceSandboxAlertRow("clean_md", "n1", 1, "run_1", "2026-06-01T00:00:00Z")
            ],
            Rules: new JsonObject
            {
                ["whitelist_codes"] = new JsonArray("sandbox_limit_exceeded:output")
            },
            Health: new GovernanceSandboxAlertHealth("yellow", 1, 1, 3, 60, 0, 0, 0, 0));

        var payload = GovernanceSandboxAuditExportSupport.BuildExportEnvelope(state);

        Assert.Equal(1, payload["total"]?.GetValue<int>());
        Assert.Equal("yellow", payload["health"]?["level"]?.GetValue<string>());
        Assert.Equal("clean_md", payload["by_node"]?[0]?["node_type"]?.GetValue<string>());
    }
}
