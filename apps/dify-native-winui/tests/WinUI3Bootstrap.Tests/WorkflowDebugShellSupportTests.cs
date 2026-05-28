using System.Text.Json.Nodes;
using AIWF.Native.Runtime;
using Xunit;

namespace AIWF.Native.Tests;

public sealed class WorkflowDebugShellSupportTests
{
    [Fact]
    public void ParseNodeIds_NormalizesCsvAndDeduplicates()
    {
        var ids = WorkflowDebugShellSupport.ParseNodeIds(" n1, n2,\n n1 ,, n3 ");

        Assert.Equal(["n1", "n2", "n3"], ids);
    }

    [Fact]
    public void BuildRouteStats_ProducesStableDebugPayload()
    {
        var stats = WorkflowDebugShellSupport.BuildRouteStats(
            nodeCount: 6,
            userNodeCount: 3,
            edgeCount: 4,
            selectedNodeIds: ["n1", "n2"],
            scale: 1.23456,
            translateX: 12.3456,
            translateY: -8.7654);

        Assert.Equal(6, stats["node_count"]?.GetValue<int>());
        Assert.Equal(3, stats["user_node_count"]?.GetValue<int>());
        Assert.Equal(4, stats["edge_count"]?.GetValue<int>());
        Assert.Equal(1.235, stats["scale"]?.GetValue<double>());
        Assert.Equal(12.346, stats["translate_x"]?.GetValue<double>());
        Assert.Equal(-8.765, stats["translate_y"]?.GetValue<double>());
        Assert.Equal("n1", stats["selected_node_ids"]?[0]?.GetValue<string>());
    }

    [Fact]
    public void ResolveWorkflowDefinitionForImport_UnwrapsWrappedPayload()
    {
        var resolved = WorkflowDebugShellSupport.ResolveWorkflowDefinitionForImport(new JsonObject
        {
            ["workflow_definition"] = new JsonObject
            {
                ["workflow_id"] = "wf_debug",
                ["version"] = "1.0.0",
                ["nodes"] = new JsonArray(),
                ["edges"] = new JsonArray()
            }
        });

        Assert.Equal("wf_debug", resolved["workflow_id"]?.GetValue<string>());
        Assert.NotNull(resolved["nodes"]);
        Assert.NotNull(resolved["edges"]);
    }
}
