using System.Text.Json.Nodes;
using AIWF.Native.Runtime;
using Xunit;

namespace AIWF.Native.Tests;

public sealed class GovernanceSandboxAlertCoordinatorTests
{
    [Fact]
    public void BuildFromRuns_AggregatesByNodeAndAppliesSuppression()
    {
        var runs = new[]
        {
            new GovernanceWorkflowRunRecordDetail(
                RunId: "run_1",
                WorkflowId: "wf_1",
                Status: "failed",
                Ok: false,
                Timestamp: "2026-06-01T00:00:00Z",
                RunRequestKind: "legacy_flow",
                VersionId: "",
                PublishedVersionId: "",
                WorkflowDefinitionSource: "legacy_flow_dispatch",
                Payload: new JsonObject(),
                Steps: Array.Empty<GovernanceWorkflowRunStepItem>(),
                ResultPayload: new JsonObject
                {
                    ["violations"] = new JsonArray
                    {
                        new JsonObject
                        {
                            ["run_id"] = "run_1",
                            ["workflow_id"] = "wf_1",
                            ["node_id"] = "n1",
                            ["node_type"] = "clean_md",
                            ["error"] = "sandbox_limit_exceeded:output"
                        }
                    }
                }),
            new GovernanceWorkflowRunRecordDetail(
                RunId: "run_2",
                WorkflowId: "wf_1",
                Status: "failed",
                Ok: false,
                Timestamp: "2026-06-01T00:01:00Z",
                RunRequestKind: "legacy_flow",
                VersionId: "",
                PublishedVersionId: "",
                WorkflowDefinitionSource: "legacy_flow_dispatch",
                Payload: new JsonObject(),
                Steps: Array.Empty<GovernanceWorkflowRunStepItem>(),
                ResultPayload: new JsonObject
                {
                    ["violations"] = new JsonArray
                    {
                        new JsonObject
                        {
                            ["run_id"] = "run_2",
                            ["workflow_id"] = "wf_1",
                            ["node_id"] = "n2",
                            ["node_type"] = "ai_refine",
                            ["error"] = "sandbox_egress_blocked"
                        }
                    }
                })
        };

        var rules = new JsonObject
        {
            ["whitelist_node_types"] = new JsonArray("ai_refine"),
            ["mute_until_by_key"] = new JsonObject
            {
                ["clean_md::n1::sandbox_limit_exceeded:output"] = "2099-01-01T00:00:00Z"
            }
        };

        var result = GovernanceSandboxAlertCoordinator.BuildFromRuns(runs, rules, 1, 3, 60);

        Assert.Empty(result.ByNode);
        Assert.Equal("green", result.Health.Level);
        Assert.Equal(0, result.Health.Total);
        Assert.Equal(1, result.Health.SuppressedWhitelist);
        Assert.Equal(1, result.Health.SuppressedMuted);
    }
}
