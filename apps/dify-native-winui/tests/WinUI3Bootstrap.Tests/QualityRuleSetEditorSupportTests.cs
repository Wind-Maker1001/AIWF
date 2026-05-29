using System.Text.Json.Nodes;
using AIWF.Native.Runtime;
using Xunit;

namespace AIWF.Native.Tests;

public sealed class QualityRuleSetEditorSupportTests
{
    [Fact]
    public void CollectQualityRulesFromWorkflowDocument_ReturnsRulesFromFirstQualityCheckNode()
    {
        var document = new WorkflowGraphDocument(
            "wf_quality",
            "1.0.0",
            [
                new WorkflowGraphNodeDocument(
                    "n1",
                    "load_rows_v3",
                    "Load",
                    string.Empty,
                    10,
                    10,
                    new JsonObject()),
                new WorkflowGraphNodeDocument(
                    "n2",
                    "quality_check_v3",
                    "Quality",
                    string.Empty,
                    30,
                    30,
                    new JsonObject
                    {
                        ["rules"] = new JsonObject
                        {
                            ["required_columns"] = new JsonArray("amount", "currency")
                        }
                    })
            ],
            [],
            WorkflowGraphViewportDocument.Default,
            WorkflowGraphSelectionDocument.Empty);

        var rules = QualityRuleSetEditorSupport.CollectQualityRulesFromWorkflowDocument(document);

        Assert.Equal("amount", rules["required_columns"]?[0]?.GetValue<string>());
        Assert.Equal("currency", rules["required_columns"]?[1]?.GetValue<string>());
    }

    [Fact]
    public void CollectQualityRulesFromWorkflowDocument_ReturnsEmptyWhenMissingQualityNode()
    {
        var document = new WorkflowGraphDocument(
            "wf_quality",
            "1.0.0",
            [
                new WorkflowGraphNodeDocument(
                    "n1",
                    "load_rows_v3",
                    "Load",
                    string.Empty,
                    10,
                    10,
                    new JsonObject())
            ],
            [],
            WorkflowGraphViewportDocument.Default,
            WorkflowGraphSelectionDocument.Empty);

        var rules = QualityRuleSetEditorSupport.CollectQualityRulesFromWorkflowDocument(document);

        Assert.Empty(rules);
    }
}
