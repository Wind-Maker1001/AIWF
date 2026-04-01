using AIWF.Native.Runtime;
using Xunit;

namespace AIWF.Native.Tests;

public sealed class SqlStudioWorkflowMapperTests
{
    [Fact]
    public void Build_FallsBackToSingleLoadNodeForTextOwnedDraft()
    {
        var document = SqlStudioWorkflowMapper.Build(
            SqlConnectionProfile.Default with
            {
                SourceType = SqlConnectionProfile.Sqlite,
                SQLitePath = @"D:\demo.db"
            },
            SqlBuilderDraft.Empty,
            new SqlTextDraft("SELECT * FROM demo", true),
            workflowId: "wf_text");

        Assert.Equal("wf_text", document.WorkflowId);
        Assert.Single(document.Nodes);
        Assert.Equal("load_rows_v3", document.Nodes[0].Type);
        Assert.Equal("SELECT * FROM demo", document.Nodes[0].Config["query"]?.GetValue<string>());
    }

    [Fact]
    public void Build_CreatesStructuredGraphForEqFilterAndAggregate()
    {
        var document = SqlStudioWorkflowMapper.Build(
            SqlConnectionProfile.Default with
            {
                SourceType = SqlConnectionProfile.Sqlite,
                SQLitePath = @"D:\demo.db"
            },
            SqlBuilderDraft.Empty with
            {
                Schema = "main",
                Table = "sales",
                SelectFields = ["category"],
                Filters = [new SqlFilterClause("region", "=", "cn")],
                GroupByFields = ["category"],
                Aggregates = [new SqlAggregateClause("count", "*", "row_count")]
            },
            SqlTextDraft.Empty,
            workflowId: "wf_structured");

        Assert.Equal("wf_structured", document.WorkflowId);
        Assert.True(document.Nodes.Count >= 3);
        Assert.Contains(document.Nodes, node => node.Type == "load_rows_v3");
        Assert.Contains(document.Nodes, node => node.Type == "columnar_eval_v1");
        Assert.Contains(document.Nodes, node => node.Type == "aggregate_rows_v2");
    }
}
