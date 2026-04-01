using System.Text.Json;
using System.Text.Json.Nodes;
using AIWF.Native.Runtime;
using Xunit;

namespace AIWF.Native.Tests;

public sealed class WorkflowCanvasIntegrationTests
{
    [Fact]
    public void WorkflowMapper_SingleLoadNode_ProducesValidSchema()
    {
        var document = SqlStudioWorkflowMapper.Build(
            SqlConnectionProfile.Default with
            {
                SourceType = SqlConnectionProfile.Sqlite,
                SQLitePath = @"D:\demo.db"
            },
            SqlBuilderDraft.Empty,
            new SqlTextDraft("SELECT * FROM users", true),
            workflowId: "wf_test");

        Assert.Equal("wf_test", document.WorkflowId);
        Assert.Equal("1.0.0", document.Version);
        Assert.Single(document.Nodes);

        var node = document.Nodes[0];
        Assert.Equal("load_rows_v3", node.Type);
        Assert.NotNull(node.Config);
        Assert.Equal("sqlite", node.Config["source_type"]?.GetValue<string>());
        Assert.Equal(@"D:\demo.db", node.Config["source"]?.GetValue<string>());
        Assert.Equal("SELECT * FROM users", node.Config["query"]?.GetValue<string>());
    }

    [Fact]
    public void WorkflowMapper_StructuredGraph_ProducesExpectedNodeTypes()
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
                SelectFields = ["category", "amount"],
                Filters = [new SqlFilterClause("region", "=", "cn")],
                GroupByFields = ["category"],
                Aggregates = [new SqlAggregateClause("sum", "amount", "total")],
                Chart = SqlChartDraft.Default with { Enabled = true },
            },
            SqlTextDraft.Empty,
            workflowId: "wf_structured");

        var types = document.Nodes.Select(n => n.Type).ToHashSet();
        Assert.Contains("load_rows_v3", types);
        Assert.Contains("columnar_eval_v1", types);
        Assert.Contains("aggregate_rows_v2", types);
        Assert.Contains("sql_chart_v1", types);

        // Edges form a connected graph
        Assert.True(document.Edges.Count >= 3);
        foreach (var edge in document.Edges)
        {
            Assert.Contains(document.Nodes, n => n.Id == edge.From);
            Assert.Contains(document.Nodes, n => n.Id == edge.To);
        }
    }

    [Fact]
    public void WorkflowMapper_JoinGraph_ProducesMultipleLoadNodes()
    {
        var document = SqlStudioWorkflowMapper.Build(
            SqlConnectionProfile.Default with
            {
                SourceType = SqlConnectionProfile.SqlServer,
                SqlServerHost = "localhost",
                Database = "AIWF",
            },
            SqlBuilderDraft.Empty with
            {
                Schema = "dbo",
                Table = "orders",
                Joins =
                [
                    new SqlJoinClause(true, "dbo", "customers", "left", "customer_id", "id"),
                ],
                Filters = [new SqlFilterClause("status", "=", "paid")],
            },
            SqlTextDraft.Empty,
            workflowId: "wf_join");

        var loadNodes = document.Nodes.Where(n => n.Type == "load_rows_v3").ToList();
        Assert.True(loadNodes.Count >= 2, "Should have at least 2 load nodes (base + join)");

        var joinNodes = document.Nodes.Where(n => n.Type == "join_rows_v2").ToList();
        Assert.Single(joinNodes);

        // The join node should have input_map with left_rows and right_rows
        var joinConfig = joinNodes[0].Config;
        Assert.NotNull(joinConfig["input_map"]);
    }

    [Fact]
    public void WorkflowDocument_RoundTripsJson()
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
                Table = "metrics",
                SelectFields = ["category", "value"],
            },
            SqlTextDraft.Empty,
            workflowId: "wf_roundtrip");

        var json = JsonSerializer.Serialize(document);
        var deserialized = JsonSerializer.Deserialize<WorkflowGraphDocument>(json);

        Assert.NotNull(deserialized);
        Assert.Equal(document.WorkflowId, deserialized.WorkflowId);
        Assert.Equal(document.Version, deserialized.Version);
        Assert.Equal(document.Nodes.Count, deserialized.Nodes.Count);
        Assert.Equal(document.Edges.Count, deserialized.Edges.Count);
    }

    [Fact]
    public void ChartData_FromWorkflowOutput_ParsesCorrectly()
    {
        // Simulate the output shape of sql_chart_v1 from the Rust backend
        var workflowOutput = new JsonObject
        {
            ["ok"] = true,
            ["operator"] = "sql_chart_v1",
            ["status"] = "done",
            ["chart_type"] = "bar",
            ["categories"] = new JsonArray("Q1", "Q2", "Q3", "Q4"),
            ["series"] = new JsonArray(
                new JsonObject
                {
                    ["name"] = "revenue",
                    ["data"] = new JsonArray(100.0, 200.0, 150.0, 300.0)
                }),
            ["rows_in"] = 100,
        };

        var chartData = SqlChartData.FromJson(workflowOutput);

        Assert.Equal("bar", chartData.ChartType);
        Assert.Equal(4, chartData.Categories.Count);
        Assert.Single(chartData.Series);
        Assert.Equal("revenue", chartData.Series[0].Name);
        Assert.Equal(4, chartData.Series[0].Data.Count);
        Assert.Equal(300.0, chartData.MaxValue);
    }

    [Fact]
    public void ConnectionProfile_BuildRuntimeSource_SqlServer_PreservesStructure()
    {
        var profile = SqlConnectionProfile.Default with
        {
            SourceType = SqlConnectionProfile.SqlServer,
            SqlServerHost = "db.corp",
            SqlServerPort = "1433",
            Database = "Analytics",
            UseWindowsAuth = true,
        };

        var source = profile.BuildRuntimeSource();
        Assert.Equal("db.corp:1433/Analytics?windows_auth=true", source);

        var accelUrl = profile.ResolveAccelUrl(null);
        Assert.Equal("http://127.0.0.1:18082", accelUrl);
    }
}
