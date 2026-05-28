using System.Text.Json.Nodes;
using AIWF.Native.Runtime;
using Xunit;

namespace AIWF.Native.Tests;

public sealed class WorkflowCanvasDocumentBuilderTests
{
    [Fact]
    public void Build_ReturnsNullWhenNoCanvasWorkflowNodes()
    {
        var document = WorkflowCanvasDocumentBuilder.Build(
            "wf_empty",
            "1.0.0",
            [],
            [],
            new CanvasWorkflowViewportState(1, 0, 0),
            new CanvasWorkflowSelectionState([]));

        Assert.Null(document);
    }

    [Fact]
    public void Build_DerivesRuntimeInputMapsForColumnarAndJoinNodes()
    {
        var document = WorkflowCanvasDocumentBuilder.Build(
            "wf_canvas",
            "1.0.0",
            [
                new CanvasWorkflowNodeState("load_a", "load_rows_v3", "Load A", "source", 20, 20, new JsonObject
                {
                    ["source_type"] = "sqlite",
                    ["source"] = "D:/demo.db",
                    ["query"] = "select * from a"
                }),
                new CanvasWorkflowNodeState("load_b", "load_rows_v3", "Load B", "source", 20, 120, new JsonObject
                {
                    ["source_type"] = "sqlite",
                    ["source"] = "D:/demo.db",
                    ["query"] = "select * from b"
                }),
                new CanvasWorkflowNodeState("columnar", "columnar_eval_v1", "Columnar", "filter", 240, 20, new JsonObject()),
                new CanvasWorkflowNodeState("join", "join_rows_v2", "Join", "combine", 480, 20, new JsonObject())
            ],
            [
                new CanvasWorkflowEdgeState("load_a", "columnar"),
                new CanvasWorkflowEdgeState("columnar", "join"),
                new CanvasWorkflowEdgeState("load_b", "join")
            ],
            new CanvasWorkflowViewportState(1.2, 10, 20),
            new CanvasWorkflowSelectionState(["columnar"]));

        Assert.NotNull(document);
        Assert.Equal("wf_canvas", document!.WorkflowId);
        Assert.Equal("1.0.0", document.Version);

        var columnar = document.Nodes.Single(node => node.Id == "columnar");
        Assert.Equal("load_a", columnar.Config["input_map"]?["rows"]?["from"]?.GetValue<string>());
        Assert.Equal("rows", columnar.Config["input_map"]?["rows"]?["path"]?.GetValue<string>());

        var join = document.Nodes.Single(node => node.Id == "join");
        Assert.Equal("columnar", join.Config["input_map"]?["left_rows"]?["from"]?.GetValue<string>());
        Assert.Equal("load_b", join.Config["input_map"]?["right_rows"]?["from"]?.GetValue<string>());
    }

    [Fact]
    public void SerializeWorkflowDefinition_ProducesCanonicalWorkflowObject()
    {
        var document = new WorkflowGraphDocument(
            "wf_serialized",
            "1.0.0",
            [
                new WorkflowGraphNodeDocument(
                    "load_1",
                    "load_rows_v3",
                    "Load",
                    "subtitle",
                    80,
                    120,
                    new JsonObject
                    {
                        ["source_type"] = "sqlite",
                        ["source"] = "D:/demo.db",
                        ["query"] = "select * from x"
                    })
            ],
            [
                new WorkflowGraphEdgeDocument("load_1", "load_1")
            ],
            WorkflowGraphViewportDocument.Default,
            WorkflowGraphSelectionDocument.Empty);

        var payload = WorkflowCanvasDocumentBuilder.SerializeWorkflowDefinition(document);

        Assert.Equal("wf_serialized", payload["workflow_id"]?.GetValue<string>());
        Assert.Equal("1.0.0", payload["version"]?.GetValue<string>());
        Assert.Single(payload["nodes"]!.AsArray());
        Assert.Single(payload["edges"]!.AsArray());
        Assert.Equal("load_rows_v3", payload["nodes"]![0]!["type"]?.GetValue<string>());
    }

    [Fact]
    public void ImportWorkflowDefinition_MapsWorkflowDefinitionToCanvasDocument()
    {
        var document = WorkflowCanvasDocumentBuilder.ImportWorkflowDefinition(new JsonObject
        {
            ["workflow_id"] = "wf_template",
            ["version"] = "1.0.0",
            ["nodes"] = new JsonArray
            {
                new JsonObject
                {
                    ["id"] = "n1",
                    ["type"] = "ingest_files",
                    ["x"] = 40,
                    ["y"] = 80,
                    ["config"] = new JsonObject()
                },
                new JsonObject
                {
                    ["id"] = "n2",
                    ["type"] = "clean_md",
                    ["title"] = "Clean",
                    ["subtitle"] = "Step",
                    ["x"] = 160,
                    ["y"] = 120,
                    ["config"] = new JsonObject { ["mode"] = "strict" }
                }
            },
            ["edges"] = new JsonArray
            {
                new JsonObject { ["from"] = "n1", ["to"] = "n2" }
            }
        });

        Assert.Equal("wf_template", document.WorkflowId);
        Assert.Equal("1.0.0", document.Version);
        Assert.Equal("ingest_files", document.Nodes[0].Title);
        Assert.Equal(string.Empty, document.Nodes[0].Subtitle);
        Assert.Equal("Clean", document.Nodes[1].Title);
        Assert.Equal("Step", document.Nodes[1].Subtitle);
        Assert.Single(document.Edges);
        Assert.Equal(WorkflowGraphViewportDocument.Default, document.Viewport);
        Assert.Empty(document.Selection.NodeIds);
    }
}
