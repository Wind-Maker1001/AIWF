using System.Text.Json;
using AIWF.Native.CanvasRuntime;
using AIWF.Native.Runtime;
using Xunit;

namespace AIWF.Native.Tests;

public sealed class CanvasAuthoringPersistenceServiceTests
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true
    };

    [Fact]
    public void SaveSnapshotAndLoadSnapshot_RoundTripCanvasState()
    {
        var root = Path.Combine(Path.GetTempPath(), $"aiwf-canvas-persist-{Guid.NewGuid():N}");
        Directory.CreateDirectory(root);
        try
        {
            var service = new CanvasAuthoringPersistenceService(
                Path.Combine(root, "canvas-workflow.json"),
                Path.Combine(root, "canvas-workflow.graph.json"));
            var snapshot = new CanvasSnapshot
            {
                CanvasWidth = 3200,
                CanvasHeight = 2200,
                ViewScale = 1.1,
                ViewTranslateX = 12,
                ViewTranslateY = 24,
                Nodes =
                [
                    new CanvasNodeSnapshot
                    {
                        NodeKey = "node_1",
                        Title = "Node 1",
                        Subtitle = "subtitle",
                        X = 120,
                        Y = 160,
                        IsUserNode = true
                    }
                ],
                Edges =
                [
                    new CanvasEdgeSnapshot
                    {
                        SourceKey = "node_1",
                        TargetKey = "node_1"
                    }
                ]
            };

            var firstWrite = service.SaveSnapshot(snapshot, JsonOptions, previousJson: null);
            var loaded = service.LoadSnapshot(JsonOptions);

            Assert.True(firstWrite);
            Assert.True(loaded.Exists);
            Assert.NotNull(loaded.Json);
            Assert.NotNull(loaded.Snapshot);
            Assert.Equal(3200, loaded.Snapshot!.CanvasWidth);
            Assert.Single(loaded.Snapshot.Nodes);
            Assert.Equal("node_1", loaded.Snapshot.Nodes[0].NodeKey);
        }
        finally
        {
            if (Directory.Exists(root))
            {
                Directory.Delete(root, recursive: true);
            }
        }
    }

    [Fact]
    public void SaveSnapshot_RespectsWriteDeciderForUnchangedContent()
    {
        var root = Path.Combine(Path.GetTempPath(), $"aiwf-canvas-persist-{Guid.NewGuid():N}");
        Directory.CreateDirectory(root);
        try
        {
            var service = new CanvasAuthoringPersistenceService(
                Path.Combine(root, "canvas-workflow.json"),
                Path.Combine(root, "canvas-workflow.graph.json"));
            var snapshot = new CanvasSnapshot
            {
                CanvasWidth = 100,
                CanvasHeight = 100
            };

            Assert.True(service.SaveSnapshot(snapshot, JsonOptions, previousJson: null));
            var previousJson = service.LoadSnapshot(JsonOptions).Json;
            Assert.NotNull(previousJson);
            Assert.False(service.SaveSnapshot(snapshot, JsonOptions, previousJson));
        }
        finally
        {
            if (Directory.Exists(root))
            {
                Directory.Delete(root, recursive: true);
            }
        }
    }

    [Fact]
    public void SaveWorkflowGraph_RoundTripsAndDeletesSidecar()
    {
        var root = Path.Combine(Path.GetTempPath(), $"aiwf-canvas-persist-{Guid.NewGuid():N}");
        Directory.CreateDirectory(root);
        try
        {
            var graphPath = Path.Combine(root, "canvas-workflow.graph.json");
            var service = new CanvasAuthoringPersistenceService(
                Path.Combine(root, "canvas-workflow.json"),
                graphPath);
            var document = new WorkflowGraphDocument(
                "wf_sidecar",
                "1.0.0",
                [
                    new WorkflowGraphNodeDocument(
                        "load_1",
                        "load_rows_v3",
                        "Load",
                        "source",
                        10,
                        20,
                        new System.Text.Json.Nodes.JsonObject
                        {
                            ["source_type"] = "sqlite",
                            ["source"] = "D:/demo.db",
                            ["query"] = "select * from data"
                        })
                ],
                [],
                WorkflowGraphViewportDocument.Default,
                WorkflowGraphSelectionDocument.Empty);

            service.SaveWorkflowGraph(document, JsonOptions);
            var loaded = service.LoadWorkflowGraph(JsonOptions);
            Assert.NotNull(loaded);
            Assert.Equal("wf_sidecar", loaded!.WorkflowId);
            Assert.True(File.Exists(graphPath));

            service.SaveWorkflowGraph(null, JsonOptions);
            Assert.False(File.Exists(graphPath));
            Assert.Null(service.LoadWorkflowGraph(JsonOptions));
        }
        finally
        {
            if (Directory.Exists(root))
            {
                Directory.Delete(root, recursive: true);
            }
        }
    }
}
