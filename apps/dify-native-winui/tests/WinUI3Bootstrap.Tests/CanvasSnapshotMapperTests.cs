using AIWF.Native.CanvasRuntime;
using Xunit;

namespace AIWF.Native.Tests;

public sealed class CanvasSnapshotMapperTests
{
    [Fact]
    public void CreateSnapshot_FiltersInvalidAndDuplicateEdges()
    {
        var snapshot = CanvasSnapshotMapper.CreateSnapshot(
            canvasWidth: 3200,
            canvasHeight: 2200,
            viewScale: 3.1,
            viewTranslateX: -100,
            viewTranslateY: -40,
            nodes:
            [
                new CanvasNodeState("input", "Input", "源数据", 0, 0, false, false, null, null),
                new CanvasNodeState("clean", "Clean", "处理", 240, 120, true, false, null, null),
                new CanvasNodeState("clean", "Duplicate", "重复", 480, 240, true, false, null, null),
                new CanvasNodeState("", "Invalid", "", 0, 0, false, false, null, null)
            ],
            edges:
            [
                new CanvasEdgeState("input", "clean"),
                new CanvasEdgeState("input", "clean"),
                new CanvasEdgeState("missing", "clean")
            ],
            minScale: 0.6,
            maxScale: 2.4);

        Assert.Equal(2.4, snapshot.ViewScale);
        Assert.Collection(
            snapshot.Nodes,
            node => Assert.Equal("input", node.NodeKey),
            node => Assert.Equal("clean", node.NodeKey));
        var edge = Assert.Single(snapshot.Edges);
        Assert.Equal("input", edge.SourceKey);
        Assert.Equal("clean", edge.TargetKey);
    }

    [Fact]
    public void CreateRestorePlan_NormalizesExtentAndKeepsArtifactMetadata()
    {
        var plan = CanvasSnapshotMapper.CreateRestorePlan(
            new CanvasSnapshot
            {
                CanvasWidth = 500,
                CanvasHeight = 400,
                ViewScale = 9,
                ViewTranslateX = -200,
                ViewTranslateY = -100,
                Nodes =
                [
                    new CanvasNodeSnapshot
                    {
                        NodeKey = "artifact-1",
                        Title = "Excel",
                        Subtitle = "report.xlsx",
                        X = -40,
                        Y = 960,
                        IsArtifactNode = true,
                        ArtifactPath = "C:/tmp/report.xlsx",
                        ArtifactKind = "xlsx"
                    },
                    new CanvasNodeSnapshot
                    {
                        NodeKey = "custom-1",
                        Title = "Node 1",
                        Subtitle = "desc",
                        X = 120,
                        Y = 180,
                        IsUserNode = true
                    }
                ],
                Edges =
                [
                    new CanvasEdgeSnapshot { SourceKey = "custom-1", TargetKey = "artifact-1" },
                    new CanvasEdgeSnapshot { SourceKey = "missing", TargetKey = "artifact-1" }
                ]
            },
            defaultCanvasWidth: 3200,
            defaultCanvasHeight: 2200,
            gridSize: 20,
            minScale: 0.6,
            maxScale: 2.4);

        Assert.Equal(2.4, plan.ViewScale);
        Assert.True(plan.CanvasHeight >= 2200);
        Assert.Equal(2, plan.Nodes.Count);
        Assert.Equal(2, plan.NextCustomNodeCounter);
        var artifactNode = Assert.Single(plan.Nodes, static x => x.IsArtifactNode);
        Assert.Equal(0, artifactNode.X);
        Assert.Equal("xlsx", artifactNode.ArtifactKind);
        Assert.Equal("C:/tmp/report.xlsx", artifactNode.ArtifactPath);
        var edge = Assert.Single(plan.Edges);
        Assert.Equal("custom-1", edge.SourceKey);
        Assert.Equal("artifact-1", edge.TargetKey);
    }
}
