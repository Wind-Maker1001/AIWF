using System.Text.Json;
using AIWF.Native.CanvasRuntime;
using Xunit;

namespace AIWF.Native.Tests;

public sealed class CanvasSnapshotModelsTests
{
    [Fact]
    public void CanvasSnapshot_RoundTripsArtifactMetadata()
    {
        var snapshot = new CanvasSnapshot
        {
            CanvasWidth = 3200,
            CanvasHeight = 2200,
            ViewScale = 1.2,
            ViewTranslateX = -160,
            ViewTranslateY = -80,
            Nodes =
            [
                new CanvasNodeSnapshot
                {
                    NodeKey = "artifact-1",
                    Title = "Excel",
                    Subtitle = "report.xlsx",
                    X = 1080,
                    Y = 180,
                    IsUserNode = false,
                    IsArtifactNode = true,
                    ArtifactPath = "C:/tmp/report.xlsx",
                    ArtifactKind = "xlsx"
                }
            ],
            Edges =
            [
                new CanvasEdgeSnapshot
                {
                    SourceKey = "clean",
                    TargetKey = "artifact-1"
                }
            ]
        };

        var json = JsonSerializer.Serialize(snapshot);
        var roundTrip = JsonSerializer.Deserialize<CanvasSnapshot>(json);

        Assert.NotNull(roundTrip);
        Assert.Equal(3200, roundTrip!.CanvasWidth);
        var node = Assert.Single(roundTrip.Nodes);
        Assert.True(node.IsArtifactNode);
        Assert.Equal("xlsx", node.ArtifactKind);
        Assert.Equal("C:/tmp/report.xlsx", node.ArtifactPath);
        var edge = Assert.Single(roundTrip.Edges);
        Assert.Equal("artifact-1", edge.TargetKey);
    }
}
