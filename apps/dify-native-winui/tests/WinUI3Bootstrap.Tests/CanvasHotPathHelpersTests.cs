using AIWF.Native.CanvasRuntime;
using Xunit;

namespace AIWF.Native.Tests;

public sealed class CanvasHotPathHelpersTests
{
    [Fact]
    public void SelectionDiff_ReturnsOnlyIncrementalChanges()
    {
        var delta = CanvasSelectionDiff.Calculate(
            previousSelection: ["a", "b"],
            nextSelection: ["b", "c"]);

        Assert.Equal(["c"], delta.Activated);
        Assert.Equal(["a"], delta.Deactivated);
    }

    [Fact]
    public void ConnectionIndex_ReturnsEdgesOnlyForTouchedNode()
    {
        var index = new CanvasConnectionIndex<string, string>();
        index.Add("input", "clean", "e1");
        index.Add("clean", "output", "e2");

        Assert.Equal(["e1"], index.Get("input"));
        Assert.Equal(2, index.Get("clean").Count);
        Assert.Empty(index.Get("missing"));

        index.Remove("input", "clean", "e1");
        Assert.Empty(index.Get("input"));
        Assert.Equal(["e2"], index.Get("clean"));
    }

    [Theory]
    [InlineData("Mouse", true, false, false, true)]
    [InlineData("Mouse", false, false, true, false)]
    [InlineData("Mouse", true, true, false, false)]
    [InlineData("Touch", false, false, false, true)]
    [InlineData("Pen", true, false, false, true)]
    [InlineData("Pen", false, false, true, false)]
    public void PointerIntent_AllowsOnlyPrimaryCanvasAction(
        string deviceTypeName,
        bool isLeftPressed,
        bool isMiddlePressed,
        bool isRightPressed,
        bool expected)
    {
        var allowed = CanvasPointerIntent.ShouldStartPrimaryCanvasAction(
            deviceTypeName,
            isLeftPressed,
            isMiddlePressed,
            isRightPressed);

        Assert.Equal(expected, allowed);
    }

    [Theory]
    [InlineData(null, "{}", true, true)]
    [InlineData("{}", "{}", true, false)]
    [InlineData("{}", "{\"x\":1}", true, true)]
    [InlineData("{}", "{}", false, true)]
    public void SnapshotWriteDecider_WritesOnlyWhenNeeded(string? previous, string next, bool fileExists, bool expected)
    {
        var shouldWrite = CanvasSnapshotWriteDecider.ShouldWrite(previous, next, fileExists);

        Assert.Equal(expected, shouldWrite);
    }
}
