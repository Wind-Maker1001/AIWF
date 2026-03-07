using AIWF.Native.CanvasRuntime;
using Windows.Foundation;
using Xunit;

namespace AIWF.Native.Tests;

public sealed class CanvasInteractionMathTests
{
    [Fact]
    public void ComputeMarqueeBox_NormalizesDraggedCorners()
    {
        var box = CanvasInteractionMath.ComputeMarqueeBox(
            new Point(180, 120),
            new Point(60, 30));

        Assert.Equal(60, box.Left);
        Assert.Equal(30, box.Top);
        Assert.Equal(120, box.Width);
        Assert.Equal(90, box.Height);
    }

    [Fact]
    public void ToCanvasRect_AppliesViewportTransform()
    {
        var rect = CanvasInteractionMath.ToCanvasRect(
            new MarqueeBox(110, 70, 80, 40),
            translateX: 10,
            translateY: 30,
            scale: 2);

        Assert.Equal(50, rect.X, 6);
        Assert.Equal(20, rect.Y, 6);
        Assert.Equal(40, rect.Width, 6);
        Assert.Equal(20, rect.Height, 6);
    }

    [Fact]
    public void Intersects_ExcludesTouchingEdgesButIncludesOverlap()
    {
        var overlapping = CanvasInteractionMath.Intersects(
            new Rect(0, 0, 100, 100),
            new Rect(80, 80, 40, 40));
        var touchingOnly = CanvasInteractionMath.Intersects(
            new Rect(0, 0, 100, 100),
            new Rect(100, 0, 20, 20));

        Assert.True(overlapping);
        Assert.False(touchingOnly);
    }
}
