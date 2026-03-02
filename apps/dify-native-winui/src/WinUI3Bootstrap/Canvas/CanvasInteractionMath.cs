using Windows.Foundation;

namespace AIWF.Native.CanvasRuntime;

public readonly record struct MarqueeBox(double Left, double Top, double Width, double Height);

public static class CanvasInteractionMath
{
    public static MarqueeBox ComputeMarqueeBox(Point start, Point current)
    {
        var left = Math.Min(start.X, current.X);
        var top = Math.Min(start.Y, current.Y);
        var width = Math.Abs(current.X - start.X);
        var height = Math.Abs(current.Y - start.Y);
        return new MarqueeBox(left, top, width, height);
    }

    public static Rect ToCanvasRect(MarqueeBox box, double translateX, double translateY, double scale)
    {
        var safeScale = Math.Max(scale, 0.001);
        var contentLeft = (box.Left - translateX) / safeScale;
        var contentTop = (box.Top - translateY) / safeScale;
        var contentRight = ((box.Left + box.Width) - translateX) / safeScale;
        var contentBottom = ((box.Top + box.Height) - translateY) / safeScale;
        return new Rect(
            contentLeft,
            contentTop,
            Math.Max(0, contentRight - contentLeft),
            Math.Max(0, contentBottom - contentTop));
    }

    public static bool Intersects(Rect a, Rect b)
    {
        var aRight = a.X + a.Width;
        var aBottom = a.Y + a.Height;
        var bRight = b.X + b.Width;
        var bBottom = b.Y + b.Height;
        return a.X < bRight && aRight > b.X && a.Y < bBottom && aBottom > b.Y;
    }
}
