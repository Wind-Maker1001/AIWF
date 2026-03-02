using Windows.Foundation;

namespace AIWF.Native.CanvasRuntime;

public readonly record struct NodePosition(double Left, double Top);

public static class NodeDragMath
{
    public static NodePosition ComputeDragPosition(
        double startLeft,
        double startTop,
        Point dragStartPoint,
        Point currentPoint,
        double minLeft = 0,
        double minTop = 0)
    {
        var dx = currentPoint.X - dragStartPoint.X;
        var dy = currentPoint.Y - dragStartPoint.Y;
        return new NodePosition(
            Math.Max(minLeft, startLeft + dx),
            Math.Max(minTop, startTop + dy));
    }

    public static NodePosition SnapToGrid(NodePosition pos, double gridSize, double minLeft = 0, double minTop = 0)
    {
        static double Snap(double value, double step) => Math.Round(value / step) * step;
        return new NodePosition(
            Math.Max(minLeft, Snap(pos.Left, gridSize)),
            Math.Max(minTop, Snap(pos.Top, gridSize)));
    }
}
