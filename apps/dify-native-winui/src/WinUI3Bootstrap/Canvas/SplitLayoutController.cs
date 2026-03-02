namespace AIWF.Native.CanvasRuntime;

public readonly record struct SplitColumnsResult(bool IsValid, double Left, double Right);
public readonly record struct SplitRowsResult(bool IsValid, double Top, double Bottom);

public static class SplitLayoutController
{
    public static SplitColumnsResult CalculateColumns(
        double total,
        double startLeft,
        double delta,
        double minLeft,
        double minRight)
    {
        var maxLeft = total - minRight;
        if (maxLeft < minLeft)
        {
            maxLeft = minLeft;
        }

        var left = Math.Clamp(startLeft + delta, minLeft, maxLeft);
        var right = Math.Max(minRight, total - left);
        if (left <= 0 || right <= 0)
        {
            return new SplitColumnsResult(false, 0, 0);
        }

        return new SplitColumnsResult(true, left, right);
    }

    public static SplitRowsResult CalculateRows(
        double total,
        double startTop,
        double delta,
        double minTop,
        double minBottom)
    {
        var maxTop = total - minBottom;
        if (maxTop < minTop)
        {
            maxTop = minTop;
        }

        var top = Math.Clamp(startTop + delta, minTop, maxTop);
        var bottom = Math.Max(minBottom, total - top);
        if (top <= 0 || bottom <= 0)
        {
            return new SplitRowsResult(false, 0, 0);
        }

        return new SplitRowsResult(true, top, bottom);
    }
}
