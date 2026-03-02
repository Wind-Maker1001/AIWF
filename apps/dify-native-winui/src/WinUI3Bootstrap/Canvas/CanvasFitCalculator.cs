namespace AIWF.Native.CanvasRuntime;

public readonly record struct CanvasFitResult(double Scale, double TranslateX, double TranslateY);

public static class CanvasFitCalculator
{
    public static CanvasFitResult Calculate(
        double minLeft,
        double minTop,
        double maxRight,
        double maxBottom,
        double viewportWidth,
        double viewportHeight,
        double minScale,
        double maxScale,
        double padding)
    {
        var contentWidth = Math.Max(1.0, maxRight - minLeft);
        var contentHeight = Math.Max(1.0, maxBottom - minTop);
        var availableWidth = Math.Max(80.0, viewportWidth - (padding * 2));
        var availableHeight = Math.Max(80.0, viewportHeight - (padding * 2));
        var scaleX = availableWidth / contentWidth;
        var scaleY = availableHeight / contentHeight;
        var targetScale = Math.Clamp(Math.Min(scaleX, scaleY), minScale, maxScale);
        var centerX = minLeft + (contentWidth * 0.5);
        var centerY = minTop + (contentHeight * 0.5);
        var translateX = (viewportWidth * 0.5) - (centerX * targetScale);
        var translateY = (viewportHeight * 0.5) - (centerY * targetScale);
        return new CanvasFitResult(targetScale, translateX, translateY);
    }
}
