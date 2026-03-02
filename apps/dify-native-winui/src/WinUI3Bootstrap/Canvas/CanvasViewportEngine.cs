namespace AIWF.Native.CanvasRuntime;

public readonly record struct CanvasViewportState(double Scale, double TranslateX, double TranslateY);

public sealed class CanvasViewportEngine
{
    private readonly double _minScale;
    private readonly double _maxScale;

    public CanvasViewportEngine(double minScale, double maxScale)
    {
        _minScale = minScale;
        _maxScale = maxScale;
    }

    public CanvasViewportState ApplyScale(CanvasViewportState state, double centerX, double centerY, double scaleFactor)
    {
        var oldScale = state.Scale <= 0 ? _minScale : state.Scale;
        var newScale = Math.Clamp(oldScale * scaleFactor, _minScale, _maxScale);
        if (Math.Abs(newScale - oldScale) < 0.0001)
        {
            return state;
        }

        var contentX = (centerX - state.TranslateX) / oldScale;
        var contentY = (centerY - state.TranslateY) / oldScale;
        var nextTranslateX = centerX - (contentX * newScale);
        var nextTranslateY = centerY - (contentY * newScale);
        return new CanvasViewportState(newScale, nextTranslateX, nextTranslateY);
    }

    public CanvasViewportState ClampTranslation(
        CanvasViewportState state,
        double canvasWidth,
        double canvasHeight,
        double viewportWidth,
        double viewportHeight)
    {
        var scale = Math.Max(state.Scale, 0.001);
        var scaledWidth = canvasWidth * scale;
        var scaledHeight = canvasHeight * scale;
        var minX = Math.Min(0, viewportWidth - scaledWidth);
        var minY = Math.Min(0, viewportHeight - scaledHeight);
        var tx = Math.Clamp(state.TranslateX, minX, 0);
        var ty = Math.Clamp(state.TranslateY, minY, 0);
        return new CanvasViewportState(scale, tx, ty);
    }

    public CanvasViewportState PanAndClamp(
        double panStartX,
        double panStartY,
        double currentX,
        double currentY,
        double panStartTranslateX,
        double panStartTranslateY,
        double scale,
        double canvasWidth,
        double canvasHeight,
        double viewportWidth,
        double viewportHeight)
    {
        var nextX = panStartTranslateX + (currentX - panStartX);
        var nextY = panStartTranslateY + (currentY - panStartY);
        return ClampTranslation(
            new CanvasViewportState(scale, nextX, nextY),
            canvasWidth,
            canvasHeight,
            viewportWidth,
            viewportHeight);
    }

    public CanvasViewportState TranslateAndClamp(
        CanvasViewportState state,
        double deltaX,
        double deltaY,
        double canvasWidth,
        double canvasHeight,
        double viewportWidth,
        double viewportHeight)
    {
        var next = new CanvasViewportState(state.Scale, state.TranslateX + deltaX, state.TranslateY + deltaY);
        return ClampTranslation(next, canvasWidth, canvasHeight, viewportWidth, viewportHeight);
    }
}
