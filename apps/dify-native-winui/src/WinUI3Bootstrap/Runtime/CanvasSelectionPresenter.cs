using Windows.UI;

namespace AIWF.Native.Runtime;

public sealed record NodeSelectionVisual(double BorderThickness, Color BorderColor);
public sealed record ConnectionSelectionVisual(double StrokeThickness, Color StrokeColor);

public static class CanvasSelectionPresenter
{
    public static NodeSelectionVisual ResolveNode(bool active)
    {
        return active
            ? new NodeSelectionVisual(2, Color.FromArgb(0xFF, 0xC6, 0x28, 0x28))
            : new NodeSelectionVisual(1, Color.FromArgb(0x66, 0xC6, 0x28, 0x28));
    }

    public static ConnectionSelectionVisual ResolveConnection(bool active)
    {
        return active
            ? new ConnectionSelectionVisual(3, Color.FromArgb(0xFF, 0xC6, 0x28, 0x28))
            : new ConnectionSelectionVisual(2, Color.FromArgb(0xCC, 0x11, 0x11, 0x11));
    }
}
