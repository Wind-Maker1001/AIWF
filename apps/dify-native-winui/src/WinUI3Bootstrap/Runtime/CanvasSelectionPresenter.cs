using Windows.UI;

namespace AIWF.Native.Runtime;

public sealed record NodeSelectionVisual(double BorderThickness, Color BorderColor);
public sealed record ConnectionSelectionVisual(double StrokeThickness, Color StrokeColor);

public static class CanvasSelectionPresenter
{
    public static NodeSelectionVisual ResolveNode(bool active)
    {
        return active
            ? new NodeSelectionVisual(1.5, Color.FromArgb(0xCC, 0xB5, 0x1C, 0x23))
            : new NodeSelectionVisual(1, Color.FromArgb(0x66, 0x30, 0x36, 0x40));
    }

    public static ConnectionSelectionVisual ResolveConnection(bool active)
    {
        return active
            ? new ConnectionSelectionVisual(2.5, Color.FromArgb(0xCC, 0xB5, 0x1C, 0x23))
            : new ConnectionSelectionVisual(2.25, Color.FromArgb(0xCC, 0x34, 0x3A, 0x42));
    }
}
