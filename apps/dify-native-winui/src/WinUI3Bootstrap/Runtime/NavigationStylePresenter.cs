using Windows.UI;

namespace AIWF.Native.Runtime;

public sealed record ButtonVisual(
    bool IsActive,
    Color Background,
    Color Foreground,
    Color Border,
    double BorderThickness = 1,
    double CornerRadius = 8);

public static class NavigationStylePresenter
{
    public static ButtonVisual NavButton(bool active)
    {
        return active
            ? new ButtonVisual(
                true,
                Color.FromArgb(0xFF, 0xD7, 0x26, 0x2E),
                Color.FromArgb(0xFF, 0xFF, 0xFF, 0xFF),
                Color.FromArgb(0xFF, 0x8E, 0x12, 0x18))
            : new ButtonVisual(
                false,
                Color.FromArgb(0xFF, 0x25, 0x2A, 0x31),
                Color.FromArgb(0xFF, 0xF5, 0xF6, 0xF7),
                Color.FromArgb(0xFF, 0x3F, 0x46, 0x50));
    }

    public static (ButtonVisual RunButton, ButtonVisual HealthButton) CommandButtons()
    {
        var run = NavButton(true);
        var health = new ButtonVisual(
            false,
            Color.FromArgb(0xFF, 0xF5, 0xF5, 0xF6),
            Color.FromArgb(0xFF, 0x11, 0x13, 0x17),
            Color.FromArgb(0xFF, 0x3F, 0x46, 0x50));
        return (run, health);
    }
}
