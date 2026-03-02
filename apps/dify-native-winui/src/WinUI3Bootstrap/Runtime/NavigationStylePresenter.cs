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
                Color.FromArgb(0xFF, 0xC6, 0x28, 0x28),
                Color.FromArgb(0xFF, 0xFF, 0xFF, 0xFF),
                Color.FromArgb(0xFF, 0xC6, 0x28, 0x28))
            : new ButtonVisual(
                false,
                Color.FromArgb(0x00, 0x00, 0x00, 0x00),
                Color.FromArgb(0xFF, 0x11, 0x11, 0x11),
                Color.FromArgb(0x33, 0x22, 0x22, 0x22));
    }

    public static (ButtonVisual RunButton, ButtonVisual HealthButton) CommandButtons()
    {
        var run = NavButton(true);
        var health = NavButton(false);
        return (run, health);
    }
}
