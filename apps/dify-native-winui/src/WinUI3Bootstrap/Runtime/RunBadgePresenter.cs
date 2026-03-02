using Windows.UI;

namespace AIWF.Native.Runtime;

public sealed record BadgeVisual(
    string Text,
    Color Foreground,
    Color Border,
    Color Background);

public static class RunBadgePresenter
{
    public static BadgeVisual Resolve(bool? ok)
    {
        return RunVisualStateMapper.MapBadge(ok) switch
        {
            RunBadgeState.Success => new BadgeVisual(
                "成功",
                Color.FromArgb(0xFF, 0x11, 0x11, 0x11),
                Color.FromArgb(0x66, 0x11, 0x11, 0x11),
                Color.FromArgb(0x33, 0xE5, 0xE7, 0xEB)),
            RunBadgeState.Failed => new BadgeVisual(
                "失败",
                Color.FromArgb(0xFF, 0xC6, 0x28, 0x28),
                Color.FromArgb(0x66, 0xC6, 0x28, 0x28),
                Color.FromArgb(0x33, 0xFE, 0xE2, 0xE2)),
            _ => new BadgeVisual(
                "待运行",
                Color.FromArgb(0xFF, 0x6B, 0x72, 0x80),
                Color.FromArgb(0x66, 0x6B, 0x72, 0x80),
                Color.FromArgb(0x33, 0xE5, 0xE7, 0xEB))
        };
    }
}
