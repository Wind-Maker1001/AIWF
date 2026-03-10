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
                Color.FromArgb(0xFF, 0xF8, 0xF9, 0xFA),
                Color.FromArgb(0xFF, 0x17, 0x1A, 0x1F),
                Color.FromArgb(0xFF, 0x17, 0x1A, 0x1F)),
            RunBadgeState.Failed => new BadgeVisual(
                "失败",
                Color.FromArgb(0xFF, 0xFF, 0xFF, 0xFF),
                Color.FromArgb(0xFF, 0x8E, 0x12, 0x18),
                Color.FromArgb(0xFF, 0xD7, 0x26, 0x2E)),
            _ => new BadgeVisual(
                "待运行",
                Color.FromArgb(0xFF, 0xF5, 0xF6, 0xF7),
                Color.FromArgb(0xFF, 0x43, 0x48, 0x50),
                Color.FromArgb(0xFF, 0x2A, 0x2F, 0x37))
        };
    }
}
