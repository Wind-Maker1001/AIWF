using Windows.UI;

namespace AIWF.Native.Runtime;

public static class StatusPresenter
{
    public const string ToneNeutral = "neutral";
    public const string ToneBusy = "busy";
    public const string ToneSuccess = "success";
    public const string ToneError = "error";

    public static string NormalizeMessage(string? message, string defaultText)
    {
        var text = (message ?? string.Empty).Trim();
        return string.IsNullOrWhiteSpace(text) ? defaultText : text;
    }

    public static Color ResolveForeground(string tone)
    {
        return tone switch
        {
            ToneSuccess => Color.FromArgb(0xFF, 0x11, 0x13, 0x17),
            ToneError => Color.FromArgb(0xFF, 0xD7, 0x26, 0x2E),
            ToneBusy => Color.FromArgb(0xFF, 0x25, 0x2A, 0x31),
            _ => Color.FromArgb(0xFF, 0x59, 0x60, 0x6B)
        };
    }

    public static string InferTone(string text)
    {
        var value = text ?? string.Empty;
        if (value.Contains("失败") || value.Contains("异常") || value.Contains("请填写"))
        {
            return ToneError;
        }

        if (value.Contains("完成") || value.Contains("通过"))
        {
            return ToneSuccess;
        }

        if (value.Contains("正在"))
        {
            return ToneBusy;
        }

        return ToneNeutral;
    }
}
