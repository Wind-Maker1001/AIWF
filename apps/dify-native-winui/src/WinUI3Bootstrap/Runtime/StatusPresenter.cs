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
            ToneSuccess => Color.FromArgb(0xFF, 0x11, 0x11, 0x11),
            ToneError => Color.FromArgb(0xFF, 0xC6, 0x28, 0x28),
            ToneBusy => Color.FromArgb(0xFF, 0x6B, 0x72, 0x80),
            _ => Color.FromArgb(0xFF, 0x4B, 0x55, 0x63)
        };
    }

    public static string InferTone(string text)
    {
        var value = text ?? string.Empty;
        if (value.Contains("failed", StringComparison.OrdinalIgnoreCase)
            || value.Contains("error", StringComparison.OrdinalIgnoreCase)
            || value.Contains("invalid", StringComparison.OrdinalIgnoreCase)
            || value.Contains("required", StringComparison.OrdinalIgnoreCase))
        {
            return ToneError;
        }

        if (value.Contains("completed", StringComparison.OrdinalIgnoreCase)
            || value.Contains("passed", StringComparison.OrdinalIgnoreCase)
            || value.Contains("success", StringComparison.OrdinalIgnoreCase))
        {
            return ToneSuccess;
        }

        if (value.Contains("checking", StringComparison.OrdinalIgnoreCase)
            || value.Contains("submitting", StringComparison.OrdinalIgnoreCase)
            || value.Contains("preparing", StringComparison.OrdinalIgnoreCase))
        {
            return ToneBusy;
        }

        return ToneNeutral;
    }
}
