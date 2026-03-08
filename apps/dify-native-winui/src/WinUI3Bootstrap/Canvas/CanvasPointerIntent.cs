namespace AIWF.Native.CanvasRuntime;

internal static class CanvasPointerIntent
{
    public static bool ShouldStartPrimaryCanvasAction(
        string? deviceTypeName,
        bool isLeftButtonPressed,
        bool isMiddleButtonPressed,
        bool isRightButtonPressed)
    {
        if (string.Equals(deviceTypeName, "Mouse", StringComparison.OrdinalIgnoreCase))
        {
            return isLeftButtonPressed && !isMiddleButtonPressed && !isRightButtonPressed;
        }

        return !isMiddleButtonPressed && !isRightButtonPressed;
    }
}
