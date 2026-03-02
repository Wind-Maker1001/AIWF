using Windows.UI;

namespace AIWF.Native.Runtime;

public static class InputFieldPresenter
{
    public static Color ResolveBorderColor(bool hasError)
    {
        return hasError
            ? Color.FromArgb(0xFF, 0xC6, 0x28, 0x28)
            : Color.FromArgb(0x66, 0x55, 0x55, 0x55);
    }
}
