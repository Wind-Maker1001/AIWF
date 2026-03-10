using Windows.UI;

namespace AIWF.Native.Runtime;

public static class InputFieldPresenter
{
    public static Color ResolveBorderColor(bool hasError)
    {
        return hasError
            ? Color.FromArgb(0xFF, 0xD7, 0x26, 0x2E)
            : Color.FromArgb(0x99, 0x34, 0x3A, 0x42);
    }
}
