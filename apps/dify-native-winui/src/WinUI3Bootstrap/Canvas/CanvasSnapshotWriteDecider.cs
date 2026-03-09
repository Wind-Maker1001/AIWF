namespace AIWF.Native.CanvasRuntime;

internal static class CanvasSnapshotWriteDecider
{
    public static bool ShouldWrite(string? previousSnapshotJson, string nextSnapshotJson, bool fileExists)
    {
        return !fileExists || !string.Equals(previousSnapshotJson, nextSnapshotJson, StringComparison.Ordinal);
    }
}
