namespace AIWF.Native.CanvasRuntime;

internal sealed record CanvasSelectionDelta<T>(
    IReadOnlyList<T> Activated,
    IReadOnlyList<T> Deactivated);

internal static class CanvasSelectionDiff
{
    public static CanvasSelectionDelta<T> Calculate<T>(
        IEnumerable<T> previousSelection,
        IEnumerable<T> nextSelection,
        IEqualityComparer<T>? comparer = null)
        where T : notnull
    {
        var previous = previousSelection as HashSet<T> ?? new HashSet<T>(previousSelection, comparer);
        var next = nextSelection as HashSet<T> ?? new HashSet<T>(nextSelection, comparer);

        var activated = next.Where(item => !previous.Contains(item)).ToArray();
        var deactivated = previous.Where(item => !next.Contains(item)).ToArray();
        return new CanvasSelectionDelta<T>(activated, deactivated);
    }
}
