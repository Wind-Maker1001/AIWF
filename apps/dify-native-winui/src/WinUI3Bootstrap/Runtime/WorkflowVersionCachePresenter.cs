namespace AIWF.Native.Runtime;

public static class WorkflowVersionCachePresenter
{
    public static string BuildStatsText(WorkflowVersionCacheStats stats)
    {
        return $"entries={stats.Entries} | hits={stats.Hits} | misses={stats.Misses} | sets={stats.Sets} | hit_rate={stats.HitRate:0.###} | last_reset_at={stats.LastResetAt}";
    }

    public static string BuildClearStatusText(bool ok, string? error = null)
    {
        return ok
            ? "Workflow version cache cleared."
            : $"Workflow version cache clear failed: {error}";
    }
}
