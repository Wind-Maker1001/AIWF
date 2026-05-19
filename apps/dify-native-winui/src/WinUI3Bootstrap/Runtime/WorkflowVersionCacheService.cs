using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace AIWF.Native.Runtime;

public sealed record WorkflowVersionCacheStats(
    int Entries,
    int Hits,
    int Misses,
    int Sets,
    double HitRate,
    string LastResetAt);

internal sealed class WorkflowVersionCacheService
{
    private sealed class WorkflowVersionCacheStore
    {
        public string SchemaVersion { get; set; } = "workflow_version_cache.v1";
        public List<GovernanceWorkflowVersionItem> VersionList { get; set; } = [];
        public Dictionary<string, GovernanceWorkflowVersionCompareResult> VersionCompare { get; set; } = new(StringComparer.Ordinal);
    }

    private sealed class WorkflowVersionCacheMetrics
    {
        public string SchemaVersion { get; set; } = "workflow_version_cache_metrics.v1";
        public int Hits { get; set; }
        public int Misses { get; set; }
        public int Sets { get; set; }
        public string LastResetAt { get; set; } = string.Empty;
    }

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    private readonly string _cachePath;
    private readonly string _metricsPath;
    private readonly Func<string> _nowIso;

    public WorkflowVersionCacheService(
        string? cachePath = null,
        string? metricsPath = null,
        Func<string>? nowIso = null)
    {
        _cachePath = string.IsNullOrWhiteSpace(cachePath)
            ? Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData), "AIWF", "workflow-version-cache.json")
            : Path.GetFullPath(cachePath);
        _metricsPath = string.IsNullOrWhiteSpace(metricsPath)
            ? Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData), "AIWF", "workflow-version-cache-metrics.json")
            : Path.GetFullPath(metricsPath);
        _nowIso = nowIso ?? (() => DateTimeOffset.UtcNow.ToString("O"));
    }

    public IReadOnlyList<GovernanceWorkflowVersionItem> LoadVersionList()
    {
        return LoadStore().VersionList;
    }

    public void SaveVersionList(IReadOnlyList<GovernanceWorkflowVersionItem> items)
    {
        var store = LoadStore();
        store.VersionList = items
            .Select(static item => item with { })
            .ToList();
        SaveStore(store);
        IncrementSets();
    }

    public bool TryGetCompareResult(string versionA, string versionB, out GovernanceWorkflowVersionCompareResult? result)
    {
        var key = BuildCompareKey(versionA, versionB);
        var store = LoadStore();
        if (store.VersionCompare.TryGetValue(key, out var hit))
        {
            IncrementHit();
            result = hit;
            return true;
        }

        IncrementMiss();
        result = null;
        return false;
    }

    public void SaveCompareResult(string versionA, string versionB, GovernanceWorkflowVersionCompareResult result)
    {
        var store = LoadStore();
        store.VersionCompare[BuildCompareKey(versionA, versionB)] = result;
        SaveStore(store);
        IncrementSets();
    }

    public WorkflowVersionCacheStats GetStats()
    {
        var store = LoadStore();
        var metrics = LoadMetrics();
        var entries = (store.VersionList.Count > 0 ? 1 : 0) + store.VersionCompare.Count;
        var totalLookups = metrics.Hits + metrics.Misses;
        var hitRate = totalLookups <= 0 ? 0 : (double)metrics.Hits / totalLookups;
        return new WorkflowVersionCacheStats(
            entries,
            metrics.Hits,
            metrics.Misses,
            metrics.Sets,
            hitRate,
            metrics.LastResetAt);
    }

    public WorkflowVersionCacheStats Clear()
    {
        SaveStore(new WorkflowVersionCacheStore());
        SaveMetrics(new WorkflowVersionCacheMetrics
        {
            LastResetAt = _nowIso(),
        });
        return GetStats();
    }

    public static string BuildCompareKey(string versionA, string versionB)
    {
        return $"{(versionA ?? string.Empty).Trim()}=>{(versionB ?? string.Empty).Trim()}";
    }

    private WorkflowVersionCacheStore LoadStore()
    {
        try
        {
            if (!File.Exists(_cachePath))
            {
                return new WorkflowVersionCacheStore();
            }

            var json = File.ReadAllText(_cachePath, Encoding.UTF8);
            return JsonSerializer.Deserialize<WorkflowVersionCacheStore>(json, JsonOptions) ?? new WorkflowVersionCacheStore();
        }
        catch
        {
            return new WorkflowVersionCacheStore();
        }
    }

    private void SaveStore(WorkflowVersionCacheStore store)
    {
        var dir = Path.GetDirectoryName(_cachePath) ?? ".";
        Directory.CreateDirectory(dir);
        File.WriteAllText(_cachePath, JsonSerializer.Serialize(store, JsonOptions), Encoding.UTF8);
    }

    private WorkflowVersionCacheMetrics LoadMetrics()
    {
        try
        {
            if (!File.Exists(_metricsPath))
            {
                return new WorkflowVersionCacheMetrics();
            }

            var json = File.ReadAllText(_metricsPath, Encoding.UTF8);
            return JsonSerializer.Deserialize<WorkflowVersionCacheMetrics>(json, JsonOptions) ?? new WorkflowVersionCacheMetrics();
        }
        catch
        {
            return new WorkflowVersionCacheMetrics();
        }
    }

    private void SaveMetrics(WorkflowVersionCacheMetrics metrics)
    {
        var dir = Path.GetDirectoryName(_metricsPath) ?? ".";
        Directory.CreateDirectory(dir);
        File.WriteAllText(_metricsPath, JsonSerializer.Serialize(metrics, JsonOptions), Encoding.UTF8);
    }

    private void IncrementHit()
    {
        var metrics = LoadMetrics();
        metrics.Hits += 1;
        SaveMetrics(metrics);
    }

    private void IncrementMiss()
    {
        var metrics = LoadMetrics();
        metrics.Misses += 1;
        SaveMetrics(metrics);
    }

    private void IncrementSets()
    {
        var metrics = LoadMetrics();
        metrics.Sets += 1;
        SaveMetrics(metrics);
    }
}
