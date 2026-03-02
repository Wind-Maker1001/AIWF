using System.Text.Json.Nodes;

namespace AIWF.Native.Runtime;

public sealed record RunArtifactItem(string ArtifactId, string Kind, string Path);

public sealed class RunResultViewData
{
    public bool? Ok { get; init; }
    public string JobId { get; init; } = "-";
    public string RunMode { get; init; } = "-";
    public int? DurationMs { get; init; }
    public IReadOnlyList<RunArtifactItem> Artifacts { get; init; } = [];
}

public static class RunResultParser
{
    public static bool TryParse(string json, out RunResultViewData data)
    {
        data = new RunResultViewData();
        JsonNode? root;
        try
        {
            root = JsonNode.Parse(json);
        }
        catch
        {
            return false;
        }

        if (root is null)
        {
            return false;
        }

        var ok = root["ok"]?.GetValue<bool?>();
        var jobId = root["job_id"]?.GetValue<string?>() ?? "-";
        var payload = root["data"];
        var runMode = payload?["mode"]?.GetValue<string?>() ?? root["run_mode"]?.GetValue<string?>() ?? "-";
        var duration = payload?["duration_ms"]?.GetValue<int?>() ?? root["duration_ms"]?.GetValue<int?>();
        var artifactsArray = root["artifacts"] as JsonArray ?? payload?["artifacts"] as JsonArray;
        var artifacts = new List<RunArtifactItem>();

        if (artifactsArray is not null)
        {
            foreach (var artifact in artifactsArray)
            {
                if (artifact is null)
                {
                    continue;
                }

                var id = artifact["artifact_id"]?.GetValue<string?>() ?? "-";
                var kind = artifact["kind"]?.GetValue<string?>() ?? "-";
                var path = artifact["path"]?.GetValue<string?>() ?? "-";
                artifacts.Add(new RunArtifactItem(id, kind, path));
            }
        }

        data = new RunResultViewData
        {
            Ok = ok,
            JobId = jobId,
            RunMode = runMode,
            DurationMs = duration,
            Artifacts = artifacts
        };
        return true;
    }
}
