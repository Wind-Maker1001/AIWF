using System.Globalization;
using System.IO;
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
    private static readonly HashSet<string> OpenableArtifactExtensions = new(StringComparer.OrdinalIgnoreCase)
    {
        ".xlsx",
        ".docx",
        ".pptx",
        ".csv",
        ".json",
        ".jsonl",
        ".txt",
        ".md",
        ".pdf",
        ".png",
        ".jpg",
        ".jpeg",
        ".bmp",
        ".webp",
        ".tif",
        ".tiff",
        ".parquet"
    };

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

        if (root is not JsonObject rootObject)
        {
            return false;
        }

        var payload = rootObject["data"] as JsonObject;
        var ok = TryReadBoolean(rootObject["ok"]) ?? TryReadBoolean(payload?["ok"]);
        var jobId = ReadString(rootObject["job_id"]) ?? ReadString(payload?["job_id"]) ?? "-";
        var runMode = ReadString(payload?["mode"])
            ?? ReadString(rootObject["run_mode"])
            ?? ReadString(rootObject["mode"])
            ?? ReadString(rootObject["flow"])
            ?? "-";
        var duration = TryReadInt(payload?["duration_ms"])
            ?? TryReadInt(rootObject["duration_ms"])
            ?? TryReadSecondsAsMilliseconds(payload?["seconds"])
            ?? TryReadSecondsAsMilliseconds(rootObject["seconds"]);
        var artifacts = ReadArtifacts(rootObject["artifacts"] as JsonArray ?? payload?["artifacts"] as JsonArray);

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

    public static bool CanOpenArtifactPath(string? path)
    {
        if (string.IsNullOrWhiteSpace(path))
        {
            return false;
        }

        var ext = Path.GetExtension(path.Trim());
        if (string.IsNullOrWhiteSpace(ext))
        {
            return false;
        }

        return OpenableArtifactExtensions.Contains(ext);
    }

    private static IReadOnlyList<RunArtifactItem> ReadArtifacts(JsonArray? artifactsArray)
    {
        if (artifactsArray is null)
        {
            return [];
        }

        var artifacts = new List<RunArtifactItem>(artifactsArray.Count);
        foreach (var artifact in artifactsArray)
        {
            if (artifact is not JsonObject artifactObject)
            {
                continue;
            }

            artifacts.Add(new RunArtifactItem(
                ReadString(artifactObject["artifact_id"]) ?? "-",
                ReadString(artifactObject["kind"]) ?? "-",
                ReadString(artifactObject["path"]) ?? "-"));
        }

        return artifacts;
    }

    private static string? ReadString(JsonNode? node)
    {
        if (node is JsonValue value)
        {
            if (value.TryGetValue<string>(out var text) && !string.IsNullOrWhiteSpace(text))
            {
                return text;
            }

            if (value.TryGetValue<int>(out var intValue))
            {
                return intValue.ToString(CultureInfo.InvariantCulture);
            }

            if (value.TryGetValue<long>(out var longValue))
            {
                return longValue.ToString(CultureInfo.InvariantCulture);
            }

            if (value.TryGetValue<double>(out var doubleValue) && !double.IsNaN(doubleValue) && !double.IsInfinity(doubleValue))
            {
                return doubleValue.ToString(CultureInfo.InvariantCulture);
            }

            if (value.TryGetValue<decimal>(out var decimalValue))
            {
                return decimalValue.ToString(CultureInfo.InvariantCulture);
            }
        }

        return null;
    }

    private static bool? TryReadBoolean(JsonNode? node)
    {
        if (node is not JsonValue value)
        {
            return null;
        }

        if (value.TryGetValue<bool>(out var boolValue))
        {
            return boolValue;
        }

        if (value.TryGetValue<string>(out var text))
        {
            if (bool.TryParse(text, out var parsedBool))
            {
                return parsedBool;
            }

            if (long.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsedLong))
            {
                return parsedLong != 0;
            }
        }

        if (value.TryGetValue<int>(out var intValue))
        {
            return intValue != 0;
        }

        if (value.TryGetValue<long>(out var longValue))
        {
            return longValue != 0;
        }

        if (value.TryGetValue<double>(out var doubleValue) && !double.IsNaN(doubleValue) && !double.IsInfinity(doubleValue))
        {
            return Math.Abs(doubleValue) > double.Epsilon;
        }

        return null;
    }

    private static int? TryReadInt(JsonNode? node)
    {
        if (node is not JsonValue value)
        {
            return null;
        }

        if (value.TryGetValue<int>(out var intValue))
        {
            return intValue;
        }

        if (value.TryGetValue<long>(out var longValue) && longValue is >= int.MinValue and <= int.MaxValue)
        {
            return (int)longValue;
        }

        if (value.TryGetValue<double>(out var doubleValue) && !double.IsNaN(doubleValue) && !double.IsInfinity(doubleValue))
        {
            return (int)Math.Round(doubleValue, MidpointRounding.AwayFromZero);
        }

        if (value.TryGetValue<string>(out var text))
        {
            if (int.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsedInt))
            {
                return parsedInt;
            }

            if (double.TryParse(text, NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture, out var parsedDouble)
                && !double.IsNaN(parsedDouble)
                && !double.IsInfinity(parsedDouble))
            {
                return (int)Math.Round(parsedDouble, MidpointRounding.AwayFromZero);
            }
        }

        return null;
    }

    private static int? TryReadSecondsAsMilliseconds(JsonNode? node)
    {
        if (node is not JsonValue value)
        {
            return null;
        }

        if (value.TryGetValue<double>(out var doubleValue) && !double.IsNaN(doubleValue) && !double.IsInfinity(doubleValue))
        {
            return (int)Math.Round(doubleValue * 1000, MidpointRounding.AwayFromZero);
        }

        if (value.TryGetValue<decimal>(out var decimalValue))
        {
            return (int)Math.Round(decimalValue * 1000, MidpointRounding.AwayFromZero);
        }

        if (value.TryGetValue<string>(out var text)
            && double.TryParse(text, NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture, out var parsedDouble)
            && !double.IsNaN(parsedDouble)
            && !double.IsInfinity(parsedDouble))
        {
            return (int)Math.Round(parsedDouble * 1000, MidpointRounding.AwayFromZero);
        }

        return null;
    }
}
