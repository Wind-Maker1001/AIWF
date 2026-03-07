using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Text.Json;

namespace AIWF.Native;

internal static class NativePerfRecorder
{
    private sealed class PerfSnapshot
    {
        public string SessionId { get; init; } = string.Empty;
        public string CapturedAtUtc { get; init; } = string.Empty;
        public Dictionary<string, double> Marks { get; init; } = new(StringComparer.Ordinal);
    }

    private static readonly object Sync = new();
    private static readonly Stopwatch Stopwatch = Stopwatch.StartNew();
    private static readonly string? LogPath = Environment.GetEnvironmentVariable("AIWF_NATIVE_PERF_LOG_PATH");
    private static readonly Dictionary<string, double> Marks = new(StringComparer.Ordinal);
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true
    };
    private static readonly Encoding Utf8NoBom = new UTF8Encoding(encoderShouldEmitUTF8Identifier: false);

    internal static bool IsEnabled => !string.IsNullOrWhiteSpace(LogPath);

    internal static void Mark(string name)
    {
        if (!IsEnabled || string.IsNullOrWhiteSpace(name))
        {
            return;
        }

        lock (Sync)
        {
            if (Marks.ContainsKey(name))
            {
                return;
            }

            Marks[name] = Math.Round(Stopwatch.Elapsed.TotalMilliseconds, 3);
            PersistSnapshot();
        }
    }

    private static void PersistSnapshot()
    {
        if (string.IsNullOrWhiteSpace(LogPath))
        {
            return;
        }

        var dir = Path.GetDirectoryName(LogPath);
        if (!string.IsNullOrWhiteSpace(dir))
        {
            Directory.CreateDirectory(dir);
        }

        var snapshot = new PerfSnapshot
        {
            SessionId = Environment.ProcessId.ToString(),
            CapturedAtUtc = DateTimeOffset.UtcNow.ToString("O"),
            Marks = new Dictionary<string, double>(Marks, StringComparer.Ordinal)
        };
        var json = JsonSerializer.Serialize(snapshot, JsonOptions);
        File.WriteAllText(LogPath, json, Utf8NoBom);
    }
}
