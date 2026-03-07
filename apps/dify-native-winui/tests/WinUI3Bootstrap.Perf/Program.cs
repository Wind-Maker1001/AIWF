using System.Diagnostics;
using System.Text;
using System.Text.Json;
using AIWF.Native.CanvasRuntime;
using Windows.Foundation;

internal sealed class StartupLogSnapshot
{
    public string? SessionId { get; init; }
    public string? CapturedAtUtc { get; init; }
    public Dictionary<string, double>? Marks { get; init; }
}

internal sealed class StartupSummary
{
    public bool Available { get; init; }
    public string SourcePath { get; init; } = string.Empty;
    public double? WindowActivatedMs { get; init; }
    public double? MainWindowCtorDurationMs { get; init; }
    public double? ActivateRequestToWindowActivatedMs { get; init; }
    public Dictionary<string, double> Marks { get; init; } = new(StringComparer.Ordinal);
}

internal sealed class BenchmarkMetric
{
    public string Name { get; init; } = string.Empty;
    public string Kind { get; init; } = string.Empty;
    public int SampleCount { get; init; }
    public int OperationsPerSample { get; init; }
    public double MeanUs { get; init; }
    public double P50Us { get; init; }
    public double P95Us { get; init; }
    public double MaxUs { get; init; }
    public double Budget60FpsPercent { get; init; }
    public string StutterRisk { get; init; } = string.Empty;
    public double Sink { get; init; }
}

internal sealed class PerfReport
{
    public string GeneratedAtUtc { get; init; } = string.Empty;
    public string MachineName { get; init; } = string.Empty;
    public StartupSummary Startup { get; init; } = new();
    public IReadOnlyList<BenchmarkMetric> Benchmarks { get; init; } = Array.Empty<BenchmarkMetric>();
    public IReadOnlyList<string> Notes { get; init; } = Array.Empty<string>();
}

internal static class Program
{
    private static int Main(string[] args)
    {
        var argsMap = ParseArgs(args);
        var startupLogPath = GetOptionalArg(argsMap, "--startup-log");
        var jsonOutputPath = GetRequiredArg(argsMap, "--json");
        var markdownOutputPath = GetRequiredArg(argsMap, "--markdown");

        var startup = LoadStartupSummary(startupLogPath);
        var benchmarks = new[]
        {
            RunNodeDragBenchmark(),
            RunSplitColumnsBenchmark(),
            RunSplitRowsBenchmark()
        };

        var report = new PerfReport
        {
            GeneratedAtUtc = DateTimeOffset.UtcNow.ToString("O"),
            MachineName = Environment.MachineName,
            Startup = startup,
            Benchmarks = benchmarks,
            Notes = new[]
            {
                "Startup timing comes from the instrumented WinUI app and reflects real process startup on this machine.",
                "Drag and split metrics are pure-logic microbenchmarks. They exclude XAML layout, compositor, and GPU cost.",
                "Budget60FpsPercent compares benchmark p95 against a 16.67 ms frame budget. Lower is better."
            }
        };

        WriteJson(jsonOutputPath, report);
        WriteMarkdown(markdownOutputPath, report);
        Console.WriteLine($"JSON report: {jsonOutputPath}");
        Console.WriteLine($"Markdown report: {markdownOutputPath}");
        return 0;
    }

    private static Dictionary<string, string> ParseArgs(string[] rawArgs)
    {
        var map = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        for (var i = 0; i < rawArgs.Length; i++)
        {
            var key = rawArgs[i];
            if (!key.StartsWith("--", StringComparison.Ordinal))
            {
                continue;
            }

            if (i + 1 >= rawArgs.Length)
            {
                throw new ArgumentException($"Missing value for argument '{key}'.");
            }

            map[key] = rawArgs[++i];
        }

        return map;
    }

    private static string GetRequiredArg(IReadOnlyDictionary<string, string> argsMap, string key)
    {
        if (!argsMap.TryGetValue(key, out var value) || string.IsNullOrWhiteSpace(value))
        {
            throw new ArgumentException($"Required argument '{key}' is missing.");
        }

        return value;
    }

    private static string? GetOptionalArg(IReadOnlyDictionary<string, string> argsMap, string key)
    {
        return argsMap.TryGetValue(key, out var value) && !string.IsNullOrWhiteSpace(value)
            ? value
            : null;
    }

    private static StartupSummary LoadStartupSummary(string? startupLogPath)
    {
        if (string.IsNullOrWhiteSpace(startupLogPath) || !File.Exists(startupLogPath))
        {
            return new StartupSummary
            {
                Available = false,
                SourcePath = startupLogPath ?? string.Empty
            };
        }

        var snapshot = JsonSerializer.Deserialize<StartupLogSnapshot>(File.ReadAllText(startupLogPath));
        var marks = snapshot?.Marks is null
            ? new Dictionary<string, double>(StringComparer.Ordinal)
            : new Dictionary<string, double>(snapshot.Marks, StringComparer.Ordinal);

        return new StartupSummary
        {
            Available = true,
            SourcePath = startupLogPath,
            WindowActivatedMs = GetMark(marks, "window_activated"),
            MainWindowCtorDurationMs = GetDelta(marks, "main_window_ctor_enter", "main_window_ctor_exit"),
            ActivateRequestToWindowActivatedMs = GetDelta(marks, "main_window_activated_request", "window_activated"),
            Marks = marks
        };
    }

    private static double? GetMark(IReadOnlyDictionary<string, double> marks, string key)
    {
        return marks.TryGetValue(key, out var value) ? value : null;
    }

    private static double? GetDelta(IReadOnlyDictionary<string, double> marks, string startKey, string endKey)
    {
        if (!marks.TryGetValue(startKey, out var start) || !marks.TryGetValue(endKey, out var end))
        {
            return null;
        }

        return Math.Round(end - start, 3);
    }

    private static BenchmarkMetric RunNodeDragBenchmark()
    {
        const int sampleCount = 240;
        const int operationsPerSample = 2_000;
        var dragStart = new Point(128, 96);
        double sink = 0;
        var samples = MeasureSamples(sampleCount, operationsPerSample, index =>
        {
            var point = new Point(
                128 + (index % 173),
                96 + ((index * 7) % 137));
            var position = NodeDragMath.ComputeDragPosition(
                140 + (index % 19),
                80 + (index % 13),
                dragStart,
                point);
            sink += position.Left + position.Top;
        });

        return BuildMetric("node_drag_math", "pure_logic", sampleCount, operationsPerSample, sink, samples);
    }

    private static BenchmarkMetric RunSplitColumnsBenchmark()
    {
        const int sampleCount = 240;
        const int operationsPerSample = 2_000;
        double sink = 0;
        var samples = MeasureSamples(sampleCount, operationsPerSample, index =>
        {
            var result = SplitLayoutController.CalculateColumns(
                1280,
                760,
                (index % 181) - 90,
                320,
                240);
            sink += result.Left + result.Right;
        });

        return BuildMetric("split_columns_math", "pure_logic", sampleCount, operationsPerSample, sink, samples);
    }

    private static BenchmarkMetric RunSplitRowsBenchmark()
    {
        const int sampleCount = 240;
        const int operationsPerSample = 2_000;
        double sink = 0;
        var samples = MeasureSamples(sampleCount, operationsPerSample, index =>
        {
            var result = SplitLayoutController.CalculateRows(
                960,
                540,
                (index % 181) - 90,
                240,
                200);
            sink += result.Top + result.Bottom;
        });

        return BuildMetric("split_rows_math", "pure_logic", sampleCount, operationsPerSample, sink, samples);
    }

    private static double[] MeasureSamples(int sampleCount, int operationsPerSample, Action<int> operation)
    {
        for (var warmup = 0; warmup < operationsPerSample; warmup++)
        {
            operation(warmup);
        }

        var samples = new double[sampleCount];
        for (var sample = 0; sample < sampleCount; sample++)
        {
            var start = Stopwatch.GetTimestamp();
            for (var op = 0; op < operationsPerSample; op++)
            {
                operation((sample * operationsPerSample) + op);
            }

            var elapsedUs = Stopwatch.GetElapsedTime(start).TotalMilliseconds * 1000.0;
            samples[sample] = elapsedUs / operationsPerSample;
        }

        return samples;
    }

    private static BenchmarkMetric BuildMetric(
        string name,
        string kind,
        int sampleCount,
        int operationsPerSample,
        double sink,
        IReadOnlyList<double> samplesUs)
    {
        var ordered = samplesUs.OrderBy(static x => x).ToArray();
        var meanUs = Math.Round(samplesUs.Average(), 3);
        var p50Us = Quantile(ordered, 0.50);
        var p95Us = Quantile(ordered, 0.95);
        var maxUs = Math.Round(ordered[^1], 3);
        var budgetPercent = Math.Round((p95Us / 16666.667) * 100.0, 4);

        return new BenchmarkMetric
        {
            Name = name,
            Kind = kind,
            SampleCount = sampleCount,
            OperationsPerSample = operationsPerSample,
            MeanUs = meanUs,
            P50Us = p50Us,
            P95Us = p95Us,
            MaxUs = maxUs,
            Budget60FpsPercent = budgetPercent,
            StutterRisk = budgetPercent switch
            {
                >= 100 => "high",
                >= 50 => "medium",
                _ => "low"
            },
            Sink = Math.Round(sink, 3)
        };
    }

    private static double Quantile(IReadOnlyList<double> sortedSamples, double percentile)
    {
        if (sortedSamples.Count == 0)
        {
            return 0;
        }

        var index = (int)Math.Ceiling((sortedSamples.Count - 1) * percentile);
        return Math.Round(sortedSamples[Math.Clamp(index, 0, sortedSamples.Count - 1)], 3);
    }

    private static void WriteJson(string path, PerfReport report)
    {
        var dir = Path.GetDirectoryName(path);
        if (!string.IsNullOrWhiteSpace(dir))
        {
            Directory.CreateDirectory(dir);
        }

        var json = JsonSerializer.Serialize(report, new JsonSerializerOptions
        {
            WriteIndented = true
        });
        File.WriteAllText(path, json, new UTF8Encoding(encoderShouldEmitUTF8Identifier: false));
    }

    private static void WriteMarkdown(string path, PerfReport report)
    {
        var dir = Path.GetDirectoryName(path);
        if (!string.IsNullOrWhiteSpace(dir))
        {
            Directory.CreateDirectory(dir);
        }

        var builder = new StringBuilder();
        builder.AppendLine("# Native WinUI Performance Baseline");
        builder.AppendLine();
        builder.AppendLine($"- Generated: {report.GeneratedAtUtc}");
        builder.AppendLine($"- Machine: {report.MachineName}");
        builder.AppendLine();
        builder.AppendLine("## Startup");
        if (!report.Startup.Available)
        {
            builder.AppendLine("- Startup log: unavailable");
        }
        else
        {
            builder.AppendLine($"- Startup log: `{report.Startup.SourcePath}`");
            builder.AppendLine($"- First window activated: {FormatMs(report.Startup.WindowActivatedMs)}");
            builder.AppendLine($"- MainWindow ctor duration: {FormatMs(report.Startup.MainWindowCtorDurationMs)}");
            builder.AppendLine($"- Activate request to first activation: {FormatMs(report.Startup.ActivateRequestToWindowActivatedMs)}");
        }

        builder.AppendLine();
        builder.AppendLine("## Benchmarks");
        foreach (var metric in report.Benchmarks)
        {
            builder.AppendLine($"### {metric.Name}");
            builder.AppendLine($"- Mean: {metric.MeanUs:0.###} us");
            builder.AppendLine($"- P50: {metric.P50Us:0.###} us");
            builder.AppendLine($"- P95: {metric.P95Us:0.###} us");
            builder.AppendLine($"- Max: {metric.MaxUs:0.###} us");
            builder.AppendLine($"- 60 FPS budget usage (p95): {metric.Budget60FpsPercent:0.####}%");
            builder.AppendLine($"- Stutter risk: {metric.StutterRisk}");
            builder.AppendLine();
        }

        builder.AppendLine("## Notes");
        foreach (var note in report.Notes)
        {
            builder.AppendLine($"- {note}");
        }

        File.WriteAllText(path, builder.ToString(), new UTF8Encoding(encoderShouldEmitUTF8Identifier: false));
    }

    private static string FormatMs(double? value)
    {
        return value.HasValue ? $"{value.Value:0.###} ms" : "n/a";
    }
}
