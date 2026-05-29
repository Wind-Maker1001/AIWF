namespace AIWF.Native.Runtime;

public static class WorkflowRunBaselineExportSupport
{
    public static string RenderCompareMarkdown(WorkflowRunCompareResult output)
    {
        var summary = output.Summary;
        var lines = new List<string>
        {
            "# AIWF Run Compare Report",
            string.Empty,
            $"- Generated At: {DateTimeOffset.UtcNow:O}",
            $"- Run A: {summary.RunA}",
            $"- Run B: {summary.RunB}",
            $"- Changed Nodes: {summary.ChangedNodes}",
            string.Empty,
            "| Node | Status A | Status B | Seconds A | Seconds B | Delta |",
            "|---|---|---|---:|---:|---:|",
        };
        foreach (var row in output.NodeDiff)
        {
            lines.Add($"| {row.Id} ({row.Type}) | {row.StatusA} | {row.StatusB} | {row.SecondsA:0.###} | {row.SecondsB:0.###} | {row.SecondsDelta:0.###} |");
        }

        return string.Join("\n", lines) + "\n";
    }

    public static string RenderCompareHtml(WorkflowRunCompareResult output)
    {
        var summary = output.Summary;
        var rows = string.Join(string.Empty, output.NodeDiff.Select(row =>
        {
            var changed = row.StatusChanged || Math.Abs(row.SecondsDelta) > 0.001;
            var background = changed ? " style=\"background:#fff8f2\"" : string.Empty;
            return $"<tr{background}><td>{Escape(row.Id)} ({Escape(row.Type)})</td><td>{Escape(row.StatusA)}</td><td>{Escape(row.StatusB)}</td><td>{row.SecondsA:0.###}</td><td>{row.SecondsB:0.###}</td><td>{row.SecondsDelta:0.###}</td></tr>";
        }));

        return $$"""
        <!doctype html>
        <html lang="en">
        <head>
          <meta charset="utf-8" />
          <title>AIWF Run Compare Report</title>
          <style>
            body { font-family: "Segoe UI", "Microsoft YaHei", sans-serif; padding: 16px; color: #1f2d3d; }
            table { border-collapse: collapse; width: 100%; }
            th, td { border: 1px solid #d8e1ec; padding: 6px 8px; font-size: 13px; }
            th { background: #f3f7fd; text-align: left; }
          </style>
        </head>
        <body>
          <h2>AIWF Run Compare Report</h2>
          <p>
            Generated At: {{DateTimeOffset.UtcNow:O}}<br />
            Run A: {{Escape(summary.RunA)}}<br />
            Run B: {{Escape(summary.RunB)}}<br />
            Changed Nodes: {{summary.ChangedNodes}}
          </p>
          <table>
            <thead>
              <tr>
                <th>Node</th>
                <th>Status A</th>
                <th>Status B</th>
                <th>Seconds A</th>
                <th>Seconds B</th>
                <th>Delta</th>
              </tr>
            </thead>
            <tbody>{{rows}}</tbody>
          </table>
        </body>
        </html>
        """;
    }

    private static string Escape(string value)
    {
        return System.Net.WebUtility.HtmlEncode(value ?? string.Empty);
    }
}
