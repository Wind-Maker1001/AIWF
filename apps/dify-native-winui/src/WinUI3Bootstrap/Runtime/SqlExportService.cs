using System.Globalization;
using System.Text;
using System.Text.Json;

namespace AIWF.Native.Runtime;

public static class SqlExportService
{
    public static string ExportCsv(IReadOnlyList<string> columns, IReadOnlyList<IReadOnlyList<string>> rows)
    {
        var sb = new StringBuilder();
        sb.AppendLine(string.Join(",", columns.Select(EscapeCsvField)));
        foreach (var row in rows)
        {
            var cells = new string[columns.Count];
            for (var i = 0; i < columns.Count; i++)
            {
                cells[i] = EscapeCsvField(i < row.Count ? row[i] : string.Empty);
            }

            sb.AppendLine(string.Join(",", cells));
        }

        return sb.ToString();
    }

    public static string ExportJson(IReadOnlyList<string> columns, IReadOnlyList<IReadOnlyList<string>> rows)
    {
        var records = new List<Dictionary<string, string>>();
        foreach (var row in rows)
        {
            var record = new Dictionary<string, string>(StringComparer.Ordinal);
            for (var i = 0; i < columns.Count; i++)
            {
                record[columns[i]] = i < row.Count ? row[i] : string.Empty;
            }

            records.Add(record);
        }

        return JsonSerializer.Serialize(records, new JsonSerializerOptions { WriteIndented = true });
    }

    private static string EscapeCsvField(string value)
    {
        if (value.Contains(',') || value.Contains('"') || value.Contains('\n') || value.Contains('\r'))
        {
            return $"\"{value.Replace("\"", "\"\"", StringComparison.Ordinal)}\"";
        }

        return value;
    }
}
