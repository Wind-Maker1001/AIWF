using System.Text.Json;
using Microsoft.UI.Xaml.Controls;

namespace AIWF.Native.Nodes;

internal sealed record SqlAuthoringNodeMetadata(
    string Type,
    string KeyPrefix,
    string Title,
    string Subtitle,
    string Group,
    Symbol Icon);

internal static class SqlAuthoringMetadataLoader
{
    private static readonly IReadOnlyList<SqlAuthoringNodeMetadata> Fallback =
    [
        new("load_rows_v3", "sql-load", "SQL 读取", "连接 SQLite / SQL Server 并读取数据", "SQL Authoring", Symbol.OpenLocal),
        new("columnar_eval_v1", "sql-project", "列过滤/投影", "选择字段并执行等值过滤", "SQL Authoring", Symbol.Filter),
        new("join_rows_v2", "sql-join", "多键 Join", "关联两路表数据", "SQL Authoring", Symbol.Switch),
        new("aggregate_rows_v2", "sql-aggregate", "聚合", "分组并计算汇总指标", "SQL Authoring", Symbol.AllApps),
        new("query_lang_v1", "sql-query", "查询 DSL", "对上游 rows 执行轻量 where/select/limit", "SQL Authoring", Symbol.Find),
        new("sql_chart_v1", "sql-chart", "SQL 图表", "把 rows 转成 categories/series 图表结构", "SQL Authoring", Symbol.World),
    ];

    public static IReadOnlyList<SqlAuthoringNodeMetadata> Load()
    {
        try
        {
            var path = ResolveContractPath();
            if (string.IsNullOrWhiteSpace(path) || !File.Exists(path))
            {
                return Fallback;
            }

            using var stream = File.OpenRead(path);
            using var doc = JsonDocument.Parse(stream);
            var nodes = doc.RootElement.GetProperty("nodes");
            var items = new List<SqlAuthoringNodeMetadata>();
            foreach (var node in nodes.EnumerateArray())
            {
                var type = node.GetProperty("type").GetString() ?? string.Empty;
                var keyPrefix = node.GetProperty("key_prefix").GetString() ?? string.Empty;
                var title = node.GetProperty("title").GetString() ?? string.Empty;
                var subtitle = node.GetProperty("subtitle").GetString() ?? string.Empty;
                var group = node.GetProperty("group").GetString() ?? "SQL Authoring";
                var iconName = node.TryGetProperty("icon", out var iconElement)
                    ? iconElement.GetString() ?? "Page"
                    : "Page";
                var icon = Enum.TryParse<Symbol>(iconName, true, out var parsed) ? parsed : Symbol.Page;
                if (string.IsNullOrWhiteSpace(type) || string.IsNullOrWhiteSpace(keyPrefix))
                {
                    continue;
                }

                items.Add(new SqlAuthoringNodeMetadata(type, keyPrefix, title, subtitle, group, icon));
            }

            return items.Count == 0 ? Fallback : items;
        }
        catch
        {
            return Fallback;
        }
    }

    private static string? ResolveContractPath()
    {
        var current = new DirectoryInfo(AppContext.BaseDirectory);
        while (current is not null)
        {
            var candidate = Path.Combine(current.FullName, "contracts", "desktop", "sql_authoring_metadata.v1.json");
            if (File.Exists(candidate))
            {
                return candidate;
            }
            current = current.Parent;
        }

        return null;
    }
}
