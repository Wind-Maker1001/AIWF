using System.Text.Json;
using System.Text.Json.Nodes;

namespace AIWF.Native.Runtime;

public static class SqlStudioResultMapper
{
    public static SqlPreviewState FromLoadRowsResponse(JsonObject? payload, string generatedSql)
    {
        if (payload is null)
        {
            return SqlPreviewState.Empty;
        }

        var rawJson = payload.ToJsonString(new JsonSerializerOptions { WriteIndented = true });
        var rows = payload["rows"] as JsonArray;
        var rowDisplayItems = rows is null
            ? Array.Empty<string>()
            : rows
                .OfType<JsonObject>()
                .Select(FormatRow)
                .ToArray();
        var statsText = payload["stats"] is JsonNode stats
            ? stats.ToJsonString(new JsonSerializerOptions { WriteIndented = true })
            : "{}";
        var ok = payload["ok"]?.GetValue<bool?>() == true;
        var status = payload["status"]?.GetValue<string>() ?? (ok ? "done" : "failed");

        return new SqlPreviewState(
            Ok: ok,
            StatusText: ok ? $"Preview ready: {rowDisplayItems.Length} row(s)." : $"Preview failed: {status}",
            GeneratedSql: generatedSql,
            RawJson: rawJson,
            Diagnostics: statsText,
            RowDisplayItems: rowDisplayItems);
    }

    public static SchemaBrowserState MergeBrowseResponse(
        SchemaBrowserState current,
        JsonObject? payload,
        string op)
    {
        if (payload is null)
        {
            return current with { Diagnostics = "Empty metadata response." };
        }

        var diagnostics = payload["stats"] is JsonNode stats
            ? stats.ToJsonString(new JsonSerializerOptions { WriteIndented = true })
            : payload.ToJsonString(new JsonSerializerOptions { WriteIndented = true });
        var items = (payload["items"] as JsonArray)
            ?.OfType<JsonObject>()
            .Select(item => new SqlSchemaItem(
                item["name"]?.GetValue<string>() ?? string.Empty,
                item["kind"]?.GetValue<string>() ?? string.Empty,
                item["schema"]?.GetValue<string>()))
            .ToArray()
            ?? Array.Empty<SqlSchemaItem>();
        var columns = (payload["columns"] as JsonArray)
            ?.OfType<JsonObject>()
            .Select(item => new SqlColumnMetadata(
                item["name"]?.GetValue<string>() ?? string.Empty,
                item["data_type"]?.GetValue<string>() ?? string.Empty,
                item["nullable"]?.GetValue<bool?>() != false))
            .ToArray()
            ?? Array.Empty<SqlColumnMetadata>();

        return op switch
        {
            "list_schemas" => current with { Schemas = items, Diagnostics = diagnostics },
            "list_tables" => current with { Tables = items, Diagnostics = diagnostics },
            "describe_table" => current with { Columns = columns, Diagnostics = diagnostics },
            _ => current with { Diagnostics = diagnostics },
        };
    }

    private static string FormatRow(JsonObject row)
    {
        return string.Join(" | ", row.Select(kv => $"{kv.Key}={kv.Value?.ToJsonString() ?? "null"}"));
    }
}
