using System.Text.Json.Nodes;

namespace AIWF.Native.Runtime;

public static class SqlCanvasNodeDefaults
{
    public static JsonObject Create(string? workflowNodeType)
    {
        return workflowNodeType switch
        {
            "load_rows_v3" => new JsonObject
            {
                ["source_type"] = SqlConnectionProfile.Sqlite,
                ["source"] = string.Empty,
                ["query"] = "SELECT * FROM data",
                ["limit"] = 200,
                ["max_retries"] = 2,
                ["retry_backoff_ms"] = 150,
            },
            "columnar_eval_v1" => new JsonObject
            {
                ["rows"] = new JsonArray(),
                ["select_fields"] = new JsonArray(),
                ["filter_eq"] = new JsonObject(),
                ["limit"] = 200,
            },
            "join_rows_v2" => new JsonObject
            {
                ["left_rows"] = new JsonArray(),
                ["right_rows"] = new JsonArray(),
                ["left_on"] = new JsonArray("id"),
                ["right_on"] = new JsonArray("id"),
                ["join_type"] = "inner",
            },
            "aggregate_rows_v2" => new JsonObject
            {
                ["rows"] = new JsonArray(),
                ["group_by"] = new JsonArray(),
                ["aggregates"] = new JsonArray(new JsonObject
                {
                    ["op"] = "count",
                    ["field"] = "*",
                    ["as"] = "row_count",
                }),
            },
            "query_lang_v1" => new JsonObject
            {
                ["rows"] = new JsonArray(),
                ["query"] = "limit 100",
            },
            "sql_chart_v1" => new JsonObject
            {
                ["rows"] = new JsonArray(),
                ["chart_type"] = "bar",
                ["category_field"] = "category",
                ["value_field"] = "value",
                ["series_field"] = "series",
                ["top_n"] = 20,
            },
            _ => new JsonObject(),
        };
    }
}
