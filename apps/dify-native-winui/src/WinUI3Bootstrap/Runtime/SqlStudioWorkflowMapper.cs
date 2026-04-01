using System.Text.Json.Nodes;

namespace AIWF.Native.Runtime;

public static class SqlStudioWorkflowMapper
{
    public static WorkflowGraphDocument Build(
        SqlConnectionProfile profile,
        SqlBuilderDraft draft,
        SqlTextDraft textDraft,
        string workflowId = "native_sql_workflow")
    {
        return textDraft.IsTextOwned
            ? BuildSingleSqlNode(profile, textDraft.Text, workflowId, draft.Chart)
            : TryBuildStructured(profile, draft, workflowId, out var document)
                ? document
                : BuildSingleSqlNode(profile, SqlAuthoringBuilderCompiler.BuildSql(draft, profile), workflowId, draft.Chart);
    }

    private static bool TryBuildStructured(
        SqlConnectionProfile profile,
        SqlBuilderDraft draft,
        string workflowId,
        out WorkflowGraphDocument document)
    {
        document = BuildSingleSqlNode(profile, SqlAuthoringBuilderCompiler.BuildSql(draft, profile), workflowId, draft.Chart);
        if (string.IsNullOrWhiteSpace(draft.Table))
        {
            return false;
        }
        if (draft.Filters.Any(filter => !string.Equals(filter.Operator?.Trim(), "=", StringComparison.Ordinal)))
        {
            return false;
        }
        if (!string.IsNullOrWhiteSpace(draft.OrderByField))
        {
            return false;
        }

        var nodes = new List<WorkflowGraphNodeDocument>();
        var edges = new List<WorkflowGraphEdgeDocument>();
        var runtimeSource = profile.BuildRuntimeSource();
        var loadBaseId = "load_base";
        var currentId = loadBaseId;

        nodes.Add(new WorkflowGraphNodeDocument(
            Id: loadBaseId,
            Type: "load_rows_v3",
            Title: "加载基础表",
            Subtitle: $"{draft.Schema}.{draft.Table}".Trim('.'),
            X: 120,
            Y: 120,
            Config: new JsonObject
            {
                ["source_type"] = profile.NormalizedSourceType,
                ["source"] = runtimeSource,
                ["query"] = $"SELECT * FROM {BuildStructuredTableRef(profile, draft.Schema, draft.Table)}",
                ["limit"] = Math.Max(1, draft.Limit),
                ["max_retries"] = 2,
                ["retry_backoff_ms"] = 150,
            }));

        var joinIndex = 0;
        foreach (var join in draft.Joins.Where(static item => item.Enabled && !string.IsNullOrWhiteSpace(item.Table)))
        {
            joinIndex += 1;
            var loadJoinId = $"load_join_{joinIndex}";
            var joinNodeId = $"join_{joinIndex}";
            nodes.Add(new WorkflowGraphNodeDocument(
                Id: loadJoinId,
                Type: "load_rows_v3",
                Title: $"加载关联表 {joinIndex}",
                Subtitle: $"{join.Schema}.{join.Table}".Trim('.'),
                X: 120,
                Y: 280 + ((joinIndex - 1) * 180),
                Config: new JsonObject
                {
                    ["source_type"] = profile.NormalizedSourceType,
                    ["source"] = runtimeSource,
                    ["query"] = $"SELECT * FROM {BuildStructuredTableRef(profile, join.Schema, join.Table)}",
                    ["limit"] = Math.Max(1, draft.Limit),
                    ["max_retries"] = 2,
                    ["retry_backoff_ms"] = 150,
                }));
            nodes.Add(new WorkflowGraphNodeDocument(
                Id: joinNodeId,
                Type: "join_rows_v2",
                Title: $"Join {joinIndex}",
                Subtitle: string.IsNullOrWhiteSpace(join.JoinType) ? "inner" : join.JoinType.Trim().ToLowerInvariant(),
                X: 420,
                Y: 200 + ((joinIndex - 1) * 180),
                Config: new JsonObject
                {
                    ["left_rows"] = new JsonArray(),
                    ["right_rows"] = new JsonArray(),
                    ["left_on"] = new JsonArray(join.LeftField),
                    ["right_on"] = new JsonArray(join.RightField),
                    ["join_type"] = string.IsNullOrWhiteSpace(join.JoinType) ? "inner" : join.JoinType.Trim().ToLowerInvariant(),
                    ["input_map"] = new JsonObject
                    {
                        ["left_rows"] = new JsonObject { ["from"] = currentId, ["path"] = "rows" },
                        ["right_rows"] = new JsonObject { ["from"] = loadJoinId, ["path"] = "rows" },
                    }
                }));
            edges.Add(new WorkflowGraphEdgeDocument(currentId, joinNodeId));
            edges.Add(new WorkflowGraphEdgeDocument(loadJoinId, joinNodeId));
            currentId = joinNodeId;
        }

        var needsColumnar = draft.SelectFields.Count > 0 || draft.Filters.Count > 0 || draft.Limit > 0;
        if (needsColumnar)
        {
            var columnarId = "columnar_eval";
            var filterEq = new JsonObject();
            foreach (var filter in draft.Filters.Where(static item => !string.IsNullOrWhiteSpace(item.Field)))
            {
                filterEq[filter.Field] = filter.Value;
            }

            nodes.Add(new WorkflowGraphNodeDocument(
                Id: columnarId,
                Type: "columnar_eval_v1",
                Title: "列过滤/投影",
                Subtitle: "select + eq filter",
                X: 760,
                Y: 140,
                Config: new JsonObject
                {
                    ["rows"] = new JsonArray(),
                    ["select_fields"] = new JsonArray(draft.SelectFields.Select(field => JsonValue.Create(field)).ToArray()),
                    ["filter_eq"] = filterEq,
                    ["limit"] = Math.Max(1, draft.Limit),
                    ["input_map"] = new JsonObject
                    {
                        ["rows"] = new JsonObject { ["from"] = currentId, ["path"] = "rows" }
                    }
                }));
            edges.Add(new WorkflowGraphEdgeDocument(currentId, columnarId));
            currentId = columnarId;
        }

        if (draft.GroupByFields.Count > 0 || draft.Aggregates.Count > 0)
        {
            var aggregateId = "aggregate_rows";
            nodes.Add(new WorkflowGraphNodeDocument(
                Id: aggregateId,
                Type: "aggregate_rows_v2",
                Title: "聚合",
                Subtitle: "group by + aggregate",
                X: 1060,
                Y: 140,
                Config: new JsonObject
                {
                    ["rows"] = new JsonArray(),
                    ["group_by"] = new JsonArray(draft.GroupByFields.Select(field => JsonValue.Create(field)).ToArray()),
                    ["aggregates"] = new JsonArray(draft.Aggregates.Select(ToAggregateJson).ToArray()),
                    ["input_map"] = new JsonObject
                    {
                        ["rows"] = new JsonObject { ["from"] = currentId, ["path"] = "rows" }
                    }
                }));
            edges.Add(new WorkflowGraphEdgeDocument(currentId, aggregateId));
            currentId = aggregateId;
        }

        if (draft.Chart.Enabled)
        {
            var chartId = "sql_chart";
            nodes.Add(new WorkflowGraphNodeDocument(
                Id: chartId,
                Type: "sql_chart_v1",
                Title: "SQL 图表",
                Subtitle: draft.Chart.ChartType,
                X: 1360,
                Y: 140,
                Config: new JsonObject
                {
                    ["rows"] = new JsonArray(),
                    ["chart_type"] = draft.Chart.ChartType,
                    ["category_field"] = draft.Chart.CategoryField,
                    ["value_field"] = draft.Chart.ValueField,
                    ["series_field"] = draft.Chart.SeriesField,
                    ["top_n"] = Math.Max(1, draft.Chart.TopN),
                    ["input_map"] = new JsonObject
                    {
                        ["rows"] = new JsonObject { ["from"] = currentId, ["path"] = "rows" }
                    }
                }));
            edges.Add(new WorkflowGraphEdgeDocument(currentId, chartId));
        }

        document = new WorkflowGraphDocument(
            WorkflowId: workflowId,
            Version: "1.0.0",
            Nodes: nodes,
            Edges: edges,
            Viewport: WorkflowGraphViewportDocument.Default,
            Selection: WorkflowGraphSelectionDocument.Empty);
        return true;
    }

    private static WorkflowGraphDocument BuildSingleSqlNode(
        SqlConnectionProfile profile,
        string sql,
        string workflowId,
        SqlChartDraft chart)
    {
        var nodes = new List<WorkflowGraphNodeDocument>
        {
            new(
                Id: "load_rows_1",
                Type: "load_rows_v3",
                Title: "SQL 加载",
                Subtitle: profile.NormalizedSourceType,
                X: 160,
                Y: 140,
                Config: new JsonObject
                {
                    ["source_type"] = profile.NormalizedSourceType,
                    ["source"] = profile.BuildRuntimeSource(),
                    ["query"] = sql,
                    ["limit"] = 200,
                    ["max_retries"] = 2,
                    ["retry_backoff_ms"] = 150,
                })
        };
        var edges = new List<WorkflowGraphEdgeDocument>();

        if (chart.Enabled)
        {
            nodes.Add(new WorkflowGraphNodeDocument(
                Id: "sql_chart_1",
                Type: "sql_chart_v1",
                Title: "SQL 图表",
                Subtitle: chart.ChartType,
                X: 460,
                Y: 140,
                Config: new JsonObject
                {
                    ["rows"] = new JsonArray(),
                    ["chart_type"] = chart.ChartType,
                    ["category_field"] = chart.CategoryField,
                    ["value_field"] = chart.ValueField,
                    ["series_field"] = chart.SeriesField,
                    ["top_n"] = Math.Max(1, chart.TopN),
                    ["input_map"] = new JsonObject
                    {
                        ["rows"] = new JsonObject { ["from"] = "load_rows_1", ["path"] = "rows" }
                    }
                }));
            edges.Add(new WorkflowGraphEdgeDocument("load_rows_1", "sql_chart_1"));
        }

        return new WorkflowGraphDocument(
            WorkflowId: workflowId,
            Version: "1.0.0",
            Nodes: nodes,
            Edges: edges,
            Viewport: WorkflowGraphViewportDocument.Default,
            Selection: WorkflowGraphSelectionDocument.Empty);
    }

    private static JsonObject ToAggregateJson(SqlAggregateClause aggregate)
    {
        return new JsonObject
        {
            ["op"] = string.IsNullOrWhiteSpace(aggregate.Function) ? "count" : aggregate.Function.Trim().ToLowerInvariant(),
            ["field"] = string.IsNullOrWhiteSpace(aggregate.Field) ? "*" : aggregate.Field.Trim(),
            ["as"] = string.IsNullOrWhiteSpace(aggregate.Alias)
                ? $"{aggregate.Function.Trim().ToLowerInvariant()}_{aggregate.Field.Trim()}"
                : aggregate.Alias.Trim(),
        };
    }

    private static string BuildStructuredTableRef(SqlConnectionProfile profile, string schema, string table)
    {
        return SqlAuthoringBuilderCompiler.BuildSql(
            SqlBuilderDraft.Empty with { Schema = schema, Table = table, Limit = 0 },
            profile with { SourceType = profile.NormalizedSourceType })
            .Replace("SELECT * FROM ", string.Empty, StringComparison.Ordinal);
    }
}
