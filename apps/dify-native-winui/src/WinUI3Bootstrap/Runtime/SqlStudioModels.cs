using System.Text.Json.Nodes;

namespace AIWF.Native.Runtime;

public sealed record SqlFilterClause(string Field, string Operator, string Value);

public sealed record SqlJoinClause(
    bool Enabled,
    string Schema,
    string Table,
    string JoinType,
    string LeftField,
    string RightField);

public sealed record SqlAggregateClause(string Function, string Field, string Alias);

public sealed record SqlChartDraft(
    bool Enabled,
    string ChartType,
    string CategoryField,
    string ValueField,
    string SeriesField,
    int TopN)
{
    public static SqlChartDraft Default { get; } = new(
        Enabled: false,
        ChartType: "bar",
        CategoryField: "category",
        ValueField: "value",
        SeriesField: "series",
        TopN: 20);
}

public sealed record SqlBuilderDraft(
    string Schema,
    string Table,
    IReadOnlyList<string> SelectFields,
    IReadOnlyList<SqlFilterClause> Filters,
    IReadOnlyList<SqlJoinClause> Joins,
    IReadOnlyList<string> GroupByFields,
    IReadOnlyList<SqlAggregateClause> Aggregates,
    string OrderByField,
    bool OrderByDescending,
    int Limit,
    SqlChartDraft Chart)
{
    public static SqlBuilderDraft Empty { get; } = new(
        Schema: string.Empty,
        Table: string.Empty,
        SelectFields: Array.Empty<string>(),
        Filters: Array.Empty<SqlFilterClause>(),
        Joins: Array.Empty<SqlJoinClause>(),
        GroupByFields: Array.Empty<string>(),
        Aggregates: Array.Empty<SqlAggregateClause>(),
        OrderByField: string.Empty,
        OrderByDescending: false,
        Limit: 100,
        Chart: SqlChartDraft.Default);
}

public sealed record SqlTextDraft(string Text, bool IsTextOwned)
{
    public static SqlTextDraft Empty { get; } = new(string.Empty, false);
}

public sealed record SqlSchemaItem(string Name, string Kind, string? Schema = null)
{
    public override string ToString()
    {
        return string.IsNullOrWhiteSpace(Schema) ? $"{Name} | {Kind}" : $"{Schema}.{Name} | {Kind}";
    }
}

public sealed record SqlColumnMetadata(string Name, string DataType, bool Nullable)
{
    public override string ToString()
    {
        return $"{Name} | {DataType} | {(Nullable ? "NULL" : "NOT NULL")}";
    }
}

public sealed record SchemaBrowserState(
    IReadOnlyList<SqlSchemaItem> Schemas,
    IReadOnlyList<SqlSchemaItem> Tables,
    IReadOnlyList<SqlColumnMetadata> Columns,
    string SelectedSchema,
    string SelectedTable,
    string Diagnostics)
{
    public static SchemaBrowserState Empty { get; } = new(
        Schemas: Array.Empty<SqlSchemaItem>(),
        Tables: Array.Empty<SqlSchemaItem>(),
        Columns: Array.Empty<SqlColumnMetadata>(),
        SelectedSchema: string.Empty,
        SelectedTable: string.Empty,
        Diagnostics: string.Empty);
}

public sealed record SqlPreviewState(
    bool Ok,
    string StatusText,
    string GeneratedSql,
    string RawJson,
    string Diagnostics,
    IReadOnlyList<string> RowDisplayItems)
{
    public static SqlPreviewState Empty { get; } = new(
        Ok: false,
        StatusText: "No preview yet.",
        GeneratedSql: string.Empty,
        RawJson: string.Empty,
        Diagnostics: string.Empty,
        RowDisplayItems: Array.Empty<string>());
}

public sealed record WorkflowGraphNodeDocument(
    string Id,
    string Type,
    string Title,
    string Subtitle,
    double X,
    double Y,
    JsonObject Config);

public sealed record WorkflowGraphEdgeDocument(string From, string To);

public sealed record WorkflowGraphViewportDocument(double Scale, double TranslateX, double TranslateY)
{
    public static WorkflowGraphViewportDocument Default { get; } = new(1.0, 0, 0);
}

public sealed record WorkflowGraphSelectionDocument(IReadOnlyList<string> NodeIds)
{
    public static WorkflowGraphSelectionDocument Empty { get; } = new(Array.Empty<string>());
}

public sealed record WorkflowGraphDocument(
    string WorkflowId,
    string Version,
    IReadOnlyList<WorkflowGraphNodeDocument> Nodes,
    IReadOnlyList<WorkflowGraphEdgeDocument> Edges,
    WorkflowGraphViewportDocument Viewport,
    WorkflowGraphSelectionDocument Selection);

public sealed record SqlConnectionProfile(
    string AccelUrl,
    string SourceType,
    string SQLitePath,
    string SqlServerHost,
    string SqlServerPort,
    string Database,
    string User,
    string Password,
    bool UseWindowsAuth)
{
    public const string Sqlite = "sqlite";
    public const string SqlServer = "sqlserver";

    public static SqlConnectionProfile Default { get; } = new(
        AccelUrl: "http://127.0.0.1:18082",
        SourceType: Sqlite,
        SQLitePath: string.Empty,
        SqlServerHost: "127.0.0.1",
        SqlServerPort: "1433",
        Database: "AIWF",
        User: string.Empty,
        Password: string.Empty,
        UseWindowsAuth: true);

    public string NormalizedSourceType =>
        string.Equals(SourceType?.Trim(), SqlServer, StringComparison.OrdinalIgnoreCase)
            ? SqlServer
            : Sqlite;

    public string ResolveAccelUrl(string? bridgeBaseUrl = null)
    {
        var direct = (AccelUrl ?? string.Empty).Trim();
        if (!string.IsNullOrWhiteSpace(direct))
        {
            return direct.TrimEnd('/');
        }

        var bridge = (bridgeBaseUrl ?? string.Empty).Trim().TrimEnd('/');
        if (Uri.TryCreate(bridge, UriKind.Absolute, out var bridgeUri))
        {
            var builder = new UriBuilder(bridgeUri)
            {
                Port = bridgeUri.Port == 18081 ? 18082 : (bridgeUri.Port > 0 ? bridgeUri.Port : 18082),
                Path = string.Empty,
                Query = string.Empty
            };
            return builder.Uri.ToString().TrimEnd('/');
        }

        return "http://127.0.0.1:18082";
    }

    public string BuildRuntimeSource()
    {
        if (NormalizedSourceType == Sqlite)
        {
            return (SQLitePath ?? string.Empty).Trim();
        }

        var host = (SqlServerHost ?? string.Empty).Trim();
        var db = (Database ?? string.Empty).Trim();
        var port = int.TryParse(SqlServerPort, out var parsedPort) && parsedPort > 0 ? parsedPort : 1433;
        if (string.IsNullOrWhiteSpace(host) || string.IsNullOrWhiteSpace(db))
        {
            return string.Empty;
        }

        var queryParts = new List<string>();
        if (UseWindowsAuth)
        {
            queryParts.Add("windows_auth=true");
        }
        else
        {
            if (!string.IsNullOrWhiteSpace(User))
            {
                queryParts.Add($"user={Uri.EscapeDataString(User.Trim())}");
            }
            if (!string.IsNullOrWhiteSpace(Password))
            {
                queryParts.Add($"password={Uri.EscapeDataString(Password.Trim())}");
            }
        }

        var query = queryParts.Count == 0 ? string.Empty : $"?{string.Join("&", queryParts)}";
        return $"{host}:{port}/{db}{query}";
    }

    public SqlConnectionProfile WithoutSecret()
    {
        return this with { Password = string.Empty };
    }
}
