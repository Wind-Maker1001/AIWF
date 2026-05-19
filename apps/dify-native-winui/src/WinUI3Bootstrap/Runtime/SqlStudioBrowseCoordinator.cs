using System.Text.Json.Nodes;

namespace AIWF.Native.Runtime;

public sealed class SqlStudioBrowseCoordinator
{
    private readonly WorkflowRunnerAdapter _runnerAdapter;

    public SqlStudioBrowseCoordinator(WorkflowRunnerAdapter runnerAdapter)
    {
        _runnerAdapter = runnerAdapter;
    }

    public async Task<SchemaBrowserState> ValidateAndLoadSchemasAsync(
        SqlConnectionProfile profile,
        string bridgeBaseUrl,
        string apiKey,
        CancellationToken cancellationToken = default)
    {
        var accelUrl = profile.ResolveAccelUrl(bridgeBaseUrl);
        await _runnerAdapter.PostJsonAsync(
            accelUrl,
            apiKey,
            "/operators/data_source_browser_v1",
            BuildPayload(profile, "validate_connection"),
            cancellationToken);

        var schemasPayload = await _runnerAdapter.PostJsonAsync(
            accelUrl,
            apiKey,
            "/operators/data_source_browser_v1",
            BuildPayload(profile, "list_schemas"),
            cancellationToken);

        return SqlStudioResultMapper.MergeBrowseResponse(
            SchemaBrowserState.Empty,
            schemasPayload,
            "list_schemas");
    }

    public async Task<SchemaBrowserState> LoadTablesAsync(
        SqlConnectionProfile profile,
        string bridgeBaseUrl,
        string apiKey,
        SchemaBrowserState current,
        string schema,
        CancellationToken cancellationToken = default)
    {
        var payload = await _runnerAdapter.PostJsonAsync(
            profile.ResolveAccelUrl(bridgeBaseUrl),
            apiKey,
            "/operators/data_source_browser_v1",
            BuildPayload(profile, "list_tables", schema: schema),
            cancellationToken);

        return SqlStudioResultMapper.MergeBrowseResponse(current, payload, "list_tables");
    }

    public async Task<SchemaBrowserState> DescribeTableAsync(
        SqlConnectionProfile profile,
        string bridgeBaseUrl,
        string apiKey,
        SchemaBrowserState current,
        string schema,
        string table,
        CancellationToken cancellationToken = default)
    {
        var payload = await _runnerAdapter.PostJsonAsync(
            profile.ResolveAccelUrl(bridgeBaseUrl),
            apiKey,
            "/operators/data_source_browser_v1",
            BuildPayload(profile, "describe_table", schema: schema, table: table),
            cancellationToken);

        return SqlStudioResultMapper.MergeBrowseResponse(current, payload, "describe_table");
    }

    private static JsonObject BuildPayload(
        SqlConnectionProfile profile,
        string op,
        string? schema = null,
        string? table = null)
    {
        var payload = new JsonObject
        {
            ["source_type"] = profile.NormalizedSourceType,
            ["source"] = profile.BuildRuntimeSource(),
            ["op"] = op,
        };
        if (!string.IsNullOrWhiteSpace(schema))
        {
            payload["schema"] = schema.Trim();
        }
        if (!string.IsNullOrWhiteSpace(table))
        {
            payload["table"] = table.Trim();
        }
        return payload;
    }
}
