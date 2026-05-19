using System.Net;
using System.Text;
using System.Text.Json.Nodes;
using AIWF.Native.Runtime;
using Xunit;

namespace AIWF.Native.Tests;

public sealed class SqlStudioBrowseCoordinatorTests
{
    [Fact]
    public async Task ValidateAndLoadSchemasAsync_ValidatesThenLoadsSchemas()
    {
        var requests = new List<JsonObject>();
        using var http = new HttpClient(new StubHttpMessageHandler(async request =>
        {
            requests.Add(JsonNode.Parse(await request.Content!.ReadAsStringAsync())!.AsObject());
            return requests.Count switch
            {
                1 => Json(HttpStatusCode.OK, """{"ok":true}"""),
                2 => Json(HttpStatusCode.OK, """{"items":[{"name":"main","kind":"schema"}],"stats":{"elapsed_ms":12}}"""),
                _ => Json(HttpStatusCode.NotFound, """{"error":"unexpected"}""")
            };
        }));
        var coordinator = new SqlStudioBrowseCoordinator(new WorkflowRunnerAdapter(http));

        var state = await coordinator.ValidateAndLoadSchemasAsync(
            SqlConnectionProfile.Default with
            {
                SourceType = SqlConnectionProfile.Sqlite,
                SQLitePath = @"D:\demo.db"
            },
            "http://127.0.0.1:18081",
            apiKey: "");

        Assert.Equal(2, requests.Count);
        Assert.Equal("validate_connection", requests[0]["op"]?.GetValue<string>());
        Assert.Equal("list_schemas", requests[1]["op"]?.GetValue<string>());
        Assert.Single(state.Schemas);
        Assert.Equal("main", state.Schemas[0].Name);
    }

    [Fact]
    public async Task LoadTablesAsync_MergesTableItemsIntoCurrentState()
    {
        using var http = new HttpClient(new StubHttpMessageHandler(_ =>
            Task.FromResult(Json(HttpStatusCode.OK, """{"items":[{"name":"sales","kind":"table","schema":"main"}],"stats":{"elapsed_ms":2}}"""))));
        var coordinator = new SqlStudioBrowseCoordinator(new WorkflowRunnerAdapter(http));
        var current = SchemaBrowserState.Empty with { SelectedSchema = "main" };

        var state = await coordinator.LoadTablesAsync(
            SqlConnectionProfile.Default with { SourceType = SqlConnectionProfile.Sqlite, SQLitePath = @"D:\demo.db" },
            "http://127.0.0.1:18081",
            apiKey: "",
            current,
            "main");

        Assert.Single(state.Tables);
        Assert.Equal("sales", state.Tables[0].Name);
        Assert.Equal("main", state.SelectedSchema);
    }

    [Fact]
    public async Task DescribeTableAsync_MergesColumnMetadataIntoCurrentState()
    {
        using var http = new HttpClient(new StubHttpMessageHandler(_ =>
            Task.FromResult(Json(HttpStatusCode.OK, """{"columns":[{"name":"id","data_type":"int","nullable":false}],"stats":{"elapsed_ms":1}}"""))));
        var coordinator = new SqlStudioBrowseCoordinator(new WorkflowRunnerAdapter(http));
        var current = SchemaBrowserState.Empty with { SelectedSchema = "main", SelectedTable = "sales" };

        var state = await coordinator.DescribeTableAsync(
            SqlConnectionProfile.Default with { SourceType = SqlConnectionProfile.Sqlite, SQLitePath = @"D:\demo.db" },
            "http://127.0.0.1:18081",
            apiKey: "",
            current,
            "main",
            "sales");

        Assert.Single(state.Columns);
        Assert.Equal("id", state.Columns[0].Name);
        Assert.False(state.Columns[0].Nullable);
    }

    private static HttpResponseMessage Json(HttpStatusCode statusCode, string json)
    {
        return new HttpResponseMessage(statusCode)
        {
            Content = new StringContent(json, Encoding.UTF8, "application/json")
        };
    }

    private sealed class StubHttpMessageHandler(Func<HttpRequestMessage, Task<HttpResponseMessage>> responder) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            return responder(request);
        }
    }
}
