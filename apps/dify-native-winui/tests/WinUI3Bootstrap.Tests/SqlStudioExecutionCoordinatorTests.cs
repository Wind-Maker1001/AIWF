using System.Net;
using System.Text;
using System.Text.Json.Nodes;
using AIWF.Native.Runtime;
using Xunit;

namespace AIWF.Native.Tests;

public sealed class SqlStudioExecutionCoordinatorTests
{
    [Fact]
    public async Task PreviewAsync_PreservesTextOwnedSql()
    {
        JsonObject? captured = null;
        using var http = new HttpClient(new StubHttpMessageHandler(async request =>
        {
            captured = JsonNode.Parse(await request.Content!.ReadAsStringAsync())!.AsObject();
            return Json(HttpStatusCode.OK, """{"ok":true,"status":"done","rows":[{"id":1,"name":"Alice"}],"stats":{"elapsed_ms":4}}""");
        }));
        var coordinator = new SqlStudioExecutionCoordinator(new WorkflowRunnerAdapter(http));
        var result = await coordinator.PreviewAsync(
            SqlConnectionProfile.Default with { SourceType = SqlConnectionProfile.Sqlite, SQLitePath = @"D:\demo.db" },
            SqlBuilderDraft.Empty,
            new SqlTextDraft("SELECT * FROM users", true),
            "http://127.0.0.1:18081",
            apiKey: "");

        Assert.NotNull(captured);
        Assert.Equal("SELECT * FROM users", captured!["query"]?.GetValue<string>());
        Assert.True(result.Success);
        Assert.Equal("SELECT * FROM users", result.ExecutedSql);
        Assert.Equal("SELECT * FROM users", result.EffectiveTextDraft.Text);
        Assert.Single(result.PreviewState.GridRows);
    }

    [Fact]
    public async Task PreviewAsync_UsesBuilderGeneratedSqlWhenDraftIsNotTextOwned()
    {
        JsonObject? captured = null;
        using var http = new HttpClient(new StubHttpMessageHandler(async request =>
        {
            captured = JsonNode.Parse(await request.Content!.ReadAsStringAsync())!.AsObject();
            return Json(HttpStatusCode.OK, """{"ok":true,"status":"done","rows":[],"stats":{"elapsed_ms":1}}""");
        }));
        var coordinator = new SqlStudioExecutionCoordinator(new WorkflowRunnerAdapter(http));
        var profile = SqlConnectionProfile.Default with
        {
            SourceType = SqlConnectionProfile.Sqlite,
            SQLitePath = @"D:\demo.db"
        };
        var draft = SqlBuilderDraft.Empty with
        {
            Schema = "main",
            Table = "sales",
            Limit = 50
        };

        var result = await coordinator.PreviewAsync(
            profile,
            draft,
            SqlTextDraft.Empty,
            "http://127.0.0.1:18081",
            apiKey: "");

        var expectedSql = SqlStudioDraftController.SyncGeneratedSql(draft, profile, SqlTextDraft.Empty).Text;
        Assert.NotNull(captured);
        Assert.Equal(expectedSql, captured!["query"]?.GetValue<string>());
        Assert.Equal(expectedSql, result.ExecutedSql);
        Assert.Equal(expectedSql, result.EffectiveTextDraft.Text);
    }

    [Theory]
    [InlineData(SqlConnectionProfile.Sqlite, "EXPLAIN QUERY PLAN ")]
    [InlineData(SqlConnectionProfile.SqlServer, "SET SHOWPLAN_TEXT ON; ")]
    [InlineData(SqlConnectionProfile.Postgres, "EXPLAIN ANALYZE ")]
    public async Task ExplainAsync_UsesExpectedPrefix(string sourceType, string expectedPrefix)
    {
        JsonObject? captured = null;
        using var http = new HttpClient(new StubHttpMessageHandler(async request =>
        {
            captured = JsonNode.Parse(await request.Content!.ReadAsStringAsync())!.AsObject();
            return Json(HttpStatusCode.OK, """{"rows":[{"plan":"scan users"}]}""");
        }));
        var coordinator = new SqlStudioExecutionCoordinator(new WorkflowRunnerAdapter(http));
        var profile = SqlConnectionProfile.Default with
        {
            SourceType = sourceType,
            SQLitePath = @"D:\demo.db",
            SqlServerHost = "localhost",
            Database = "AIWF"
        };

        var result = await coordinator.ExplainAsync(
            profile,
            SqlBuilderDraft.Empty,
            new SqlTextDraft("SELECT * FROM users", true),
            "http://127.0.0.1:18081",
            apiKey: "");

        Assert.NotNull(captured);
        Assert.StartsWith(expectedPrefix, captured!["query"]?.GetValue<string>() ?? string.Empty, StringComparison.Ordinal);
        Assert.True(result.Success);
        Assert.Contains("scan users", result.ExplainText, StringComparison.Ordinal);
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
