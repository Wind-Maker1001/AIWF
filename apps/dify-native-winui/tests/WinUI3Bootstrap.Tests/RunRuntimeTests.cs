using System.Net;
using System.Text;
using System.Text.Json.Nodes;
using AIWF.Native.Runtime;
using Xunit;

namespace AIWF.Native.Tests;

public sealed class RunRuntimeTests
{
    [Fact]
    public void TryParse_AcceptsStringNumericAndSecondsFallback()
    {
        const string json = """
            {
              "ok": "true",
              "job_id": 123,
              "flow": "cleaning",
              "seconds": "0.912",
              "artifacts": [
                { "artifact_id": 456, "kind": "xlsx", "path": "C:/tmp/report.xlsx" },
                "skip-me",
                { "artifact_id": "profile", "kind": 7, "path": 999 }
              ]
            }
            """;

        var parsed = RunResultParser.TryParse(json, out var data);

        Assert.True(parsed);
        Assert.True(data.Ok);
        Assert.Equal("123", data.JobId);
        Assert.Equal("cleaning", data.RunMode);
        Assert.Equal(912, data.DurationMs);
        Assert.Collection(
            data.Artifacts,
            artifact =>
            {
                Assert.Equal("456", artifact.ArtifactId);
                Assert.Equal("xlsx", artifact.Kind);
                Assert.Equal("C:/tmp/report.xlsx", artifact.Path);
            },
            artifact =>
            {
                Assert.Equal("profile", artifact.ArtifactId);
                Assert.Equal("7", artifact.Kind);
                Assert.Equal("999", artifact.Path);
            });
    }

    [Fact]
    public void TryParse_FallsBackToNestedDataFields()
    {
        const string json = """
            {
              "data": {
                "ok": true,
                "job_id": "job-nested",
                "mode": "cleaning",
                "duration_ms": 512,
                "artifacts": [
                  { "artifact_id": "a-1", "kind": "json", "path": "C:/tmp/out.json" }
                ]
              }
            }
            """;

        var parsed = RunResultParser.TryParse(json, out var data);

        Assert.True(parsed);
        Assert.True(data.Ok);
        Assert.Equal("job-nested", data.JobId);
        Assert.Equal("cleaning", data.RunMode);
        Assert.Equal(512, data.DurationMs);
        Assert.Single(data.Artifacts);
    }

    [Fact]
    public void Validate_AllowsBlankJobId()
    {
        var result = RunInputValidator.Validate(new RunInputData(
            "http://127.0.0.1:18081",
            "native",
            "",
            "cleaning",
            "Smoke Report"));

        Assert.True(result.IsValid);
        Assert.DoesNotContain("job_id", result.MissingKeys);
    }

    [Fact]
    public void CreateParseFailureState_UsesExplicitMessage()
    {
        var state = ResultPanelController.CreateParseFailureState();

        Assert.Equal("0 项", state.ArtifactsCountText);
        Assert.Equal("运行完成，但返回结果无法解析。", state.RunResultText);
        Assert.Equal("-", state.OkMetricText);
    }

    [Fact]
    public async Task ExecuteAsync_CreatesJobWhenJobIdMissing()
    {
        var requests = new List<Uri>();
        using var http = new HttpClient(new StubHttpMessageHandler(request =>
        {
            requests.Add(request.RequestUri!);

            if (request.RequestUri!.AbsoluteUri == "http://127.0.0.1:18080/api/v1/jobs/create?owner=native")
            {
                return Json(HttpStatusCode.OK, """{"job_id":"auto-123"}""");
            }

            if (request.RequestUri.AbsoluteUri == "http://127.0.0.1:18081/jobs/auto-123/run/cleaning")
            {
                return Json(HttpStatusCode.OK, """{"ok":true}""");
            }

            return Json(HttpStatusCode.NotFound, """{"ok":false}""");
        }));
        var coordinator = new RunFlowCoordinator(http, new WorkflowRunnerAdapter(http));

        var result = await coordinator.ExecuteAsync(
            "http://127.0.0.1:18081",
            apiKey: "",
            owner: "native",
            jobId: "",
            flow: "cleaning",
            payload: new JsonObject());

        Assert.Equal("auto-123", result.EffectiveJobId);
        Assert.Equal("预检创建作业：auto-123", result.RetryInfo);
        Assert.False(result.RetriedAfterServerError);
        Assert.Contains(requests, uri => uri.AbsoluteUri == "http://127.0.0.1:18080/api/v1/jobs/create?owner=native");
        Assert.Contains(requests, uri => uri.AbsoluteUri == "http://127.0.0.1:18081/jobs/auto-123/run/cleaning");
    }

    [Fact]
    public async Task ExecuteAsync_ThrowsWhenJobIdCannotBeEnsured()
    {
        var requests = new List<Uri>();
        using var http = new HttpClient(new StubHttpMessageHandler(request =>
        {
            requests.Add(request.RequestUri!);

            if (request.RequestUri!.AbsoluteUri == "http://127.0.0.1:18080/api/v1/jobs/create?owner=native")
            {
                return Json(HttpStatusCode.ServiceUnavailable, """{"ok":false}""");
            }

            return Json(HttpStatusCode.NotFound, """{"ok":false}""");
        }));
        var coordinator = new RunFlowCoordinator(http, new WorkflowRunnerAdapter(http));

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => coordinator.ExecuteAsync(
            "http://127.0.0.1:18081",
            apiKey: "",
            owner: "native",
            jobId: "",
            flow: "cleaning",
            payload: new JsonObject()));

        Assert.Contains("无法自动创建作业", ex.Message);
        Assert.DoesNotContain(requests, uri => uri.AbsoluteUri.Contains("/jobs//run/"));
    }

    private static HttpResponseMessage Json(HttpStatusCode statusCode, string json)
    {
        return new HttpResponseMessage(statusCode)
        {
            Content = new StringContent(json, Encoding.UTF8, "application/json")
        };
    }

    private sealed class StubHttpMessageHandler(Func<HttpRequestMessage, HttpResponseMessage> responder) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            return Task.FromResult(responder(request));
        }
    }
}
