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

    [Theory]
    [InlineData("C:/tmp/report.xlsx", true)]
    [InlineData("C:/tmp/report.docx", true)]
    [InlineData("C:/tmp/report.exe", false)]
    [InlineData("C:/tmp/report.ps1", false)]
    [InlineData("C:/tmp/report", false)]
    public void CanOpenArtifactPath_AllowsOnlySafeArtifactExtensions(string path, bool expected)
    {
        Assert.Equal(expected, RunResultParser.CanOpenArtifactPath(path));
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

        Assert.Equal("0 items", state.ArtifactsCountText);
        Assert.Equal("Run completed, but the response could not be parsed.", state.RunResultText);
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
        Assert.Equal("Preflight created job: auto-123", result.RetryInfo);
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

        Assert.Contains("Unable to auto-create a job", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain(requests, uri => uri.AbsoluteUri.Contains("/jobs//run/"));
    }

    [Fact]
    public async Task ExecuteAsync_DoesNotAutoRetryAfterServerError()
    {
        var requests = new List<Uri>();
        using var http = new HttpClient(new StubHttpMessageHandler(request =>
        {
            requests.Add(request.RequestUri!);

            if (request.RequestUri!.AbsoluteUri == "http://127.0.0.1:18080/api/v1/jobs/job-500")
            {
                return Json(HttpStatusCode.OK, """{"job_id":"job-500"}""");
            }

            if (request.RequestUri!.AbsoluteUri == "http://127.0.0.1:18081/jobs/job-500/run/cleaning")
            {
                return Json(HttpStatusCode.InternalServerError, """{"ok":false,"error":"boom"}""");
            }

            return Json(HttpStatusCode.NotFound, """{"ok":false}""");
        }));
        var coordinator = new RunFlowCoordinator(http, new WorkflowRunnerAdapter(http));

        var result = await coordinator.ExecuteAsync(
            "http://127.0.0.1:18081",
            apiKey: "",
            owner: "native",
            jobId: "job-500",
            flow: "cleaning",
            payload: new JsonObject());

        Assert.Equal(HttpStatusCode.InternalServerError, result.StatusCode);
        Assert.False(result.IsSuccessStatusCode);
        Assert.Equal("job-500", result.EffectiveJobId);
        Assert.Equal("Not retried", result.RetryInfo);
        Assert.False(result.RetriedAfterServerError);
        Assert.Equal(2, requests.Count);
        Assert.Contains(requests, uri => uri.AbsoluteUri == "http://127.0.0.1:18080/api/v1/jobs/job-500");
        Assert.Contains(requests, uri => uri.AbsoluteUri == "http://127.0.0.1:18081/jobs/job-500/run/cleaning");
        Assert.DoesNotContain(requests, uri => uri.AbsoluteUri == "http://127.0.0.1:18080/api/v1/jobs/create?owner=native");
    }

    [Fact]
    public async Task PrecheckCleaningAsync_PostsToAuthoritativePrecheckRoute()
    {
        var requests = new List<Uri>();
        using var http = new HttpClient(new StubHttpMessageHandler(request =>
        {
            requests.Add(request.RequestUri!);
            if (request.RequestUri!.AbsoluteUri == "http://127.0.0.1:18081/cleaning/precheck")
            {
                return Json(HttpStatusCode.OK, """{"ok":true,"precheck_action":"allow"}""");
            }

            return Json(HttpStatusCode.NotFound, """{"ok":false}""");
        }));
        var adapter = new WorkflowRunnerAdapter(http);

        var result = await adapter.PrecheckCleaningAsync(
            "http://127.0.0.1:18081",
            apiKey: "",
            payload: new JsonObject { ["input_path"] = "D:/tmp/input.csv" });

        Assert.True(result["ok"]?.GetValue<bool>());
        Assert.Equal("allow", result["precheck_action"]?.GetValue<string>());
        Assert.Contains(requests, uri => uri.AbsoluteUri == "http://127.0.0.1:18081/cleaning/precheck");
    }

    [Fact]
    public void BuildCleaningPrecheckPayload_UsesDesktopGuardrailDefaults()
    {
        var payload = RunPayloadBuilder.BuildCleaningPrecheckPayload(new CleaningPrecheckPayloadInput(
            "D:/tmp/input.csv",
            "finance_report_v1"));

        Assert.Equal("D:/tmp/input.csv", payload["input_path"]?.GetValue<string>());
        Assert.Equal("finance_report_v1", payload["cleaning_template"]?.GetValue<string>());
        Assert.Equal("auto", payload["header_mapping_mode"]?.GetValue<string>());
        Assert.False(payload["blank_output_expected"]?.GetValue<bool>() ?? true);
        Assert.Equal("block", payload["profile_mismatch_action"]?.GetValue<string>());
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
