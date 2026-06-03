using System.Net;
using System.Text;
using System.Text.Json.Nodes;
using AIWF.Native.Runtime;
using Xunit;

namespace AIWF.Native.Tests;

public sealed class WorkflowQueueAdminCoordinatorTests
{
    [Fact]
    public async Task EnqueueLegacyFlowAsync_ProcessesTaskAndMirrorsRun()
    {
        var tempDir = CreateTempDir();
        try
        {
            using var http = new HttpClient(new StubHttpMessageHandler(request =>
            {
                if (request.RequestUri!.AbsoluteUri == "http://127.0.0.1:18080/api/v1/jobs/job-legacy")
                {
                    return Json(HttpStatusCode.OK, """{"job_id":"job-legacy"}""");
                }

                if (request.RequestUri.AbsoluteUri == "http://127.0.0.1:18081/jobs/job-legacy/run/cleaning")
                {
                    return Json(HttpStatusCode.OK, """
                        {
                          "ok": true,
                          "status": "done",
                          "run_id": "run_legacy_1",
                          "workflow_id": "job-legacy",
                          "node_runs": [
                            {
                              "id": "load_1",
                              "status": "done",
                              "started_at": "2026-06-03T16:00:00Z",
                              "ended_at": "2026-06-03T16:00:01Z",
                              "seconds": 1
                            }
                          ]
                        }
                        """);
                }

                return Json(HttpStatusCode.NotFound, """{"ok":false}""");
            }));
            var coordinator = CreateCoordinator(tempDir, http);

            var state = await coordinator.EnqueueLegacyFlowAsync(
                "http://127.0.0.1:18081",
                "token",
                "native",
                "job-legacy",
                "cleaning",
                new JsonObject
                {
                    ["flow"] = "cleaning",
                    ["params"] = new JsonObject
                    {
                        ["region"] = "cn",
                    },
                },
                "Legacy Flow");

            var task = Assert.Single(state.QueueItems);
            Assert.Equal("done", task.Status);
            Assert.Equal("run_legacy_1", task.RunId);
            var run = Assert.Single(state.RunHistory);
            Assert.Equal("run_legacy_1", run.RunId);
            Assert.Equal("legacy_flow", run.RunRequestKind);
        }
        finally
        {
            Directory.Delete(tempDir, recursive: true);
        }
    }

    [Fact]
    public async Task EnqueueCanvasDraftAsync_ProcessesTaskAndMirrorsRun()
    {
        var tempDir = CreateTempDir();
        try
        {
            using var http = new HttpClient(new StubHttpMessageHandler(request =>
            {
                if (request.RequestUri!.AbsoluteUri == "http://127.0.0.1:18082/operators/workflow_draft_run_v1")
                {
                    return Json(HttpStatusCode.OK, """
                        {
                          "ok": true,
                          "status": "done",
                          "run_id": "run_canvas_1",
                          "workflow_id": "wf_canvas",
                          "node_runs": [
                            {
                              "id": "load_1",
                              "status": "done",
                              "started_at": "2026-06-03T16:05:00Z",
                              "ended_at": "2026-06-03T16:05:01Z",
                              "seconds": 1
                            }
                          ]
                        }
                        """);
                }

                return Json(HttpStatusCode.NotFound, """{"ok":false}""");
            }));
            var coordinator = CreateCoordinator(tempDir, http);

            var state = await coordinator.EnqueueCanvasDraftAsync(
                "http://127.0.0.1:18082",
                "token",
                "job-canvas",
                BuildDocument(),
                "Canvas Flow");

            var task = Assert.Single(state.QueueItems);
            Assert.Equal("done", task.Status);
            Assert.Equal("run_canvas_1", task.RunId);
            var run = Assert.Single(state.RunHistory);
            Assert.Equal("run_canvas_1", run.RunId);
            Assert.Equal("draft", run.RunRequestKind);
            Assert.Equal("draft_inline", run.WorkflowDefinitionSource);
        }
        finally
        {
            Directory.Delete(tempDir, recursive: true);
        }
    }

    [Fact]
    public async Task ResumeAndProcessAsync_ProcessesQueuedTaskWhenQueueWasPaused()
    {
        var tempDir = CreateTempDir();
        try
        {
            using var http = new HttpClient(new StubHttpMessageHandler(request =>
            {
                if (request.RequestUri!.AbsoluteUri == "http://127.0.0.1:18080/api/v1/jobs/job-paused")
                {
                    return Json(HttpStatusCode.OK, """{"job_id":"job-paused"}""");
                }

                if (request.RequestUri.AbsoluteUri == "http://127.0.0.1:18081/jobs/job-paused/run/cleaning")
                {
                    return Json(HttpStatusCode.OK, """
                        {
                          "ok": true,
                          "status": "done",
                          "run_id": "run_paused_1",
                          "workflow_id": "job-paused"
                        }
                        """);
                }

                return Json(HttpStatusCode.NotFound, """{"ok":false}""");
            }));
            var queueStore = new WorkflowQueueStoreService(
                Path.Combine(tempDir, "workflow_task_queue.json"),
                Path.Combine(tempDir, "workflow_queue_control.json"));
            queueStore.SaveControl(new WorkflowQueueControlState(true, new Dictionary<string, int>()));
            var coordinator = CreateCoordinator(tempDir, http, queueStore);

            var queuedState = await coordinator.EnqueueLegacyFlowAsync(
                "http://127.0.0.1:18081",
                "token",
                "native",
                "job-paused",
                "cleaning",
                new JsonObject
                {
                    ["flow"] = "cleaning",
                },
                "Paused Flow");
            Assert.Equal("queued", Assert.Single(queuedState.QueueItems).Status);
            Assert.Empty(queuedState.RunHistory);

            var resumedState = await coordinator.ResumeAndProcessAsync("token");
            Assert.Equal("done", Assert.Single(resumedState.QueueItems).Status);
            Assert.Single(resumedState.RunHistory);
        }
        finally
        {
            Directory.Delete(tempDir, recursive: true);
        }
    }

    [Fact]
    public async Task ReplayRunAsync_UsesReferenceRouteAndMirrorsReplay()
    {
        var tempDir = CreateTempDir();
        try
        {
            HttpRequestMessage? capturedRequest = null;
            using var http = new HttpClient(new StubHttpMessageHandler(request =>
            {
                capturedRequest = request;
                if (request.RequestUri!.AbsoluteUri == "http://127.0.0.1:18081/api/v1/jobs/job-ref/run-reference")
                {
                    return Json(HttpStatusCode.OK, """
                        {
                          "ok": true,
                          "status": "done",
                          "run_id": "run_ref_replay_1",
                          "workflow_id": "wf_ref"
                        }
                        """);
                }

                return Json(HttpStatusCode.NotFound, """{"ok":false}""");
            }));
            var coordinator = CreateCoordinator(tempDir, http);
            var record = new GovernanceWorkflowRunRecordDetail(
                RunId: "run_ref_1",
                WorkflowId: "wf_ref",
                Status: "done",
                Ok: true,
                Timestamp: "2026-06-03T16:10:00Z",
                RunRequestKind: "reference",
                VersionId: "version_ref_1",
                PublishedVersionId: "version_ref_1",
                WorkflowDefinitionSource: "version_reference",
                Payload: new JsonObject
                {
                    ["job_id"] = "job-ref",
                    ["version_id"] = "version_ref_1",
                    ["params"] = new JsonObject(),
                },
                Steps: Array.Empty<GovernanceWorkflowRunStepItem>(),
                ResultPayload: new JsonObject());

            var result = await coordinator.ReplayRunAsync(
                "http://127.0.0.1:18081",
                "http://127.0.0.1:18082",
                "token",
                record);

            Assert.True(result.Ok);
            Assert.NotNull(capturedRequest);
            Assert.Equal("http://127.0.0.1:18081/api/v1/jobs/job-ref/run-reference", capturedRequest!.RequestUri!.AbsoluteUri);

            var runAudit = new WorkflowRunAuditStoreService(Path.Combine(tempDir, "run_history.jsonl"));
            var replayed = Assert.Single(runAudit.ListRuns());
            Assert.Equal("run_ref_replay_1", replayed.RunId);
            Assert.Equal("reference", replayed.RunRequestKind);
        }
        finally
        {
            Directory.Delete(tempDir, recursive: true);
        }
    }

    private static WorkflowQueueAdminCoordinator CreateCoordinator(string tempDir, HttpClient http, WorkflowQueueStoreService? queueStore = null)
    {
        queueStore ??= new WorkflowQueueStoreService(
            Path.Combine(tempDir, "workflow_task_queue.json"),
            Path.Combine(tempDir, "workflow_queue_control.json"));
        var runAudit = new WorkflowRunAuditStoreService(Path.Combine(tempDir, "run_history.jsonl"));
        return new WorkflowQueueAdminCoordinator(
            queueStore,
            runAudit,
            new RunFlowCoordinator(http, new WorkflowRunnerAdapter(http)),
            new WorkflowRunnerAdapter(http));
    }

    private static WorkflowGraphDocument BuildDocument()
    {
        return new WorkflowGraphDocument(
            "wf_canvas",
            "1.0.0",
            [
                new WorkflowGraphNodeDocument(
                    "load_1",
                    "load_rows_v3",
                    "Load",
                    "source",
                    40,
                    80,
                    new JsonObject
                    {
                        ["source_type"] = "sqlite",
                        ["source"] = "D:/demo.db",
                        ["query"] = "select * from data"
                    })
            ],
            [],
            WorkflowGraphViewportDocument.Default,
            WorkflowGraphSelectionDocument.Empty);
    }

    private static HttpResponseMessage Json(HttpStatusCode statusCode, string json)
    {
        return new HttpResponseMessage(statusCode)
        {
            Content = new StringContent(json, Encoding.UTF8, "application/json")
        };
    }

    private static string CreateTempDir()
    {
        var tempDir = Path.Combine(Path.GetTempPath(), "aiwf-queue-admin-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tempDir);
        return tempDir;
    }

    private sealed class StubHttpMessageHandler(Func<HttpRequestMessage, HttpResponseMessage> responder) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            return Task.FromResult(responder(request));
        }
    }
}
