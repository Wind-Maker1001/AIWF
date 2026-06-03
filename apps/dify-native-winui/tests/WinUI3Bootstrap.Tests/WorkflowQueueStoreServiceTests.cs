using System.Text.Json;
using System.Text.Json.Nodes;
using AIWF.Native.Runtime;
using Xunit;

namespace AIWF.Native.Tests;

public sealed class WorkflowQueueStoreServiceTests
{
    [Fact]
    public void SaveControl_CancelTask_AndRetryTask_PersistNormalizedState()
    {
        var tempDir = Path.Combine(Path.GetTempPath(), "aiwf-queue-store-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tempDir);
        try
        {
            var service = new WorkflowQueueStoreService(
                Path.Combine(tempDir, "workflow_task_queue.json"),
                Path.Combine(tempDir, "workflow_queue_control.json"),
                now: () => DateTimeOffset.Parse("2026-06-03T15:20:00Z"),
                randomHex: () => "cafebabe");

            var control = service.SaveControl(new WorkflowQueueControlState(
                true,
                new Dictionary<string, int>
                {
                    ["alpha"] = 9,
                    ["beta"] = 0,
                }));

            Assert.True(control.Paused);
            Assert.Single(control.Quotas);
            Assert.Equal(8, control.Quotas["alpha"]);

            var queued = service.EnqueueTask(new WorkflowQueueTaskItem(
                TaskId: "task_demo",
                Label: "Demo Flow",
                DispatchKind: "legacy_flow",
                DispatchBaseUrl: "http://127.0.0.1:18081",
                WorkflowId: "job_demo",
                Owner: "native",
                RequestedJobId: "job_demo",
                Flow: "cleaning",
                Priority: 100,
                Status: "queued",
                CreatedAt: "2026-06-03T15:19:00Z",
                StartedAt: string.Empty,
                FinishedAt: string.Empty,
                RunId: string.Empty,
                Payload: new JsonObject
                {
                    ["flow"] = "cleaning",
                },
                ResultPayload: new JsonObject(),
                Error: string.Empty));

            var canceled = service.CancelTask(queued.TaskId);
            Assert.NotNull(canceled);
            Assert.Equal("canceled", canceled!.Status);

            var retried = service.RetryTask(queued.TaskId);
            Assert.Equal("queued", retried.Status);
            Assert.Equal("2026-06-03T15:20:00.0000000+00:00", retried.CreatedAt);
            Assert.Equal("20260603152000_cafebabe", retried.TaskId);

            var queueJson = JsonNode.Parse(File.ReadAllText(service.QueuePath))!.AsObject();
            Assert.False(queueJson.ContainsKey("schema_version"));
            Assert.Equal(2, queueJson["items"]!.AsArray().Count);
        }
        finally
        {
            Directory.Delete(tempDir, recursive: true);
        }
    }

    [Fact]
    public void ListTasks_AndLoadControl_ReadLegacyMinimalContainers()
    {
        var tempDir = Path.Combine(Path.GetTempPath(), "aiwf-queue-store-legacy-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tempDir);
        try
        {
            var queuePath = Path.Combine(tempDir, "workflow_task_queue.json");
            var controlPath = Path.Combine(tempDir, "workflow_queue_control.json");
            File.WriteAllText(queuePath, """
                {
                  "items": [
                    {
                      "task_id": "task_legacy",
                      "label": "Legacy Flow",
                      "dispatch_kind": "legacy_flow",
                      "dispatch_base_url": "http://127.0.0.1:18081",
                      "workflow_id": "job_legacy",
                      "owner": "native",
                      "requested_job_id": "job_legacy",
                      "flow": "cleaning",
                      "priority": 7,
                      "status": "queued",
                      "created_at": "2026-06-03T15:30:00Z",
                      "payload": {
                        "flow": "cleaning"
                      },
                      "result": {}
                    }
                  ]
                }
                """);
            File.WriteAllText(controlPath, """
                {
                  "paused": false,
                  "quotas": {
                    "alpha": 2,
                    "beta": 99,
                    "gamma": 0
                  }
                }
                """);

            var service = new WorkflowQueueStoreService(queuePath, controlPath);
            var item = Assert.Single(service.ListTasks());
            Assert.Equal("task_legacy", item.TaskId);
            Assert.Equal("legacy_flow", item.DispatchKind);
            Assert.Equal("queued", item.Status);
            Assert.Equal("cleaning", item.Flow);

            var control = service.LoadControl();
            Assert.False(control.Paused);
            Assert.Equal(2, control.Quotas["alpha"]);
            Assert.Equal(8, control.Quotas["beta"]);
            Assert.False(control.Quotas.ContainsKey("gamma"));
        }
        finally
        {
            Directory.Delete(tempDir, recursive: true);
        }
    }
}
