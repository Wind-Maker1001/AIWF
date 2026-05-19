using System.Net;
using System.Text;
using AIWF.Native.Runtime;
using Xunit;

namespace AIWF.Native.Tests;

public sealed class WorkflowVersionAdminCoordinatorTests
{
    [Fact]
    public async Task RefreshVersionsAsync_BypassesCacheAndWritesFreshList()
    {
        var root = Path.Combine(Path.GetTempPath(), $"aiwf-workflow-version-cache-{Guid.NewGuid():N}");
        Directory.CreateDirectory(root);
        try
        {
            var cache = new WorkflowVersionCacheService(
                Path.Combine(root, "workflow-version-cache.json"),
                Path.Combine(root, "workflow-version-cache-metrics.json"),
                () => "2026-05-19T10:25:00Z");
            var calls = 0;
            using var http = new HttpClient(new StubHttpMessageHandler(request =>
            {
                calls += 1;
                if (request.RequestUri!.AbsoluteUri.EndsWith("/governance/meta/control-plane", StringComparison.Ordinal))
                {
                    return GovernanceBridgeClientTests_DefaultBoundaryResponse();
                }
                return Json(HttpStatusCode.OK, """
                    {
                      "ok": true,
                      "items": [
                        {
                          "version_id": "ver_a",
                          "workflow_name": "Workflow A",
                          "workflow_id": "wf_a",
                          "ts": "2026-05-19T10:00:00Z",
                          "provider": "glue-python",
                          "owner": "glue-python"
                        }
                      ]
                    }
                    """);
            }));
            var coordinator = new WorkflowVersionAdminCoordinator(new GovernanceBridgeClient(http), cache);

            var first = await coordinator.RefreshVersionsAsync("http://127.0.0.1:18081", "");
            var second = await coordinator.RefreshVersionsAsync("http://127.0.0.1:18081", "");

            Assert.Equal(3, calls);
            Assert.Single(first.Items);
            Assert.Single(second.Items);
            Assert.True(second.Stats.Sets >= 2);
        }
        finally
        {
            if (Directory.Exists(root))
            {
                Directory.Delete(root, true);
            }
        }
    }

    [Fact]
    public async Task CompareVersionsAsync_UsesCacheOnRepeatedRequest()
    {
        var root = Path.Combine(Path.GetTempPath(), $"aiwf-workflow-version-cache-{Guid.NewGuid():N}");
        Directory.CreateDirectory(root);
        try
        {
            var cache = new WorkflowVersionCacheService(
                Path.Combine(root, "workflow-version-cache.json"),
                Path.Combine(root, "workflow-version-cache-metrics.json"),
                () => "2026-05-19T10:25:00Z");
            var compareCalls = 0;
            using var http = new HttpClient(new StubHttpMessageHandler(async request =>
            {
                if (request.RequestUri!.AbsoluteUri.EndsWith("/governance/meta/control-plane", StringComparison.Ordinal))
                {
                    return GovernanceBridgeClientTests_DefaultBoundaryResponse();
                }
                compareCalls += 1;
                var body = await request.Content!.ReadAsStringAsync();
                Assert.Contains("\"version_a\":\"ver_a\"", body, StringComparison.Ordinal);
                Assert.Contains("\"version_b\":\"ver_b\"", body, StringComparison.Ordinal);
                return Json(HttpStatusCode.OK, """
                    {
                      "ok": true,
                      "provider": "glue-python",
                      "summary": {
                        "version_a": "ver_a",
                        "version_b": "ver_b",
                        "changed_nodes": 1,
                        "added_edges": 1,
                        "removed_edges": 0
                      },
                      "node_diff": [
                        {
                          "id": "n1",
                          "change": "updated",
                          "type_a": "load_rows_v3",
                          "type_b": "load_rows_v3",
                          "config_changed": true
                        }
                      ]
                    }
                    """);
            }));
            var coordinator = new WorkflowVersionAdminCoordinator(new GovernanceBridgeClient(http), cache);

            var first = await coordinator.CompareVersionsAsync("http://127.0.0.1:18081", "", "ver_a", "ver_b");
            var second = await coordinator.CompareVersionsAsync("http://127.0.0.1:18081", "", "ver_a", "ver_b");

            Assert.Equal(1, compareCalls);
            Assert.NotNull(first.Result);
            Assert.True(second.FromCache);
            Assert.True(second.Stats.Hits >= 1);
        }
        finally
        {
            if (Directory.Exists(root))
            {
                Directory.Delete(root, true);
            }
        }
    }

    [Fact]
    public async Task CompareVersionsAsync_DoesNotCacheFailures()
    {
        var root = Path.Combine(Path.GetTempPath(), $"aiwf-workflow-version-cache-{Guid.NewGuid():N}");
        Directory.CreateDirectory(root);
        try
        {
            var cache = new WorkflowVersionCacheService(
                Path.Combine(root, "workflow-version-cache.json"),
                Path.Combine(root, "workflow-version-cache-metrics.json"),
                () => "2026-05-19T10:25:00Z");
            using var http = new HttpClient(new StubHttpMessageHandler(request =>
            {
                if (request.RequestUri!.AbsoluteUri.EndsWith("/governance/meta/control-plane", StringComparison.Ordinal))
                {
                    return GovernanceBridgeClientTests_DefaultBoundaryResponse();
                }
                return Json(HttpStatusCode.BadRequest, """
                    {
                      "ok": false,
                      "error": "version not found",
                      "error_code": "workflow_graph_invalid",
                      "error_items": [
                        {
                          "path": "request.version_a",
                          "code": "missing",
                          "message": "version not found"
                        }
                      ]
                    }
                    """);
            }));
            var coordinator = new WorkflowVersionAdminCoordinator(new GovernanceBridgeClient(http), cache);

            var out1 = await coordinator.CompareVersionsAsync("http://127.0.0.1:18081", "", "ver_a", "ver_b");
            var out2 = await coordinator.CompareVersionsAsync("http://127.0.0.1:18081", "", "ver_a", "ver_b");

            Assert.Null(out1.Result);
            Assert.Equal("workflow_graph_invalid", out1.ErrorCode);
            Assert.False(out2.FromCache);
            Assert.False(cache.TryGetCompareResult("ver_a", "ver_b", out _));
        }
        finally
        {
            if (Directory.Exists(root))
            {
                Directory.Delete(root, true);
            }
        }
    }

    private static HttpResponseMessage Json(HttpStatusCode statusCode, string json)
    {
        return new HttpResponseMessage(statusCode)
        {
            Content = new StringContent(json, Encoding.UTF8, "application/json")
        };
    }

    private static HttpResponseMessage GovernanceBridgeClientTests_DefaultBoundaryResponse()
    {
        return Json(HttpStatusCode.OK, """
            {
              "ok": true,
              "boundary": {
                "schema_version": "governance_surface.v1",
                "status": "effective_second_control_plane",
                "control_plane_role": "governance_state",
                "governance_state_control_plane_owner": "glue-python",
                "job_lifecycle_control_plane_owner": "base-java",
                "operator_semantics_authority_owner": "accel-rust",
                "workflow_authoring_surface_owner": "dify-desktop",
                "meta_route": "/governance/meta/control-plane",
                "governance_surfaces": [
                  {
                    "capability": "workflow_versions",
                    "route_prefix": "/governance/workflow-versions",
                    "owned_route_prefixes": ["/governance/workflow-versions"],
                    "state_owner": "glue-python",
                    "control_plane_role": "governance_state",
                    "lifecycle_mutation_allowed": false
                  }
                ]
              }
            }
            """);
    }

    private sealed class StubHttpMessageHandler : HttpMessageHandler
    {
        private readonly Func<HttpRequestMessage, Task<HttpResponseMessage>> _responder;

        public StubHttpMessageHandler(Func<HttpRequestMessage, HttpResponseMessage> responder)
        {
            _responder = request => Task.FromResult(responder(request));
        }

        public StubHttpMessageHandler(Func<HttpRequestMessage, Task<HttpResponseMessage>> responder)
        {
            _responder = responder;
        }

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            return _responder(request);
        }
    }
}
