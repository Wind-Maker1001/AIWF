using System.Net;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using AIWF.Native.Runtime;
using Xunit;

namespace AIWF.Native.Tests;

public sealed class WorkflowTemplateAuthoringCoordinatorTests
{
    [Fact]
    public async Task ApplySelectedTemplateAsync_AppliesBuiltinLocalAndPackTemplates()
    {
        using var http = new HttpClient(new StubHttpMessageHandler(async request =>
        {
            Assert.EndsWith("/operators/workflow_contract_v1/validate", request.RequestUri!.AbsoluteUri, StringComparison.Ordinal);
            var body = await request.Content!.ReadAsStringAsync();
            Assert.Contains("cn", body, StringComparison.Ordinal);
            return Json(HttpStatusCode.OK, """
                {
                  "ok": true,
                  "valid": true,
                  "status": "ok",
                  "workflow_definition": {
                    "workflow_id": "wf_template",
                    "version": "1.0.0",
                    "nodes": [
                      {
                        "id": "n1",
                        "type": "load_rows_v3",
                        "x": 20,
                        "y": 30,
                        "config": {
                          "source_type": "sqlite",
                          "source": "D:/demo.db",
                          "query": "select 'cn' as region"
                        }
                      }
                    ],
                    "edges": []
                  }
                }
                """);
        }));

        var tempDir = Path.Combine(Path.GetTempPath(), "aiwf-template-authoring-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tempDir);
        try
        {
            var coordinator = CreateCoordinator(http, tempDir);
            foreach (var template in new[]
            {
                CreateTemplate("builtin_tpl", "Builtin Template", "builtin"),
                CreateTemplate("local_tpl", "Local Template", "local"),
                CreateTemplate("pack_tpl", "Pack Template", "pack", "pack_demo", "Demo Pack")
            })
            {
                var result = await coordinator.ApplySelectedTemplateAsync(
                    template,
                    new JsonObject { ["region"] = "cn" },
                    "http://127.0.0.1:18082",
                    "");

                Assert.True(result.Ok);
                Assert.NotNull(result.Document);
                Assert.Equal("wf_template", result.Document!.WorkflowId);
                Assert.Equal("select 'cn' as region", result.Document.Nodes[0].Config["query"]?.GetValue<string>());
            }
        }
        finally
        {
            Directory.Delete(tempDir, recursive: true);
        }
    }

    [Fact]
    public async Task ApplySelectedTemplateAsync_BlocksOnValidationFailure()
    {
        using var http = new HttpClient(new StubHttpMessageHandler(_ => Task.FromResult(Json(HttpStatusCode.OK, """
            {
              "ok": true,
              "valid": false,
              "status": "invalid",
              "error_items": [
                {
                  "path": "workflow.nodes",
                  "code": "unknown_node_type",
                  "message": "workflow contains unregistered node types: unknown_future_node"
                }
              ]
            }
            """))));

        var tempDir = Path.Combine(Path.GetTempPath(), "aiwf-template-authoring-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tempDir);
        try
        {
            var coordinator = CreateCoordinator(http, tempDir);
            var result = await coordinator.ApplySelectedTemplateAsync(
                CreateTemplate("builtin_tpl", "Builtin Template", "builtin"),
                new JsonObject { ["region"] = "cn" },
                "http://127.0.0.1:18082",
                "");

            Assert.False(result.Ok);
            Assert.Null(result.Document);
            Assert.Contains("unknown_future_node", result.StatusMessage, StringComparison.Ordinal);
        }
        finally
        {
            Directory.Delete(tempDir, recursive: true);
        }
    }

    [Fact]
    public async Task SaveCurrentAsTemplateAsync_PersistsLocalTemplateAndRefreshesCatalog()
    {
        using var http = new HttpClient(new StubHttpMessageHandler(_ => Task.FromResult(Json(HttpStatusCode.OK, """{"ok":true}"""))));
        var tempDir = Path.Combine(Path.GetTempPath(), "aiwf-template-authoring-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tempDir);
        try
        {
            var coordinator = CreateCoordinator(http, tempDir);
            var result = await coordinator.SaveCurrentAsTemplateAsync(
                new WorkflowGraphDocument(
                    "wf_canvas",
                    "1.0.0",
                    [
                        new WorkflowGraphNodeDocument(
                            "n1",
                            "load_rows_v3",
                            "Load",
                            "source",
                            20,
                            20,
                            new JsonObject
                            {
                                ["source_type"] = "sqlite",
                                ["source"] = "D:/demo.db",
                                ["query"] = "select 1"
                            })
                    ],
                    [],
                    WorkflowGraphViewportDocument.Default,
                    WorkflowGraphSelectionDocument.Empty),
                "Canvas Template",
                new JsonObject
                {
                    ["region"] = new JsonObject { ["type"] = "string", ["required"] = true }
                },
                new JsonObject
                {
                    ["region"] = "cn"
                },
                true);

            Assert.True(result.Ok);
            Assert.NotEmpty(result.SelectedTemplateId);
            var saved = Assert.Single(result.Items, item => string.Equals(item.Origin, "local", StringComparison.Ordinal));
            Assert.Equal("Canvas Template", saved.Name);
            Assert.True(WorkflowAppSchemaSupport.ReadRequirePreflight(saved.Governance));
            Assert.Equal("cn", saved.RuntimeDefaults["region"]?.GetValue<string>());
        }
        finally
        {
            Directory.Delete(tempDir, recursive: true);
        }
    }

    private static WorkflowTemplateAuthoringCoordinator CreateCoordinator(HttpClient http, string tempDir)
    {
        var localStore = new WorkflowTemplateLocalStoreService(Path.Combine(tempDir, "local_templates.json"));
        var packService = new WorkflowTemplatePackService(Path.Combine(tempDir, "template_marketplace.json"));
        var catalog = new WorkflowTemplateCatalogService(localStore, packService, Path.Combine(tempDir, "workflow_builtin_templates.v1.json"));
        return new WorkflowTemplateAuthoringCoordinator(
            new WorkflowRunnerAdapter(http),
            catalog,
            localStore,
            packService,
            () => DateTimeOffset.Parse("2026-05-27T08:00:00Z"),
            () => "deadbeef");
    }

    private static WorkflowTemplateCatalogItem CreateTemplate(string id, string name, string origin, string packId = "", string packName = "")
    {
        return new WorkflowTemplateCatalogItem(
            Id: id,
            Name: name,
            Origin: origin,
            PackId: packId,
            PackName: packName,
            WorkflowDefinition: new JsonObject
            {
                ["workflow_id"] = "wf_template",
                ["version"] = "1.0.0",
                ["nodes"] = new JsonArray
                {
                    new JsonObject
                    {
                        ["id"] = "n1",
                        ["type"] = "load_rows_v3",
                        ["x"] = 20,
                        ["y"] = 30,
                        ["config"] = new JsonObject
                        {
                            ["source_type"] = "sqlite",
                            ["source"] = "D:/demo.db",
                            ["query"] = "select '{{region}}' as region"
                        }
                    }
                },
                ["edges"] = new JsonArray()
            },
            ParamsSchema: new JsonObject
            {
                ["region"] = new JsonObject
                {
                    ["type"] = "string",
                    ["required"] = true,
                    ["min_length"] = 2
                }
            },
            Governance: new JsonObject
            {
                ["preflight_gate_required"] = true
            },
            RuntimeDefaults: new JsonObject
            {
                ["region"] = "cn"
            },
            TemplateSpecVersion: 1,
            CreatedAt: "2026-05-27T00:00:00Z");
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
