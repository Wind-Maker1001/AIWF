using System.Text.Json.Nodes;
using AIWF.Native.Runtime;
using Xunit;

namespace AIWF.Native.Tests;

public sealed class WorkflowTemplateLocalStoreServiceTests
{
    [Fact]
    public void SaveAll_LoadAll_RoundTripsLocalTemplates()
    {
        var tempDir = Path.Combine(Path.GetTempPath(), "aiwf-template-local-store-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tempDir);
        try
        {
            var service = new WorkflowTemplateLocalStoreService(Path.Combine(tempDir, "local_templates.json"));
            service.SaveAll([
                CreateTemplate("local_tpl_1", "Local One", "local")
            ]);

            var loaded = service.LoadAll();

            var item = Assert.Single(loaded);
            Assert.Equal("local_tpl_1", item.Id);
            Assert.Equal("Local One", item.Name);
            Assert.Equal("local", item.Origin);
            Assert.Equal("wf_local_tpl_1", item.WorkflowDefinition["workflow_id"]?.GetValue<string>());
        }
        finally
        {
            Directory.Delete(tempDir, recursive: true);
        }
    }

    private static WorkflowTemplateCatalogItem CreateTemplate(string id, string name, string origin)
    {
        return new WorkflowTemplateCatalogItem(
            Id: id,
            Name: name,
            Origin: origin,
            PackId: string.Empty,
            PackName: string.Empty,
            WorkflowDefinition: new JsonObject
            {
                ["workflow_id"] = $"wf_{id}",
                ["version"] = "1.0.0",
                ["nodes"] = new JsonArray
                {
                    new JsonObject
                    {
                        ["id"] = "n1",
                        ["type"] = "load_rows_v3",
                        ["x"] = 20,
                        ["y"] = 20,
                        ["config"] = new JsonObject
                        {
                            ["source_type"] = "sqlite",
                            ["source"] = "D:/demo.db",
                            ["query"] = "select 1"
                        }
                    }
                },
                ["edges"] = new JsonArray()
            },
            ParamsSchema: new JsonObject
            {
                ["title"] = new JsonObject { ["type"] = "string", ["required"] = true }
            },
            Governance: new JsonObject
            {
                ["preflight_gate_required"] = true
            },
            RuntimeDefaults: new JsonObject
            {
                ["title"] = "demo"
            },
            TemplateSpecVersion: 1,
            CreatedAt: "2026-05-27T00:00:00Z");
    }
}
