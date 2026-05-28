using System.Text.Json;
using System.Text.Json.Nodes;
using AIWF.Native.Runtime;
using Xunit;

namespace AIWF.Native.Tests;

public sealed class WorkflowTemplateCatalogServiceTests
{
    [Fact]
    public void LoadAll_MergesBuiltinLocalAndPackTemplatesInOriginOrder()
    {
        var tempDir = Path.Combine(Path.GetTempPath(), "aiwf-template-catalog-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tempDir);
        try
        {
            var builtinPath = Path.Combine(tempDir, "workflow_builtin_templates.v1.json");
            File.WriteAllText(builtinPath, JsonSerializer.Serialize(new JsonObject
            {
                ["schema_version"] = "workflow_builtin_templates.v1",
                ["items"] = new JsonArray
                {
                    SerializeTemplate("builtin_tpl", "Builtin Template")
                }
            }, new JsonSerializerOptions { WriteIndented = true }));

            var localStore = new WorkflowTemplateLocalStoreService(Path.Combine(tempDir, "local_templates.json"));
            localStore.SaveAll([
                CreateTemplate("local_tpl", "Local Template", "local")
            ]);

            var packService = new WorkflowTemplatePackService(Path.Combine(tempDir, "template_marketplace.json"));
            var artifactPath = Path.Combine(tempDir, "pack.json");
            File.WriteAllText(artifactPath, JsonSerializer.Serialize(new JsonObject
            {
                ["schema_version"] = WorkflowTemplateContractSupport.TemplatePackArtifactSchemaVersion,
                ["id"] = "pack_demo",
                ["name"] = "Demo Pack",
                ["templates"] = new JsonArray
                {
                    SerializeTemplate("pack_tpl", "Pack Template")
                }
            }, new JsonSerializerOptions { WriteIndented = true }));
            packService.InstallFromPath(artifactPath);

            var catalog = new WorkflowTemplateCatalogService(localStore, packService, builtinPath);
            var items = catalog.LoadAll();

            Assert.Equal(3, items.Count);
            Assert.Equal("builtin", items[0].Origin);
            Assert.Equal("local", items[1].Origin);
            Assert.Equal("pack", items[2].Origin);
            Assert.Equal("pack_demo", items[2].PackId);
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
            WorkflowDefinition: CreateWorkflowDefinition(id),
            ParamsSchema: new JsonObject(),
            Governance: new JsonObject(),
            RuntimeDefaults: new JsonObject(),
            TemplateSpecVersion: 1,
            CreatedAt: string.Empty);
    }

    private static JsonObject SerializeTemplate(string id, string name)
    {
        return new JsonObject
        {
            ["schema_version"] = WorkflowTemplateContractSupport.LocalTemplateEntrySchemaVersion,
            ["id"] = id,
            ["name"] = name,
            ["workflow_definition"] = CreateWorkflowDefinition(id),
            ["template_spec_version"] = 1,
            ["params_schema"] = new JsonObject(),
            ["governance"] = new JsonObject(),
            ["runtime_defaults"] = new JsonObject(),
            ["created_at"] = string.Empty,
        };
    }

    private static JsonObject CreateWorkflowDefinition(string id)
    {
        return new JsonObject
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
        };
    }
}
