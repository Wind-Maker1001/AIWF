using System.Text.Json;
using System.Text.Json.Nodes;
using AIWF.Native.Runtime;
using Xunit;

namespace AIWF.Native.Tests;

public sealed class WorkflowTemplatePackServiceTests
{
    [Fact]
    public void InstallExportRemove_RoundTripsTemplatePackCatalog()
    {
        var tempDir = Path.Combine(Path.GetTempPath(), "aiwf-template-pack-service-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tempDir);
        try
        {
            var artifactPath = Path.Combine(tempDir, "pack.json");
            File.WriteAllText(artifactPath, JsonSerializer.Serialize(new JsonObject
            {
                ["schema_version"] = WorkflowTemplateContractSupport.TemplatePackArtifactSchemaVersion,
                ["id"] = "pack_finance",
                ["name"] = "Finance Pack",
                ["version"] = "v1",
                ["templates"] = new JsonArray
                {
                    new JsonObject
                    {
                        ["id"] = "tpl_finance",
                        ["name"] = "Finance Template",
                        ["workflow_definition"] = new JsonObject
                        {
                            ["workflow_id"] = "wf_finance",
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
                                        ["query"] = "select * from finance"
                                    }
                                }
                            },
                            ["edges"] = new JsonArray()
                        }
                    }
                }
            }, new JsonSerializerOptions { WriteIndented = true }));

            var service = new WorkflowTemplatePackService(Path.Combine(tempDir, "template_marketplace.json"));
            var installed = service.InstallFromPath(artifactPath);
            Assert.Equal("pack_finance", installed.Id);

            var loadedPack = Assert.Single(service.LoadPacks());
            Assert.Equal("Finance Pack", loadedPack.Name);
            Assert.Equal("pack", loadedPack.Templates[0].Origin);

            var exportPath = Path.Combine(tempDir, "pack-export.json");
            service.ExportPack("pack_finance", exportPath);
            var exported = JsonNode.Parse(File.ReadAllText(exportPath)) as JsonObject;
            Assert.Equal(WorkflowTemplateContractSupport.TemplatePackArtifactSchemaVersion, exported?["schema_version"]?.GetValue<string>());
            Assert.NotNull(exported?["templates"]?[0]?["workflow_definition"]);
            Assert.Null(exported?["templates"]?[0]?["graph"]);

            Assert.True(service.RemovePack("pack_finance"));
            Assert.Empty(service.LoadPacks());
        }
        finally
        {
            Directory.Delete(tempDir, recursive: true);
        }
    }
}
