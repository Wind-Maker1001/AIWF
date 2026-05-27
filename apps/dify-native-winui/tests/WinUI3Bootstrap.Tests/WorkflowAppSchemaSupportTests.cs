using System.Text.Json.Nodes;
using AIWF.Native.Runtime;
using Xunit;

namespace AIWF.Native.Tests;

public sealed class WorkflowAppSchemaSupportTests
{
    [Fact]
    public void ParseSchemaJson_NormalizesRules()
    {
        var schema = WorkflowAppSchemaSupport.ParseSchemaJson("""
            {
              "region": { "type": "string", "required": true, "description": "Region" },
              "limit": { "type": "number", "default": 10 }
            }
            """);

        var fields = WorkflowAppSchemaSupport.EnumerateFields(schema);
        Assert.Equal(2, fields.Count);
        Assert.Equal("region", fields[0].Key);
        Assert.True(fields[0].Required);
        Assert.Equal("number", fields[1].Type);
        Assert.Equal(10, fields[1].DefaultValue?.GetValue<int>());
    }

    [Fact]
    public void ParseSchemaJson_RejectsNonObjectJson()
    {
        var ex = Assert.Throws<InvalidOperationException>(() => WorkflowAppSchemaSupport.ParseSchemaJson("[]"));
        Assert.Contains("object", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void ValidateParams_RespectsEnumAndConditionalRules()
    {
        var schema = WorkflowAppSchemaSupport.ParseSchemaJson("""
            {
              "title": { "type": "string", "required": true, "min_length": 3 },
              "language": { "type": "string", "enum": ["zh-CN", "en-US"], "default": "zh-CN" },
              "advanced": { "type": "boolean", "default": false },
              "threshold": {
                "type": "number",
                "depends_on": { "field": "advanced", "equals": true },
                "required": true
              }
            }
            """);

        WorkflowAppSchemaSupport.ValidateParams(schema, new JsonObject
        {
            ["title"] = "Report",
            ["language"] = "zh-CN",
            ["advanced"] = false,
        });

        var ex = Assert.Throws<InvalidOperationException>(() => WorkflowAppSchemaSupport.ValidateParams(
            schema,
            new JsonObject
            {
                ["title"] = "ab",
                ["language"] = "fr-FR",
                ["advanced"] = true,
            }));

        Assert.Contains("title", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("language", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("threshold", ex.Message, StringComparison.OrdinalIgnoreCase);
    }
}
