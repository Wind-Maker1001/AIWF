using System.Text.Json.Nodes;
using AIWF.Native.Runtime;
using Xunit;

namespace AIWF.Native.Tests;

public sealed class WorkflowAppRunParamsSupportTests
{
    [Fact]
    public void BuildDefaultsFromSchema_UsesPreferredValuesAndDesktopDefaults()
    {
        var fields = new[]
        {
            new WorkflowAppSchemaField("region", "string", true, null, string.Empty),
            new WorkflowAppSchemaField("limit", "number", false, null, string.Empty),
            new WorkflowAppSchemaField("strict", "boolean", false, null, string.Empty),
        };

        var defaults = WorkflowAppRunParamsSupport.BuildDefaultsFromSchema(
            fields,
            new JsonObject { ["region"] = "cn" });

        Assert.Equal("cn", defaults["region"]?.GetValue<string>());
        Assert.Equal(0, defaults["limit"]?.GetValue<int>());
        Assert.False(defaults["strict"]?.GetValue<bool>() ?? true);
    }

    [Fact]
    public void CollectRunParamsFromRawValues_ParsesTypedValues()
    {
        var fields = new[]
        {
            new WorkflowAppSchemaField("region", "string", true, null, string.Empty),
            new WorkflowAppSchemaField("limit", "number", false, null, string.Empty),
            new WorkflowAppSchemaField("strict", "boolean", false, null, string.Empty),
            new WorkflowAppSchemaField("meta", "object", false, null, string.Empty),
        };

        var collected = WorkflowAppRunParamsSupport.CollectRunParamsFromRawValues(
            fields,
            new Dictionary<string, string>
            {
                ["region"] = "cn",
                ["limit"] = "12",
                ["strict"] = "true",
                ["meta"] = "{\"mode\":\"fast\"}",
            });

        Assert.Equal("cn", collected["region"]?.GetValue<string>());
        Assert.Equal(12, collected["limit"]?.GetValue<double>());
        Assert.True(collected["strict"]?.GetValue<bool>() ?? false);
        Assert.Equal("fast", collected["meta"]?["mode"]?.GetValue<string>());
    }

    [Fact]
    public void BuildTemplatePolicyPreview_BuildsFixedGovernanceShape()
    {
        var outJson = WorkflowAppRunParamsSupport.BuildTemplatePolicyPreview(
            requirePreflight: true,
            new JsonObject { ["region"] = "cn" });

        Assert.Equal(1, outJson["version"]?.GetValue<int>());
        Assert.True(outJson["governance"]?["preflight_gate_required"]?.GetValue<bool>() ?? false);
        Assert.Equal("preflight_autofix_recheck_v1", outJson["governance"]?["acceptance_mode"]?.GetValue<string>());
        Assert.Equal("cn", outJson["runtime_defaults"]?["region"]?.GetValue<string>());
    }
}
