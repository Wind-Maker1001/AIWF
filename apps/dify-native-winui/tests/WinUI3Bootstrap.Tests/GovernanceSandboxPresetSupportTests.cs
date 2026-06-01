using System.Text.Json.Nodes;
using AIWF.Native.Runtime;
using Xunit;

namespace AIWF.Native.Tests;

public sealed class GovernanceSandboxPresetSupportTests
{
    [Fact]
    public void ResolvePreset_ReturnsExpectedDefaults()
    {
        var strict = GovernanceSandboxPresetSupport.ResolvePreset("strict");
        var balanced = GovernanceSandboxPresetSupport.ResolvePreset("balanced");
        var loose = GovernanceSandboxPresetSupport.ResolvePreset("loose");

        Assert.Equal(1, strict.Yellow);
        Assert.Equal(2, strict.Red);
        Assert.Equal(600, balanced.DedupWindowSec);
        Assert.Equal("sandbox_limit_exceeded:output", loose.WhitelistCodes);
    }

    [Fact]
    public void BuildRules_NormalizesCsvListsAndPreservesMuteMap()
    {
        var rules = GovernanceSandboxPresetSupport.BuildRules(
            "sandbox_limit_exceeded:output, sandbox_limit_exceeded:output, sandbox_egress_blocked",
            "ai_refine, clean_md",
            new JsonObject
            {
                ["mute_until_by_key"] = new JsonObject
                {
                    ["*::*::*"] = "2026-06-01T00:00:00Z"
                }
            });

        Assert.Equal(2, rules["whitelist_codes"]?.AsArray().Count);
        Assert.Equal(2, rules["whitelist_node_types"]?.AsArray().Count);
        Assert.Equal("2026-06-01T00:00:00Z", rules["mute_until_by_key"]?["*::*::*"]?.GetValue<string>());
    }

    [Fact]
    public void BuildAndParsePresetPayload_RoundTripsState()
    {
        var state = new GovernanceSandboxPresetViewState(
            Yellow: 2,
            Red: 5,
            DedupWindowSec: 120,
            WhitelistCodes: "sandbox_limit_exceeded:output",
            WhitelistNodeTypes: "clean_md");

        var payload = GovernanceSandboxPresetSupport.BuildPresetPayload(state);
        var parsed = GovernanceSandboxPresetSupport.ParsePresetPayload(payload);

        Assert.Equal(2, parsed.Yellow);
        Assert.Equal(5, parsed.Red);
        Assert.Equal(120, parsed.DedupWindowSec);
        Assert.Equal("sandbox_limit_exceeded:output", parsed.WhitelistCodes);
        Assert.Equal("clean_md", parsed.WhitelistNodeTypes);
    }
}
