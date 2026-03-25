using System.Text.Json.Nodes;
using AIWF.Native.Runtime;
using Xunit;

namespace AIWF.Native.Tests;

public sealed class GovernanceEditorSupportTests
{
    [Fact]
    public void ApplySandboxMute_NormalizesWildcardsAndWritesMuteUntil()
    {
        var rules = new JsonObject
        {
            ["whitelist_codes"] = new JsonArray("sandbox_limit_exceeded:output")
        };

        var updated = GovernanceEditorSupport.ApplySandboxMute(
            rules,
            nodeType: "AI_REFINE",
            nodeId: "",
            code: "*",
            minutes: 30,
            nowUtc: new DateTimeOffset(2026, 3, 22, 0, 0, 0, TimeSpan.Zero));

        var mute = Assert.IsType<JsonObject>(updated["mute_until_by_key"]);
        Assert.Equal("2026-03-22T00:30:00Z", mute["ai_refine::*::*"]?.GetValue<string>());
        Assert.Single(Assert.IsType<JsonArray>(updated["whitelist_codes"]));
    }

    [Fact]
    public void ApplySandboxMute_ClonesInputRules()
    {
        var original = new JsonObject();

        var updated = GovernanceEditorSupport.ApplySandboxMute(
            original,
            nodeType: null,
            nodeId: null,
            code: null,
            minutes: 1,
            nowUtc: new DateTimeOffset(2026, 3, 22, 0, 0, 0, TimeSpan.Zero));

        Assert.Null(original["mute_until_by_key"]);
        Assert.NotNull(updated["mute_until_by_key"]);
    }
}
