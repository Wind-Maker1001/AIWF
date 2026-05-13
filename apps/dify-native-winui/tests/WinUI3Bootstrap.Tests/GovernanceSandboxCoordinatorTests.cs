using System.Text.Json.Nodes;
using AIWF.Native.Runtime;
using Xunit;

namespace AIWF.Native.Tests;

public sealed class GovernanceSandboxCoordinatorTests
{
    [Fact]
    public async Task RefreshAsync_UsesExpectedLimitsAndReturnsBundledState()
    {
        string? seenBaseUrl = null;
        string? seenApiKey = null;
        int seenVersionLimit = 0;
        int seenActionLimit = 0;

        var coordinator = new GovernanceSandboxCoordinator(
            getRules: (baseUrl, apiKey, _) =>
            {
                seenBaseUrl = baseUrl;
                seenApiKey = apiKey;
                return Task.FromResult(new JsonObject
                {
                    ["whitelist_codes"] = new JsonArray("sandbox_limit_exceeded:output")
                });
            },
            listVersions: (_, _, limit, _) =>
            {
                seenVersionLimit = limit;
                return Task.FromResult<IReadOnlyList<GovernanceSandboxRuleVersionItem>>(
                [
                    new GovernanceSandboxRuleVersionItem("ver_1", "2026-05-09T00:00:00Z", "set_rules", "{}"),
                ]);
            },
            getAutoFixState: (_, _, _) => Task.FromResult(
                new GovernanceSandboxAutoFixState(
                    "process",
                    "2026-05-09T01:00:00Z",
                    2,
                    new JsonArray(new JsonObject { ["run_id"] = "run_1" }),
                    new JsonArray(new JsonObject { ["ts"] = "2026-05-09T00:10:00Z" }))),
            listAutoFixActions: (_, _, limit, _) =>
            {
                seenActionLimit = limit;
                return Task.FromResult<IReadOnlyList<GovernanceSandboxAutoFixActionItem>>(
                [
                    new GovernanceSandboxAutoFixActionItem("2026-05-09T00:10:00Z", 3, "pause_queue,require_manual_review"),
                ]);
            });

        var result = await coordinator.RefreshAsync(
            "http://127.0.0.1:18081",
            "token");

        Assert.Equal("http://127.0.0.1:18081", seenBaseUrl);
        Assert.Equal("token", seenApiKey);
        Assert.Equal(80, seenVersionLimit);
        Assert.Equal(60, seenActionLimit);
        Assert.Contains("sandbox_limit_exceeded:output", result.Rules.ToJsonString(), StringComparison.Ordinal);
        Assert.Single(result.Versions);
        Assert.Equal("process", result.AutoFixState.ForcedIsolationMode);
        Assert.Single(result.AutoFixActions);
    }
}
