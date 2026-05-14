using System.Text.Json.Nodes;
using AIWF.Native.Runtime;
using Xunit;

namespace AIWF.Native.Tests;

public sealed class GovernanceSandboxMutationCoordinatorTests
{
    [Fact]
    public async Task SaveRulesAsync_ForwardsReason()
    {
        string? seenReason = null;
        var coordinator = new GovernanceSandboxMutationCoordinator(
            saveRules: (_, _, _, reason, _) =>
            {
                seenReason = reason;
                return Task.FromResult("ver_1");
            },
            rollbackRules: (_, _, _, _) => throw new NotImplementedException(),
            saveAutoFixState: (_, _, _, _) => throw new NotImplementedException());

        var versionId = await coordinator.SaveRulesAsync(
            "http://127.0.0.1:18081",
            "",
            new JsonObject(),
            reason: "custom_reason");

        Assert.Equal("ver_1", versionId);
        Assert.Equal("custom_reason", seenReason);
    }

    [Fact]
    public async Task RollbackRulesAsync_ValidatesVersionId()
    {
        string? seenVersionId = null;
        var coordinator = new GovernanceSandboxMutationCoordinator(
            saveRules: (_, _, _, _, _) => throw new NotImplementedException(),
            rollbackRules: (_, _, versionId, _) =>
            {
                seenVersionId = versionId;
                return Task.FromResult("ver_2");
            },
            saveAutoFixState: (_, _, _, _) => throw new NotImplementedException());

        var versionId = await coordinator.RollbackRulesAsync(
            "http://127.0.0.1:18081",
            "",
            " ver_1 ");

        Assert.Equal("ver_2", versionId);
        Assert.Equal("ver_1", seenVersionId);
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            coordinator.RollbackRulesAsync("http://127.0.0.1:18081", "", "   "));
    }

    [Fact]
    public async Task ApplyMuteAsync_BuildsMutedRulesAndUsesMuteReason()
    {
        string? seenReason = null;
        JsonObject? seenRules = null;
        var coordinator = new GovernanceSandboxMutationCoordinator(
            saveRules: (_, _, rules, reason, _) =>
            {
                seenRules = rules;
                seenReason = reason;
                return Task.FromResult("ver_mute");
            },
            rollbackRules: (_, _, _, _) => throw new NotImplementedException(),
            saveAutoFixState: (_, _, _, _) => throw new NotImplementedException());

        var result = await coordinator.ApplyMuteAsync(
            "http://127.0.0.1:18081",
            "",
            new JsonObject(),
            "AI_REFINE",
            "",
            "sandbox_limit_exceeded:output",
            "15",
            new DateTimeOffset(2026, 5, 13, 0, 0, 0, TimeSpan.Zero));

        Assert.Equal("ver_mute", result.VersionId);
        Assert.NotNull(seenRules);
        Assert.Equal("winui_governance_mute", seenReason);
        Assert.Contains("ai_refine::*::sandbox_limit_exceeded:output", seenRules!["mute_until_by_key"]!.ToJsonString(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task SaveAutoFixStateAsync_NormalizesGreenStreak()
    {
        GovernanceSandboxAutoFixState? seenState = null;
        var coordinator = new GovernanceSandboxMutationCoordinator(
            saveRules: (_, _, _, _, _) => throw new NotImplementedException(),
            rollbackRules: (_, _, _, _) => throw new NotImplementedException(),
            saveAutoFixState: (_, _, state, _) =>
            {
                seenState = state;
                return Task.FromResult(state);
            });

        var state = await coordinator.SaveAutoFixStateAsync(
            "http://127.0.0.1:18081",
            "",
            new GovernanceSandboxAutoFixState("", "", 0, new JsonArray(), new JsonArray()),
            " process ",
            " 2026-05-13T01:00:00Z ",
            "-3");

        Assert.NotNull(seenState);
        Assert.Equal("process", seenState!.ForcedIsolationMode);
        Assert.Equal("2026-05-13T01:00:00Z", seenState.ForcedUntil);
        Assert.Equal(0, seenState.GreenStreak);
        Assert.Equal("process", state.ForcedIsolationMode);
    }
}
