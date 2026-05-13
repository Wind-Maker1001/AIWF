using System.Text.Json.Nodes;
using AIWF.Native.Runtime;
using Xunit;

namespace AIWF.Native.Tests;

public sealed class GovernanceQualityRuleSetCoordinatorTests
{
    [Fact]
    public async Task RefreshAsync_PreservesMatchingSelectionAndUsesExpectedLimit()
    {
        string? seenBaseUrl = null;
        string? seenApiKey = null;
        int seenLimit = 0;

        var coordinator = new GovernanceQualityRuleSetCoordinator(
            listQualityRuleSets: (baseUrl, apiKey, limit, _) =>
            {
                seenBaseUrl = baseUrl;
                seenApiKey = apiKey;
                seenLimit = limit;
                return Task.FromResult<IReadOnlyList<GovernanceQualityRuleSetItem>>(
                [
                    new GovernanceQualityRuleSetItem("finance_default", "Finance Default", "v1", "workflow", "{}"),
                    new GovernanceQualityRuleSetItem("debate_default", "Debate Default", "v2", "workflow", "{}"),
                ]);
            },
            saveQualityRuleSet: (_, _, _, _, _, _, _, _) => throw new NotImplementedException(),
            deleteQualityRuleSet: (_, _, _, _) => throw new NotImplementedException());

        var result = await coordinator.RefreshAsync(
            "http://127.0.0.1:18081",
            "token",
            "debate_default");

        Assert.Equal("http://127.0.0.1:18081", seenBaseUrl);
        Assert.Equal("token", seenApiKey);
        Assert.Equal(80, seenLimit);
        Assert.Equal(2, result.Items.Count);
        Assert.Equal("debate_default", result.SelectedItem?.Id);
    }

    [Fact]
    public async Task RefreshAsync_SelectsFirstItemWhenSelectionMissing()
    {
        var coordinator = new GovernanceQualityRuleSetCoordinator(
            listQualityRuleSets: (_, _, _, _) => Task.FromResult<IReadOnlyList<GovernanceQualityRuleSetItem>>(
            [
                new GovernanceQualityRuleSetItem("finance_default", "Finance Default", "v1", "workflow", "{}"),
                new GovernanceQualityRuleSetItem("debate_default", "Debate Default", "v2", "workflow", "{}"),
            ]),
            saveQualityRuleSet: (_, _, _, _, _, _, _, _) => throw new NotImplementedException(),
            deleteQualityRuleSet: (_, _, _, _) => throw new NotImplementedException());

        var result = await coordinator.RefreshAsync(
            "http://127.0.0.1:18081",
            "",
            "missing_id");

        Assert.Equal("finance_default", result.SelectedItem?.Id);
    }

    [Fact]
    public async Task SaveAsync_NormalizesNameAndVersion()
    {
        string? seenId = null;
        string? seenName = null;
        string? seenVersion = null;
        string? seenScope = null;
        JsonObject? seenRules = null;

        var coordinator = new GovernanceQualityRuleSetCoordinator(
            listQualityRuleSets: (_, _, _, _) => throw new NotImplementedException(),
            saveQualityRuleSet: (_, _, id, name, version, scope, rules, _) =>
            {
                seenId = id;
                seenName = name;
                seenVersion = version;
                seenScope = scope;
                seenRules = rules;
                return Task.FromResult(new GovernanceQualityRuleSetItem(id, name, version, scope, rules.ToJsonString()));
            },
            deleteQualityRuleSet: (_, _, _, _) => throw new NotImplementedException());

        var item = await coordinator.SaveAsync(
            "http://127.0.0.1:18081",
            "",
            " finance_default ",
            "",
            "",
            new JsonObject { ["required_columns"] = new JsonArray("amount") });

        Assert.Equal("finance_default", seenId);
        Assert.Equal("finance_default", seenName);
        Assert.Equal("v1", seenVersion);
        Assert.Equal("workflow", seenScope);
        Assert.NotNull(seenRules);
        Assert.Equal("finance_default", item.Id);
        Assert.Equal("v1", item.Version);
    }

    [Fact]
    public async Task DeleteAsync_ValidatesIdAndForwardsCall()
    {
        string? seenId = null;
        var coordinator = new GovernanceQualityRuleSetCoordinator(
            listQualityRuleSets: (_, _, _, _) => throw new NotImplementedException(),
            saveQualityRuleSet: (_, _, _, _, _, _, _, _) => throw new NotImplementedException(),
            deleteQualityRuleSet: (_, _, id, _) =>
            {
                seenId = id;
                return Task.CompletedTask;
            });

        await coordinator.DeleteAsync("http://127.0.0.1:18081", "", " finance_default ");

        Assert.Equal("finance_default", seenId);
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            coordinator.DeleteAsync("http://127.0.0.1:18081", "", "   "));
    }
}
