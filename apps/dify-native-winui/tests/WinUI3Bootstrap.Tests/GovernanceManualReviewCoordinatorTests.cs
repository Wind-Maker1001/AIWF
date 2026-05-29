using AIWF.Native.Runtime;
using Xunit;

namespace AIWF.Native.Tests;

public sealed class GovernanceManualReviewCoordinatorTests
{
    [Fact]
    public async Task RefreshPendingAsync_UsesExpectedLimitAndBuildsHint()
    {
        string? seenBaseUrl = null;
        string? seenApiKey = null;
        int seenLimit = 0;

        var coordinator = new GovernanceManualReviewCoordinator(
            listPendingReviews: (baseUrl, apiKey, limit, _) =>
            {
                seenBaseUrl = baseUrl;
                seenApiKey = apiKey;
                seenLimit = limit;
                return Task.FromResult<IReadOnlyList<GovernanceManualReviewItem>>(
                [
                    new GovernanceManualReviewItem("run_1", "gate_a", "wf_1", "n1", "", "", "", "", "pending", false),
                    new GovernanceManualReviewItem("run_2", "gate_b", "wf_1", "n2", "", "", "", "", "pending", false),
                ]);
            },
            listReviewHistory: (_, _, _, _, _, _, _, _, _) => throw new NotImplementedException(),
            submitReviewDecision: (_, _, _, _, _, _, _, _) => throw new NotImplementedException());

        var result = await coordinator.RefreshPendingAsync("http://127.0.0.1:18081", "token");

        Assert.Equal("http://127.0.0.1:18081", seenBaseUrl);
        Assert.Equal("token", seenApiKey);
        Assert.Equal(120, seenLimit);
        Assert.Equal(2, result.Items.Count);
        Assert.Equal("Pending review items: 2", result.HintText);
    }

    [Fact]
    public async Task RefreshPendingAsync_UsesEmptyHintWhenNoItems()
    {
        var coordinator = new GovernanceManualReviewCoordinator(
            listPendingReviews: (_, _, _, _) => Task.FromResult<IReadOnlyList<GovernanceManualReviewItem>>(Array.Empty<GovernanceManualReviewItem>()),
            listReviewHistory: (_, _, _, _, _, _, _, _, _) => throw new NotImplementedException(),
            submitReviewDecision: (_, _, _, _, _, _, _, _) => throw new NotImplementedException());

        var result = await coordinator.RefreshPendingAsync("http://127.0.0.1:18081", "");

        Assert.Empty(result.Items);
        Assert.Equal("No pending review items.", result.HintText);
    }

    [Fact]
    public async Task RefreshHistoryAsync_TrimsBlankFiltersToNull()
    {
        string? seenRunId = "unset";
        string? seenReviewer = "unset";
        string? seenStatus = "unset";
        string? seenDateFrom = "unset";
        string? seenDateTo = "unset";
        int seenLimit = 0;

        var coordinator = new GovernanceManualReviewCoordinator(
            listPendingReviews: (_, _, _, _) => throw new NotImplementedException(),
            listReviewHistory: (_, _, limit, runId, reviewer, status, dateFrom, dateTo, _) =>
            {
                seenLimit = limit;
                seenRunId = runId;
                seenReviewer = reviewer;
                seenStatus = status;
                seenDateFrom = dateFrom;
                seenDateTo = dateTo;
                return Task.FromResult<IReadOnlyList<GovernanceManualReviewItem>>(Array.Empty<GovernanceManualReviewItem>());
            },
            submitReviewDecision: (_, _, _, _, _, _, _, _) => throw new NotImplementedException());

        await coordinator.RefreshHistoryAsync(
            "http://127.0.0.1:18081",
            "",
            "   ",
            " alice ",
            "",
            " 2026-05-01T00:00:00Z ",
            "   ");

        Assert.Equal(120, seenLimit);
        Assert.Null(seenRunId);
        Assert.Equal("alice", seenReviewer);
        Assert.Null(seenStatus);
        Assert.Equal("2026-05-01T00:00:00Z", seenDateFrom);
        Assert.Null(seenDateTo);
    }

    [Fact]
    public async Task SubmitDecisionAsync_NormalizesInputsAndValidatesSelectedItem()
    {
        string? seenRunId = null;
        string? seenReviewKey = null;
        string? seenReviewer = null;
        string? seenComment = null;
        bool? seenApproved = null;

        var coordinator = new GovernanceManualReviewCoordinator(
            listPendingReviews: (_, _, _, _) => throw new NotImplementedException(),
            listReviewHistory: (_, _, _, _, _, _, _, _, _) => throw new NotImplementedException(),
            submitReviewDecision: (_, _, runId, reviewKey, approved, reviewer, comment, _) =>
            {
                seenRunId = runId;
                seenReviewKey = reviewKey;
                seenApproved = approved;
                seenReviewer = reviewer;
                seenComment = comment;
                return Task.FromResult(new GovernanceManualReviewItem(runId, reviewKey, "wf_1", "n1", reviewer, comment, "", "", approved ? "approved" : "rejected", approved));
            });

        var item = await coordinator.SubmitDecisionAsync(
            "http://127.0.0.1:18081",
            "",
            new GovernanceManualReviewItem(" run_1 ", " gate_a ", "wf_1", "n1", "", "", "", "", "pending", false),
            true,
            " alice ",
            " looks good ");

        Assert.Equal("run_1", seenRunId);
        Assert.Equal("gate_a", seenReviewKey);
        Assert.True(seenApproved);
        Assert.Equal("alice", seenReviewer);
        Assert.Equal("looks good", seenComment);
        Assert.Equal("approved", item.Status);

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            coordinator.SubmitDecisionAsync(
                "http://127.0.0.1:18081",
                "",
                new GovernanceManualReviewItem("", "", "", "", "", "", "", "", "pending", false),
                false,
                "",
                ""));
    }
}
