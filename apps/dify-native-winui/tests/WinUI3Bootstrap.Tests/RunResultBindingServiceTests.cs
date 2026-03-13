using AIWF.Native.Runtime;
using Xunit;

namespace AIWF.Native.Tests;

public sealed class RunResultBindingServiceTests
{
    [Fact]
    public void CreateInitialState_UsesNeutralDefaults()
    {
        var state = RunResultBindingService.CreateInitialState();

        Assert.Null(state.BadgeOk);
        Assert.False(state.SyncArtifactsToCanvas);
        Assert.Equal("Source ready", state.InputNodeSubtitle);
        Assert.Equal("Processing rules", state.CleanNodeSubtitle);
        Assert.Equal("Waiting for run result", state.OutputNodeSubtitle);
        Assert.Empty(state.ArtifactDisplayItems);
    }

    [Fact]
    public void TryCreateFromJson_BuildsCanvasAndPanelState()
    {
        const string json = """
            {
              "ok": true,
              "job_id": "job-1",
              "flow": "cleaning",
              "seconds": 1.5,
              "artifacts": [
                { "artifact_id": "xlsx_fin_001", "kind": "xlsx", "path": "C:/tmp/report.xlsx" }
              ]
            }
            """;

        var created = RunResultBindingService.TryCreateFromJson(json, "Not retried", out var state);

        Assert.True(created);
        Assert.True(RunResultBindingService.IsBusinessSuccess(state));
        Assert.True(state.SyncArtifactsToCanvas);
        Assert.True(state.BadgeOk);
        Assert.Equal("cleaning", state.MetricMode);
        Assert.Equal(1500, state.MetricDurationMs);
        Assert.Equal("Generated 1 artifacts", state.OutputNodeSubtitle);
        Assert.Equal("Processing complete", state.CleanNodeSubtitle);
        Assert.Single(state.Artifacts);
        Assert.Single(state.ArtifactDisplayItems);
        Assert.Equal("1 items", state.PanelState.ArtifactsCountText);
    }

    [Fact]
    public void TryCreateFromJson_ReturnsParseFailureStateForInvalidPayload()
    {
        var created = RunResultBindingService.TryCreateFromJson("[]", "Preflight created job: job-auto", out var state);

        Assert.False(created);
        Assert.False(state.SyncArtifactsToCanvas);
        Assert.False(state.BadgeOk);
        Assert.Equal("Parse failed", state.CleanNodeSubtitle);
        Assert.Equal("Response format not recognized", state.OutputNodeSubtitle);
        Assert.Equal("Preflight created job: job-auto", state.PanelState.RetryInfoText);
    }

    [Fact]
    public void TryCreateFromJson_FailurePayloadDisablesBusinessSuccessAndCanvasSync()
    {
        const string json = """
            {
              "ok": false,
              "job_id": "job-fail",
              "flow": "cleaning",
              "seconds": 1.5,
              "artifacts": [
                { "artifact_id": "xlsx_fin_001", "kind": "xlsx", "path": "C:/tmp/report.xlsx" }
              ]
            }
            """;

        var created = RunResultBindingService.TryCreateFromJson(json, "Not retried", out var state);

        Assert.True(created);
        Assert.False(RunResultBindingService.IsBusinessSuccess(state));
        Assert.False(state.BadgeOk);
        Assert.False(state.SyncArtifactsToCanvas);
        Assert.Equal("Processing failed", state.CleanNodeSubtitle);
        Assert.Equal("Run failed, no usable artifacts generated", state.OutputNodeSubtitle);
        Assert.Single(state.Artifacts);
    }

    [Fact]
    public void TryCreateFromJson_MissingOkDoesNotCountAsBusinessSuccess()
    {
        const string json = """
            {
              "job_id": "job-unknown",
              "flow": "cleaning",
              "seconds": 1.5,
              "artifacts": [
                { "artifact_id": "xlsx_fin_001", "kind": "xlsx", "path": "C:/tmp/report.xlsx" }
              ]
            }
            """;

        var created = RunResultBindingService.TryCreateFromJson(json, "Not retried", out var state);

        Assert.True(created);
        Assert.False(RunResultBindingService.IsBusinessSuccess(state));
        Assert.Null(state.BadgeOk);
        Assert.False(state.SyncArtifactsToCanvas);
        Assert.Equal("Result pending confirmation", state.CleanNodeSubtitle);
        Assert.Equal("Missing ok status, inspect the raw response", state.OutputNodeSubtitle);
        Assert.Single(state.Artifacts);
    }
}
