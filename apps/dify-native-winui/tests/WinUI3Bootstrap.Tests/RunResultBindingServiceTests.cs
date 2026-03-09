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
        Assert.Equal("源数据准备", state.InputNodeSubtitle);
        Assert.Equal("规则处理", state.CleanNodeSubtitle);
        Assert.Equal("等待运行结果", state.OutputNodeSubtitle);
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

        var created = RunResultBindingService.TryCreateFromJson(json, "未重试", out var state);

        Assert.True(created);
        Assert.True(state.SyncArtifactsToCanvas);
        Assert.True(state.BadgeOk);
        Assert.Equal("cleaning", state.MetricMode);
        Assert.Equal(1500, state.MetricDurationMs);
        Assert.Equal("已生成 1 个产物", state.OutputNodeSubtitle);
        Assert.Equal("处理完成", state.CleanNodeSubtitle);
        Assert.Single(state.Artifacts);
        Assert.Single(state.ArtifactDisplayItems);
        Assert.Equal("1 项", state.PanelState.ArtifactsCountText);
    }

    [Fact]
    public void TryCreateFromJson_ReturnsParseFailureStateForInvalidPayload()
    {
        var created = RunResultBindingService.TryCreateFromJson("[]", "预检创建作业：job-auto", out var state);

        Assert.False(created);
        Assert.False(state.SyncArtifactsToCanvas);
        Assert.False(state.BadgeOk);
        Assert.Equal("结果解析失败", state.CleanNodeSubtitle);
        Assert.Equal("无法识别返回结构", state.OutputNodeSubtitle);
        Assert.Equal("预检创建作业：job-auto", state.PanelState.RetryInfoText);
    }
}
