namespace AIWF.Native.Runtime;

public sealed record ResultPanelState(
    string ArtifactsCountText,
    string JobIdText,
    string RetryInfoText,
    string RunResultText,
    string RunModeText,
    string DurationText,
    string OkMetricText,
    string ModeMetricText,
    string DurationMetricText)
{
    public static ResultPanelState Empty { get; } = new(
        "0 项",
        "-",
        "未重试",
        "-",
        "-",
        "-",
        "-",
        "-",
        "-");
}
