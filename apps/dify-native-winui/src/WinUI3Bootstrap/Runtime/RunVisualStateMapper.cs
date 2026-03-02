namespace AIWF.Native.Runtime;

public enum MetricVisualState
{
    Neutral,
    Good,
    Warning,
    Danger
}

public enum RunBadgeState
{
    Idle,
    Success,
    Failed
}

public sealed record RunMetricVisuals(
    MetricVisualState OkState,
    MetricVisualState ModeState,
    MetricVisualState DurationState);

public static class RunVisualStateMapper
{
    public static RunMetricVisuals MapMetrics(bool? ok, string mode, int? durationMs)
    {
        var okState = ok switch
        {
            true => MetricVisualState.Good,
            false => MetricVisualState.Danger,
            _ => MetricVisualState.Neutral
        };

        var modeState = string.IsNullOrWhiteSpace(mode) || mode == "-"
            ? MetricVisualState.Neutral
            : MetricVisualState.Good;

        var durationState = durationMs switch
        {
            null => MetricVisualState.Neutral,
            <= 1500 => MetricVisualState.Good,
            <= 4000 => MetricVisualState.Warning,
            _ => MetricVisualState.Danger
        };

        return new RunMetricVisuals(okState, modeState, durationState);
    }

    public static RunBadgeState MapBadge(bool? ok)
    {
        return ok switch
        {
            true => RunBadgeState.Success,
            false => RunBadgeState.Failed,
            _ => RunBadgeState.Idle
        };
    }
}
