namespace AIWF.Native.CanvasRuntime;

internal static class CanvasWorkflowScaffold
{
    public const string InputNodeKey = "system-input";
    public const string CleanNodeKey = "system-clean";
    public const string OutputNodeKey = "system-output";

    public static IReadOnlyList<CanvasNodeState> CreateDefaultNodes()
    {
        return
        [
            new CanvasNodeState(InputNodeKey, "输入", "源数据准备", 140, 100, false, false, null, null),
            new CanvasNodeState(CleanNodeKey, "清洗", "规则处理", 460, 100, false, false, null, null),
            new CanvasNodeState(OutputNodeKey, "输出", "等待运行结果", 780, 100, false, false, null, null)
        ];
    }

    public static IReadOnlyList<CanvasEdgeState> CreateDefaultEdges()
    {
        return
        [
            new CanvasEdgeState(InputNodeKey, CleanNodeKey),
            new CanvasEdgeState(CleanNodeKey, OutputNodeKey)
        ];
    }

    public static bool IsCoreNodeKey(string? nodeKey)
    {
        return string.Equals(nodeKey, InputNodeKey, StringComparison.Ordinal)
            || string.Equals(nodeKey, CleanNodeKey, StringComparison.Ordinal)
            || string.Equals(nodeKey, OutputNodeKey, StringComparison.Ordinal);
    }
}
