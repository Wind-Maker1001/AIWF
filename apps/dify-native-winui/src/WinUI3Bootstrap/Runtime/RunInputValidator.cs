namespace AIWF.Native.Runtime;

public sealed record RunInputData(
    string BridgeUrl,
    string Actor,
    string JobId,
    string Flow,
    string ReportTitle);

public sealed class RunInputValidationResult
{
    public bool IsValid { get; init; }
    public string Message { get; init; } = "校验通过。";
    public HashSet<string> MissingKeys { get; init; } = [];
}

public static class RunInputValidator
{
    public static RunInputValidationResult Validate(RunInputData data)
    {
        var missing = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var labels = new List<string>();

        AddIfMissing(data.BridgeUrl, "bridge_url", "桥接地址", missing, labels);
        AddIfMissing(data.Actor, "actor", "执行者", missing, labels);
        AddIfMissing(data.Flow, "flow", "Flow", missing, labels);
        AddIfMissing(data.ReportTitle, "report_title", "报告标题", missing, labels);

        if (missing.Count == 0)
        {
            return new RunInputValidationResult
            {
                IsValid = true,
                Message = "校验通过。",
                MissingKeys = missing
            };
        }

        return new RunInputValidationResult
        {
            IsValid = false,
            Message = $"请填写必填项：{string.Join("、", labels)}。",
            MissingKeys = missing
        };
    }

    private static void AddIfMissing(
        string value,
        string key,
        string label,
        ISet<string> missingKeys,
        ICollection<string> labels)
    {
        if (!string.IsNullOrWhiteSpace(value))
        {
            return;
        }

        missingKeys.Add(key);
        labels.Add(label);
    }
}
