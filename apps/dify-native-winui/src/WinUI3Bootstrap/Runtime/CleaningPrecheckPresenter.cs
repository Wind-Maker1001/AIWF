using System.Text.Json.Nodes;

namespace AIWF.Native.Runtime;

public sealed record CleaningPrecheckPresentation(
    string Action,
    string StatusText,
    string ReferenceText);

public static class CleaningPrecheckPresenter
{
    public static CleaningPrecheckPresentation Create(JsonObject response)
    {
        var action = ReadString(response, "precheck_action");
        var statusText = action switch
        {
            "allow" => "预检通过",
            "warn" => "建议调整模板后再运行",
            "block" => "预检拦截",
            _ => ReadBool(response, "ok") ? "预检完成" : "预检失败"
        };

        var requestedProfile = OrDash(ReadString(response, "requested_profile"));
        var recommendedProfile = OrDash(ReadString(response, "recommended_profile"));
        var recommendedTemplateId = OrDash(ReadString(response, "recommended_template_id"));
        var blockingReasonCodes = JoinArray(response["blocking_reason_codes"] as JsonArray);
        var predictedZeroOutput = ReadBool(response, "predicted_zero_output_unexpected") ? "是" : "否";

        var lines = new List<string>
        {
            $"请求画像：{requestedProfile}",
            $"推荐画像：{recommendedProfile}",
            $"推荐模板：{recommendedTemplateId}",
            $"阻断原因：{(string.IsNullOrWhiteSpace(blockingReasonCodes) ? "-" : blockingReasonCodes)}",
            $"将产生空结果：{predictedZeroOutput}"
        };

        var issues = JoinArray(response["issues"] as JsonArray);
        if (!string.IsNullOrWhiteSpace(issues))
        {
            lines.Add($"问题：{issues}");
        }

        return new CleaningPrecheckPresentation(
            action,
            statusText,
            string.Join(Environment.NewLine, lines));
    }

    private static string ReadString(JsonObject response, string key)
    {
        return response[key]?.GetValue<string>()?.Trim() ?? string.Empty;
    }

    private static bool ReadBool(JsonObject response, string key)
    {
        return response[key]?.GetValue<bool>() ?? false;
    }

    private static string JoinArray(JsonArray? items)
    {
        if (items is null || items.Count == 0)
        {
            return string.Empty;
        }

        return string.Join(", ", items.Select(static item => item?.ToString() ?? string.Empty).Where(static item => !string.IsNullOrWhiteSpace(item)));
    }

    private static string OrDash(string value)
    {
        return string.IsNullOrWhiteSpace(value) ? "-" : value;
    }
}
