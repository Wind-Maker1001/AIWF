using System.Text.Json.Nodes;
using AIWF.Native.Runtime;
using Xunit;

namespace AIWF.Native.Tests;

public sealed class CleaningPrecheckPresenterTests
{
    [Theory]
    [InlineData("allow", "预检通过")]
    [InlineData("warn", "建议调整模板后再运行")]
    [InlineData("block", "预检拦截")]
    public void Create_MapsActionToStatusText(string action, string expected)
    {
        var payload = new JsonObject
        {
            ["ok"] = action != "block",
            ["precheck_action"] = action,
            ["requested_profile"] = "finance_statement",
            ["recommended_profile"] = "debate_evidence",
            ["recommended_template_id"] = "debate_evidence_v1",
            ["predicted_zero_output_unexpected"] = action == "block",
            ["blocking_reason_codes"] = new JsonArray("profile_mismatch_blocked"),
            ["issues"] = new JsonArray("template profile mismatch")
        };

        var presentation = CleaningPrecheckPresenter.Create(payload);

        Assert.Equal(expected, presentation.StatusText);
        Assert.Contains("请求画像：finance_statement", presentation.ReferenceText, StringComparison.Ordinal);
        Assert.Contains("推荐画像：debate_evidence", presentation.ReferenceText, StringComparison.Ordinal);
        Assert.Contains("推荐模板：debate_evidence_v1", presentation.ReferenceText, StringComparison.Ordinal);
    }
}
