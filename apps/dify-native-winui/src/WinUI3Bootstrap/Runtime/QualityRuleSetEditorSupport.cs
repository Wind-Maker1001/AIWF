using System.Text.Json.Nodes;

namespace AIWF.Native.Runtime;

public static class QualityRuleSetEditorSupport
{
    public static JsonObject CollectQualityRulesFromWorkflowDocument(WorkflowGraphDocument? document)
    {
        if (document is null)
        {
            return new JsonObject();
        }

        var target = document.Nodes.FirstOrDefault(node =>
            string.Equals(node.Type, "quality_check_v2", StringComparison.Ordinal)
            || string.Equals(node.Type, "quality_check_v3", StringComparison.Ordinal)
            || string.Equals(node.Type, "quality_check_v4", StringComparison.Ordinal));
        if (target is null)
        {
            return new JsonObject();
        }

        return target.Config["rules"] as JsonObject is JsonObject rules
            ? JsonNode.Parse(rules.ToJsonString()) as JsonObject ?? new JsonObject()
            : new JsonObject();
    }
}
