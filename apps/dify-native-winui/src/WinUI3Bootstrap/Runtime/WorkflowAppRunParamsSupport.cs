using System.Text.Json.Nodes;

namespace AIWF.Native.Runtime;

public static class WorkflowAppRunParamsSupport
{
    public static JsonObject BuildDefaultsFromSchema(IReadOnlyList<WorkflowAppSchemaField> fields, JsonObject? preferred = null)
    {
        var result = new JsonObject();
        var preferredObject = preferred ?? new JsonObject();
        foreach (var field in fields)
        {
            if (preferredObject.ContainsKey(field.Key))
            {
                result[field.Key] = preferredObject[field.Key]?.DeepClone();
                continue;
            }

            result[field.Key] = DefaultValueForField(field)?.DeepClone();
        }

        return result;
    }

    public static JsonObject ParseRunParamsJson(string raw)
    {
        var text = (raw ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(text))
        {
            return new JsonObject();
        }

        JsonNode? parsed;
        try
        {
            parsed = JsonNode.Parse(text);
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException($"Run params JSON parse failed: {ex.Message}");
        }

        if (parsed is not JsonObject obj)
        {
            throw new InvalidOperationException("Run params JSON must be an object.");
        }

        return obj;
    }

    public static JsonObject CollectRunParamsFromRawValues(
        IReadOnlyList<WorkflowAppSchemaField> fields,
        IReadOnlyDictionary<string, string> rawValues)
    {
        var result = new JsonObject();
        foreach (var field in fields)
        {
            var raw = rawValues.TryGetValue(field.Key, out var value) ? value : string.Empty;
            result[field.Key] = ParseValue(field.Type, raw);
        }

        return result;
    }

    public static JsonObject BuildTemplatePolicyPreview(bool requirePreflight, JsonObject runtimeDefaults)
    {
        return new JsonObject
        {
            ["version"] = 1,
            ["governance"] = new JsonObject
            {
                ["preflight_gate_required"] = requirePreflight,
                ["auto_fix_enabled"] = true,
                ["acceptance_mode"] = "preflight_autofix_recheck_v1",
            },
            ["runtime_defaults"] = runtimeDefaults.DeepClone(),
        };
    }

    public static JsonNode? DefaultValueForField(WorkflowAppSchemaField field)
    {
        if (field.DefaultValue is not null)
        {
            return field.DefaultValue.DeepClone();
        }

        return field.Type switch
        {
            "number" => JsonValue.Create(0),
            "boolean" => JsonValue.Create(false),
            "object" => new JsonObject(),
            "array" => new JsonArray(),
            _ => JsonValue.Create(string.Empty),
        };
    }

    private static JsonNode? ParseValue(string type, string raw)
    {
        return type switch
        {
            "boolean" => JsonValue.Create(string.Equals((raw ?? string.Empty).Trim(), "true", StringComparison.OrdinalIgnoreCase)),
            "number" => JsonValue.Create(ParseNumber(raw)),
            "object" => ParseJsonNodeOrFallback(raw, new JsonObject()),
            "array" => ParseJsonNodeOrFallback(raw, new JsonArray()),
            _ => JsonValue.Create(raw ?? string.Empty),
        };
    }

    private static double ParseNumber(string raw)
    {
        return double.TryParse((raw ?? string.Empty).Trim(), out var value) && double.IsFinite(value)
            ? value
            : 0;
    }

    private static JsonNode ParseJsonNodeOrFallback(string raw, JsonNode fallback)
    {
        var text = (raw ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(text))
        {
            return fallback;
        }

        try
        {
            return JsonNode.Parse(text) ?? fallback;
        }
        catch
        {
            return fallback;
        }
    }
}
