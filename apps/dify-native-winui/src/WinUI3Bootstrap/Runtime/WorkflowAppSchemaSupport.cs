using System.Text.Json.Nodes;

namespace AIWF.Native.Runtime;

public sealed record WorkflowAppSchemaField(
    string Key,
    string Type,
    bool Required,
    JsonNode? DefaultValue,
    string Description,
    IReadOnlyList<string>? EnumValues = null);

public static class WorkflowAppSchemaSupport
{
    public static JsonObject ParseSchemaJson(string raw)
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
            throw new InvalidOperationException(BuildSchemaErrorText($"Schema JSON parse failed: {ex.Message}"));
        }

        if (parsed is not JsonObject obj)
        {
            throw new InvalidOperationException(BuildSchemaErrorText("Schema JSON must be an object."));
        }

        return NormalizeSchemaObject(obj);
    }

    public static JsonObject NormalizeSchemaObject(JsonObject? source)
    {
        var result = new JsonObject();
        if (source is null)
        {
            return result;
        }

        foreach (var property in source)
        {
            var key = (property.Key ?? string.Empty).Trim();
            if (string.IsNullOrWhiteSpace(key))
            {
                continue;
            }

            var rule = property.Value as JsonObject;
            var type = ((rule?["type"]?.GetValue<string>()) ?? "string").Trim();
            if (string.IsNullOrWhiteSpace(type))
            {
                type = "string";
            }

            var normalized = rule is null
                ? new JsonObject()
                : JsonNode.Parse(rule.ToJsonString()) as JsonObject ?? new JsonObject();
            normalized["type"] = type;

            result[key] = normalized;
        }

        return result;
    }

    public static void ValidateParams(JsonObject schema, JsonObject paramsObject)
    {
        if (schema.Count == 0)
        {
            return;
        }

        var normalized = NormalizeSchemaObject(schema);
        var errors = new List<string>();
        foreach (var property in normalized)
        {
            if (property.Value is not JsonObject baseRule)
            {
                continue;
            }

            var rule = ResolveRule(baseRule, paramsObject);
            if (rule["__active"]?.GetValue<bool?>() == false)
            {
                continue;
            }

            var key = property.Key;
            var hasValue = paramsObject.ContainsKey(key);
            var value = hasValue ? paramsObject[key] : null;
            if (rule["required"]?.GetValue<bool?>() == true && !hasValue)
            {
                errors.Add($"missing parameter: {key}");
                continue;
            }

            if (!hasValue)
            {
                continue;
            }

            var type = rule["type"]?.GetValue<string>() ?? "string";
            switch (type)
            {
                case "string":
                    ValidateString(key, value, rule, errors);
                    break;
                case "number":
                    ValidateNumber(key, value, rule, errors);
                    break;
                case "boolean":
                    if (value is not JsonValue booleanValue || booleanValue.TryGetValue<bool>(out _) is false)
                    {
                        errors.Add($"{key} must be a boolean");
                    }
                    break;
                case "object":
                    if (value is not JsonObject)
                    {
                        errors.Add($"{key} must be an object");
                    }
                    break;
                case "array":
                    if (value is not JsonArray)
                    {
                        errors.Add($"{key} must be an array");
                    }
                    break;
            }

            if (rule["enum"] is JsonArray enumValues && enumValues.Count > 0)
            {
                var matched = enumValues.Any(item => JsonNodesEqual(item, value));
                if (!matched)
                {
                    errors.Add($"{key} must be one of: {string.Join(", ", enumValues.Select(static item => item?.ToJsonString() ?? "null"))}");
                }
            }
        }

        if (errors.Count > 0)
        {
            throw new InvalidOperationException($"Template params validation failed: {string.Join("; ", errors)}");
        }
    }

    private static JsonObject ResolveRule(JsonObject baseRule, JsonObject paramsObject)
    {
        var resolved = JsonNode.Parse(baseRule.ToJsonString()) as JsonObject ?? new JsonObject();
        var active = true;
        if (baseRule["depends_on"] is JsonArray dependsArray)
        {
            active = dependsArray.OfType<JsonObject>().All(item => MatchesCondition(item, paramsObject));
        }
        else if (baseRule["depends_on"] is JsonObject dependsObject)
        {
            active = MatchesCondition(dependsObject, paramsObject);
        }

        resolved["__active"] = active;
        if (baseRule["conditional"] is JsonArray conditional)
        {
            foreach (var item in conditional.OfType<JsonObject>())
            {
                if (item["when"] is not JsonObject when || !MatchesCondition(when, paramsObject))
                {
                    continue;
                }

                foreach (var property in item)
                {
                    if (string.Equals(property.Key, "when", StringComparison.Ordinal))
                    {
                        continue;
                    }

                    resolved[property.Key] = property.Value?.DeepClone();
                }
                break;
            }
        }

        return resolved;
    }

    private static bool MatchesCondition(JsonObject condition, JsonObject paramsObject)
    {
        var key = (condition["field"]?.GetValue<string>() ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(key))
        {
            return true;
        }

        var hasValue = paramsObject.ContainsKey(key);
        var value = hasValue ? paramsObject[key] : null;
        if (condition["exists"] is JsonValue existsValue
            && existsValue.TryGetValue<bool>(out var existsExpected)
            && existsExpected != hasValue)
        {
            return false;
        }

        if (condition.ContainsKey("equals") && !JsonNodesEqual(condition["equals"], value))
        {
            return false;
        }

        if (condition.ContainsKey("not_equals") && JsonNodesEqual(condition["not_equals"], value))
        {
            return false;
        }

        if (condition["in"] is JsonArray inValues && !inValues.Any(item => JsonNodesEqual(item, value)))
        {
            return false;
        }

        if (condition["not_in"] is JsonArray notInValues && notInValues.Any(item => JsonNodesEqual(item, value)))
        {
            return false;
        }

        return true;
    }

    private static void ValidateString(string key, JsonNode? value, JsonObject rule, ICollection<string> errors)
    {
        if (value is not JsonValue stringValue || !stringValue.TryGetValue<string>(out var text))
        {
            errors.Add($"{key} must be a string");
            return;
        }

        if (rule["min_length"] is JsonValue minLengthValue
            && minLengthValue.TryGetValue<int>(out var minLength)
            && text.Length < minLength)
        {
            errors.Add($"{key} length must be at least {minLength}");
        }
    }

    private static void ValidateNumber(string key, JsonNode? value, JsonObject rule, ICollection<string> errors)
    {
        if (value is not JsonValue numberValue || !numberValue.TryGetValue<double>(out var number) || !double.IsFinite(number))
        {
            errors.Add($"{key} must be a number");
            return;
        }

        if (rule["min"] is JsonValue minValue
            && minValue.TryGetValue<double>(out var min)
            && number < min)
        {
            errors.Add($"{key} must be >= {min}");
        }

        if (rule["max"] is JsonValue maxValue
            && maxValue.TryGetValue<double>(out var max)
            && number > max)
        {
            errors.Add($"{key} must be <= {max}");
        }
    }

    private static bool JsonNodesEqual(JsonNode? left, JsonNode? right)
    {
        if (left is null && right is null)
        {
            return true;
        }

        if (left is null || right is null)
        {
            return false;
        }

        return string.Equals(left.ToJsonString(), right.ToJsonString(), StringComparison.Ordinal);
    }

    public static bool ReadRequirePreflight(JsonObject? governance)
    {
        return governance?["preflight_gate_required"]?.GetValue<bool?>() != false;
    }

    public static JsonObject BuildGovernance(bool requirePreflight)
    {
        return new JsonObject
        {
            ["preflight_gate_required"] = requirePreflight,
            ["auto_fix_enabled"] = true,
            ["acceptance_mode"] = "preflight_autofix_recheck_v1",
        };
    }

    public static IReadOnlyList<WorkflowAppSchemaField> EnumerateFields(JsonObject schema)
    {
        var normalized = NormalizeSchemaObject(schema);
        return normalized.Select(property =>
            new WorkflowAppSchemaField(
                property.Key,
                ((property.Value as JsonObject)?["type"]?.GetValue<string>()) ?? "string",
                ((property.Value as JsonObject)?["required"]?.GetValue<bool?>()) == true,
                (property.Value as JsonObject)?["default"]?.DeepClone(),
                ((property.Value as JsonObject)?["description"]?.GetValue<string>()) ?? string.Empty,
                ((property.Value as JsonObject)?["enum"] as JsonArray)
                    ?.OfType<JsonValue>()
                    .Select(static item => item.GetValue<string>())
                    .Where(static item => !string.IsNullOrWhiteSpace(item))
                    .ToArray()))
            .ToArray();
    }

    public static string BuildSchemaErrorText(string message)
    {
        return string.IsNullOrWhiteSpace(message) ? "Schema JSON is invalid." : message;
    }
}
