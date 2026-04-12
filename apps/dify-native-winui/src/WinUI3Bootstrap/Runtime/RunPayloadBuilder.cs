using System.Text.Json.Nodes;

namespace AIWF.Native.Runtime;

public sealed record RunPayloadInput(
    string Actor,
    string OfficeTheme,
    string OfficeLang,
    string ReportTitle,
    string? InputCsvPath);

public sealed record CleaningPrecheckPayloadInput(
    string InputPath,
    string CleaningTemplate);

public static class RunPayloadBuilder
{
    public static JsonObject BuildCleaningPayload(RunPayloadInput input)
    {
        var paramsObj = new JsonObject
        {
            ["office_theme"] = input.OfficeTheme,
            ["office_lang"] = input.OfficeLang,
            ["report_title"] = input.ReportTitle
        };

        var csv = (input.InputCsvPath ?? string.Empty).Trim();
        if (!string.IsNullOrWhiteSpace(csv))
        {
            paramsObj["input_csv_path"] = csv;
        }

        return new JsonObject
        {
            ["actor"] = (input.Actor ?? string.Empty).Trim(),
            ["ruleset_version"] = "v1",
            ["params"] = paramsObj
        };
    }

    public static JsonObject BuildCleaningPrecheckPayload(CleaningPrecheckPayloadInput input)
    {
        var payload = new JsonObject
        {
            ["input_path"] = (input.InputPath ?? string.Empty).Trim(),
            ["header_mapping_mode"] = "auto",
            ["blank_output_expected"] = false,
            ["profile_mismatch_action"] = "block"
        };

        var template = (input.CleaningTemplate ?? string.Empty).Trim();
        if (!string.IsNullOrWhiteSpace(template))
        {
            payload["cleaning_template"] = template;
        }

        return payload;
    }
}
