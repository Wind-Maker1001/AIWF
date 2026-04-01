using System.Text.Json.Nodes;

namespace AIWF.Native.Runtime;

public sealed record SqlChartData(
    string ChartType,
    IReadOnlyList<string> Categories,
    IReadOnlyList<SqlChartSeries> Series)
{
    public static SqlChartData Empty { get; } = new(
        ChartType: "bar",
        Categories: Array.Empty<string>(),
        Series: Array.Empty<SqlChartSeries>());

    public static SqlChartData FromJson(JsonObject? payload)
    {
        if (payload is null)
        {
            return Empty;
        }

        var chartType = payload["chart_type"]?.GetValue<string>() ?? "bar";
        var categories = (payload["categories"] as JsonArray)?
            .Select(item => item?.GetValue<string>() ?? string.Empty)
            .ToArray()
            ?? Array.Empty<string>();
        var seriesArray = (payload["series"] as JsonArray)?
            .OfType<JsonObject>()
            .Select(item =>
            {
                var name = item["name"]?.GetValue<string>() ?? string.Empty;
                var data = (item["data"] as JsonArray)?
                    .Select(d => d?.GetValue<double?>() ?? 0.0)
                    .ToArray()
                    ?? Array.Empty<double>();
                return new SqlChartSeries(name, data);
            })
            .ToArray()
            ?? Array.Empty<SqlChartSeries>();

        return new SqlChartData(chartType, categories, seriesArray);
    }

    public double MaxValue => Series.Count == 0
        ? 0
        : Series.SelectMany(s => s.Data).DefaultIfEmpty(0).Max();
}

public sealed record SqlChartSeries(string Name, IReadOnlyList<double> Data);
