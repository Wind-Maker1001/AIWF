using System.Text.Json.Nodes;
using AIWF.Native.Runtime;
using Xunit;

namespace AIWF.Native.Tests;

public sealed class SqlChartDataTests
{
    [Fact]
    public void FromJson_ParsesChartPayload()
    {
        var payload = new JsonObject
        {
            ["chart_type"] = "bar",
            ["categories"] = new JsonArray("Q1", "Q2", "Q3"),
            ["series"] = new JsonArray(
                new JsonObject
                {
                    ["name"] = "revenue",
                    ["data"] = new JsonArray(100.0, 200.0, 150.0)
                },
                new JsonObject
                {
                    ["name"] = "cost",
                    ["data"] = new JsonArray(80.0, 120.0, 110.0)
                })
        };

        var data = SqlChartData.FromJson(payload);

        Assert.Equal("bar", data.ChartType);
        Assert.Equal(3, data.Categories.Count);
        Assert.Equal("Q1", data.Categories[0]);
        Assert.Equal(2, data.Series.Count);
        Assert.Equal("revenue", data.Series[0].Name);
        Assert.Equal(200.0, data.Series[0].Data[1]);
        Assert.Equal("cost", data.Series[1].Name);
        Assert.Equal(200.0, data.MaxValue);
    }

    [Fact]
    public void FromJson_HandlesNull()
    {
        var data = SqlChartData.FromJson(null);

        Assert.Equal("bar", data.ChartType);
        Assert.Empty(data.Categories);
        Assert.Empty(data.Series);
        Assert.Equal(0, data.MaxValue);
    }

    [Fact]
    public void FromJson_HandlesMissingFields()
    {
        var payload = new JsonObject
        {
            ["chart_type"] = "pie",
        };

        var data = SqlChartData.FromJson(payload);

        Assert.Equal("pie", data.ChartType);
        Assert.Empty(data.Categories);
        Assert.Empty(data.Series);
    }
}
