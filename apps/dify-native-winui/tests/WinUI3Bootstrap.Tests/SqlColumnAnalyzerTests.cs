using AIWF.Native.Runtime;
using Xunit;

namespace AIWF.Native.Tests;

public sealed class SqlColumnAnalyzerTests
{
    [Fact]
    public void Analyze_ComputesBasicStats()
    {
        var columns = new[] { "id", "name", "amount" };
        var rows = new IReadOnlyList<string>[]
        {
            new[] { "1", "Alice", "100.5" },
            new[] { "2", "Bob", "200" },
            new[] { "3", "", "50" },
        };

        var stats = SqlColumnAnalyzer.Analyze(columns, rows);

        Assert.Equal(3, stats.Count);

        // id column
        Assert.Equal("id", stats[0].ColumnName);
        Assert.Equal(3, stats[0].RowCount);
        Assert.Equal(0, stats[0].NullCount);
        Assert.Equal(3, stats[0].DistinctCount);
        Assert.NotNull(stats[0].NumericMean);
        Assert.Equal(2.0, stats[0].NumericMean!.Value, 1);

        // name column
        Assert.Equal("name", stats[1].ColumnName);
        Assert.Equal(1, stats[1].NullCount); // empty string counts as null
        Assert.Equal(2, stats[1].DistinctCount);
        Assert.Null(stats[1].NumericMean);

        // amount column
        Assert.Equal("amount", stats[2].ColumnName);
        Assert.NotNull(stats[2].NumericSum);
        Assert.Equal(350.5, stats[2].NumericSum!.Value, 1);
    }

    [Fact]
    public void Analyze_HandlesEmptyRows()
    {
        var columns = new[] { "x" };
        var rows = Array.Empty<IReadOnlyList<string>>();

        var stats = SqlColumnAnalyzer.Analyze(columns, rows);

        Assert.Single(stats);
        Assert.Equal(0, stats[0].RowCount);
        Assert.Equal(0, stats[0].NullCount);
        Assert.Null(stats[0].MinValue);
    }

    [Fact]
    public void Summary_IncludesNumericStatsWhenApplicable()
    {
        var columns = new[] { "score" };
        var rows = new IReadOnlyList<string>[]
        {
            new[] { "10" },
            new[] { "20" },
            new[] { "30" },
        };

        var stats = SqlColumnAnalyzer.Analyze(columns, rows);
        var summary = stats[0].Summary;

        Assert.Contains("avg=20", summary, StringComparison.Ordinal);
        Assert.Contains("sum=60", summary, StringComparison.Ordinal);
        Assert.Contains("min=10", summary, StringComparison.Ordinal);
        Assert.Contains("max=30", summary, StringComparison.Ordinal);
    }
}
