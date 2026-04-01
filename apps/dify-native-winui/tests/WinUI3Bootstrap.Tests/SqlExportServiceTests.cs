using AIWF.Native.Runtime;
using Xunit;

namespace AIWF.Native.Tests;

public sealed class SqlExportServiceTests
{
    [Fact]
    public void ExportCsv_ProducesValidCsvWithHeaders()
    {
        var columns = new[] { "id", "name", "amount" };
        var rows = new IReadOnlyList<string>[]
        {
            new[] { "1", "Alice", "100.5" },
            new[] { "2", "Bob", "200" },
        };

        var csv = SqlExportService.ExportCsv(columns, rows);

        Assert.StartsWith("id,name,amount", csv, StringComparison.Ordinal);
        Assert.Contains("1,Alice,100.5", csv, StringComparison.Ordinal);
        Assert.Contains("2,Bob,200", csv, StringComparison.Ordinal);
    }

    [Fact]
    public void ExportCsv_EscapesCommasAndQuotes()
    {
        var columns = new[] { "value" };
        var rows = new IReadOnlyList<string>[]
        {
            new[] { "hello, world" },
            new[] { "say \"hi\"" },
        };

        var csv = SqlExportService.ExportCsv(columns, rows);

        Assert.Contains("\"hello, world\"", csv, StringComparison.Ordinal);
        Assert.Contains("\"say \"\"hi\"\"\"", csv, StringComparison.Ordinal);
    }

    [Fact]
    public void ExportJson_ProducesValidJsonArray()
    {
        var columns = new[] { "id", "name" };
        var rows = new IReadOnlyList<string>[]
        {
            new[] { "1", "Alice" },
        };

        var json = SqlExportService.ExportJson(columns, rows);

        Assert.Contains("\"id\": \"1\"", json, StringComparison.Ordinal);
        Assert.Contains("\"name\": \"Alice\"", json, StringComparison.Ordinal);
    }
}
