using System.Text.Json.Nodes;
using AIWF.Native.Runtime;
using Xunit;

namespace AIWF.Native.Tests;

public sealed class SqlStudioResultMapperTests
{
    [Fact]
    public void FromLoadRowsResponse_ExtractsColumnHeadersAndGridRows()
    {
        var payload = new JsonObject
        {
            ["ok"] = true,
            ["status"] = "done",
            ["rows"] = new JsonArray(
                new JsonObject { ["id"] = 1, ["name"] = "Alice", ["amount"] = 100.5 },
                new JsonObject { ["id"] = 2, ["name"] = "Bob", ["amount"] = 200.0 }),
            ["stats"] = new JsonObject { ["elapsed_ms"] = 42 }
        };

        var state = SqlStudioResultMapper.FromLoadRowsResponse(payload, "SELECT * FROM users");

        Assert.True(state.Ok);
        Assert.Equal(3, state.ColumnHeaders.Count);
        Assert.Equal("id", state.ColumnHeaders[0]);
        Assert.Equal("name", state.ColumnHeaders[1]);
        Assert.Equal("amount", state.ColumnHeaders[2]);

        Assert.Equal(2, state.GridRows.Count);
        Assert.Equal("1", state.GridRows[0][0]);
        Assert.Equal("Alice", state.GridRows[0][1]);
        Assert.Equal("100.5", state.GridRows[0][2]);
        Assert.Equal("2", state.GridRows[1][0]);
        Assert.Equal("Bob", state.GridRows[1][1]);

        Assert.Contains("2 row(s), 3 column(s)", state.StatusText);
    }

    [Fact]
    public void FromLoadRowsResponse_HandlesEmptyRowsGracefully()
    {
        var payload = new JsonObject
        {
            ["ok"] = true,
            ["status"] = "done",
            ["rows"] = new JsonArray(),
            ["stats"] = new JsonObject()
        };

        var state = SqlStudioResultMapper.FromLoadRowsResponse(payload, "SELECT 1");

        Assert.True(state.Ok);
        Assert.Empty(state.ColumnHeaders);
        Assert.Empty(state.GridRows);
        Assert.Contains("0 row(s), 0 column(s)", state.StatusText);
    }

    [Fact]
    public void FromLoadRowsResponse_HandlesNullPayload()
    {
        var state = SqlStudioResultMapper.FromLoadRowsResponse(null, "SELECT 1");

        Assert.False(state.Ok);
        Assert.Empty(state.ColumnHeaders);
        Assert.Empty(state.GridRows);
    }

    [Fact]
    public void FromLoadRowsResponse_UnionsColumnsAcrossRows()
    {
        var payload = new JsonObject
        {
            ["ok"] = true,
            ["rows"] = new JsonArray(
                new JsonObject { ["a"] = 1 },
                new JsonObject { ["a"] = 2, ["b"] = 3 }),
        };

        var state = SqlStudioResultMapper.FromLoadRowsResponse(payload, "");

        Assert.Equal(2, state.ColumnHeaders.Count);
        Assert.Equal("a", state.ColumnHeaders[0]);
        Assert.Equal("b", state.ColumnHeaders[1]);
        Assert.Equal(string.Empty, state.GridRows[0][1]);
        Assert.Equal("3", state.GridRows[1][1]);
    }
}
