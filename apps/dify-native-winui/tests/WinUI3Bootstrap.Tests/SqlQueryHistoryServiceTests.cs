using AIWF.Native.Runtime;
using Xunit;

namespace AIWF.Native.Tests;

public sealed class SqlQueryHistoryServiceTests
{
    [Fact]
    public void AddHistoryEntry_InsertsAtFront()
    {
        var svc = new SqlQueryHistoryService();
        svc.AddHistoryEntry("SELECT 1", "sqlite", "test.db", 1, true);
        svc.AddHistoryEntry("SELECT 2", "sqlite", "test.db", 2, true);

        Assert.Equal(2, svc.History.Count);
        Assert.Equal("SELECT 2", svc.History[0].SqlText);
        Assert.Equal("SELECT 1", svc.History[1].SqlText);
    }

    [Fact]
    public void SaveQuery_UpsertsByName()
    {
        var svc = new SqlQueryHistoryService();
        svc.SaveQuery("my_query", "SELECT 1", "sqlite", "test.db");
        svc.SaveQuery("my_query", "SELECT 2", "sqlite", "test.db");

        Assert.Single(svc.SavedQueries);
        Assert.Equal("SELECT 2", svc.SavedQueries[0].SqlText);
    }

    [Fact]
    public void DeleteSavedQuery_RemovesByIndex()
    {
        var svc = new SqlQueryHistoryService();
        svc.SaveQuery("q1", "SELECT 1", "sqlite", "test.db");
        svc.SaveQuery("q2", "SELECT 2", "sqlite", "test.db");

        svc.DeleteSavedQuery(0);

        Assert.Single(svc.SavedQueries);
        Assert.Equal("q1", svc.SavedQueries[0].Name);
    }

    [Fact]
    public void HistoryEntry_DisplayText_ShowsTimestampAndStatus()
    {
        var entry = new SqlQueryHistoryEntry(
            Timestamp: "2026-04-01 12:00:00",
            SqlText: "SELECT * FROM users WHERE active = 1",
            SourceType: "sqlite",
            Database: "test.db",
            RowCount: 42,
            Success: true);

        Assert.Contains("[2026-04-01 12:00:00]", entry.DisplayText, StringComparison.Ordinal);
        Assert.Contains("OK", entry.DisplayText, StringComparison.Ordinal);
        Assert.Contains("42 rows", entry.DisplayText, StringComparison.Ordinal);
    }

    [Fact]
    public void SavedQuery_DisplayText_ShowsStarAndName()
    {
        var entry = new SqlQueryHistoryEntry(
            Timestamp: "2026-04-01 12:00:00",
            SqlText: "SELECT * FROM users",
            SourceType: "sqlite",
            Database: "test.db",
            RowCount: 0,
            Success: true,
            Name: "All Users");

        Assert.StartsWith("\u2605 All Users", entry.DisplayText, StringComparison.Ordinal);
    }
}
