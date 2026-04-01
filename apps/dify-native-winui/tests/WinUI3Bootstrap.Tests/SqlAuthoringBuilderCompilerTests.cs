using AIWF.Native.Runtime;
using Xunit;

namespace AIWF.Native.Tests;

public sealed class SqlAuthoringBuilderCompilerTests
{
    [Fact]
    public void BuildSql_RendersSqliteLimitAtTail()
    {
        var sql = SqlAuthoringBuilderCompiler.BuildSql(
            SqlBuilderDraft.Empty with
            {
                Schema = "main",
                Table = "sales",
                SelectFields = ["id", "amount"],
                Filters = [new SqlFilterClause("category", "=", "books")],
                Limit = 25
            },
            SqlConnectionProfile.Default with
            {
                SourceType = SqlConnectionProfile.Sqlite,
                SQLitePath = @"D:\demo.db"
            });

        Assert.Contains("SELECT id, amount FROM \"main\".\"sales\"", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE \"main\".\"sales\".\"category\" = 'books'", sql, StringComparison.Ordinal);
        Assert.EndsWith("LIMIT 25", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildSql_RendersSqlServerTopAndJoin()
    {
        var sql = SqlAuthoringBuilderCompiler.BuildSql(
            SqlBuilderDraft.Empty with
            {
                Schema = "dbo",
                Table = "orders",
                SelectFields = ["id"],
                Joins =
                [
                    new SqlJoinClause(true, "dbo", "customers", "left", "customer_id", "id")
                ],
                Limit = 10
            },
            SqlConnectionProfile.Default with
            {
                SourceType = SqlConnectionProfile.SqlServer
            });

        Assert.StartsWith("SELECT TOP 10 id FROM [dbo].[orders]", sql, StringComparison.Ordinal);
        Assert.Contains("LEFT JOIN [dbo].[customers]", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildSql_RendersMultipleFilters()
    {
        var sql = SqlAuthoringBuilderCompiler.BuildSql(
            SqlBuilderDraft.Empty with
            {
                Schema = "main",
                Table = "orders",
                Filters =
                [
                    new SqlFilterClause("status", "=", "paid"),
                    new SqlFilterClause("amount", ">", "100"),
                    new SqlFilterClause("region", "LIKE", "%cn%"),
                ],
            },
            SqlConnectionProfile.Default with { SourceType = SqlConnectionProfile.Sqlite });

        Assert.Contains("WHERE", sql, StringComparison.Ordinal);
        Assert.Contains("\"status\" = 'paid'", sql, StringComparison.Ordinal);
        Assert.Contains("\"amount\" > 100", sql, StringComparison.Ordinal);
        Assert.Contains("\"region\" LIKE '%cn%'", sql, StringComparison.Ordinal);
        // All three filters should be joined with AND
        Assert.Equal(2, sql.Split(" AND ").Length - 1);
    }

    [Fact]
    public void BuildSql_RendersHavingClause()
    {
        var sql = SqlAuthoringBuilderCompiler.BuildSql(
            SqlBuilderDraft.Empty with
            {
                Schema = "main",
                Table = "sales",
                GroupByFields = ["category"],
                Aggregates = [new SqlAggregateClause("count", "*", "cnt")],
                Having = new SqlHavingClause("COUNT(*) > 5"),
            },
            SqlConnectionProfile.Default with { SourceType = SqlConnectionProfile.Sqlite });

        Assert.Contains("GROUP BY", sql, StringComparison.Ordinal);
        Assert.Contains("HAVING COUNT(*) > 5", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildSql_RendersMultipleAggregates()
    {
        var sql = SqlAuthoringBuilderCompiler.BuildSql(
            SqlBuilderDraft.Empty with
            {
                Schema = "main",
                Table = "sales",
                GroupByFields = ["category"],
                Aggregates =
                [
                    new SqlAggregateClause("count", "*", "cnt"),
                    new SqlAggregateClause("sum", "amount", "total"),
                ],
            },
            SqlConnectionProfile.Default with { SourceType = SqlConnectionProfile.Sqlite });

        Assert.Contains("COUNT(*) AS cnt", sql, StringComparison.Ordinal);
        Assert.Contains("SUM(amount) AS total", sql, StringComparison.Ordinal);
    }
}
