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
}
