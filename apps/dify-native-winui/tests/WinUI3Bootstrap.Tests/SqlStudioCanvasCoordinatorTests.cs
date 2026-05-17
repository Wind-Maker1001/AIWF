using AIWF.Native.Runtime;
using Xunit;

namespace AIWF.Native.Tests;

public sealed class SqlStudioCanvasCoordinatorTests
{
    [Fact]
    public void BuildWorkflowDocument_UsesDefaultWorkflowIdWhenTableMissing()
    {
        var coordinator = new SqlStudioCanvasCoordinator();
        var profile = SqlConnectionProfile.Default with
        {
            SourceType = SqlConnectionProfile.Sqlite,
            SQLitePath = @"D:\demo.db"
        };

        var result = coordinator.BuildWorkflowDocument(
            profile,
            SqlBuilderDraft.Empty,
            new SqlTextDraft("SELECT * FROM users", true),
            selectedTableName: string.Empty);

        Assert.Equal("native_sql_workflow", result.Document.WorkflowId);
        Assert.Equal("SELECT * FROM users", result.EffectiveTextDraft.Text);
        Assert.Equal("SQL 草稿已发送到画布。", result.StatusMessage);
    }

    [Fact]
    public void BuildWorkflowDocument_UsesBuilderGeneratedSqlAndSanitizedWorkflowId()
    {
        var coordinator = new SqlStudioCanvasCoordinator();
        var profile = SqlConnectionProfile.Default with
        {
            SourceType = SqlConnectionProfile.Sqlite,
            SQLitePath = @"D:\demo.db"
        };
        var draft = SqlBuilderDraft.Empty with
        {
            Schema = "main",
            Table = "sales",
            Limit = 25
        };

        var result = coordinator.BuildWorkflowDocument(
            profile,
            draft,
            SqlTextDraft.Empty,
            selectedTableName: "My Report");

        var expectedSql = SqlStudioDraftController.SyncGeneratedSql(draft, profile, SqlTextDraft.Empty).Text;
        Assert.Equal("sql_My_Report", result.Document.WorkflowId);
        Assert.Equal(expectedSql, result.EffectiveTextDraft.Text);
        Assert.Contains(result.Document.Nodes, node => node.Type == "load_rows_v3");
    }
}
