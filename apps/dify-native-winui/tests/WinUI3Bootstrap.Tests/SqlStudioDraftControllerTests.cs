using AIWF.Native.Runtime;
using Xunit;

namespace AIWF.Native.Tests;

public sealed class SqlStudioDraftControllerTests
{
    [Fact]
    public void SyncGeneratedSql_DoesNotOverrideTextOwnedDraft()
    {
        var draft = SqlBuilderDraft.Empty with
        {
            Schema = "main",
            Table = "sales"
        };
        var current = SqlStudioDraftController.MarkTextOwned("SELECT 42");

        var next = SqlStudioDraftController.SyncGeneratedSql(draft, SqlConnectionProfile.Default, current);

        Assert.True(next.IsTextOwned);
        Assert.Equal("SELECT 42", next.Text);
    }

    [Fact]
    public void ResetToBuilder_ClearsTextOwnedFlag()
    {
        var draft = SqlBuilderDraft.Empty with
        {
            Schema = "main",
            Table = "sales"
        };

        var next = SqlStudioDraftController.ResetToBuilder(draft, SqlConnectionProfile.Default);

        Assert.False(next.IsTextOwned);
        Assert.Contains("SELECT", next.Text, StringComparison.Ordinal);
    }
}
