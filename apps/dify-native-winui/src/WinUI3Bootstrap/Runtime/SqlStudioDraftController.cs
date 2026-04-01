namespace AIWF.Native.Runtime;

public static class SqlStudioDraftController
{
    public static SqlTextDraft SyncGeneratedSql(
        SqlBuilderDraft draft,
        SqlConnectionProfile profile,
        SqlTextDraft current)
    {
        if (current.IsTextOwned)
        {
            return current;
        }

        return new SqlTextDraft(SqlAuthoringBuilderCompiler.BuildSql(draft, profile), false);
    }

    public static SqlTextDraft MarkTextOwned(string text)
    {
        return new SqlTextDraft(text ?? string.Empty, true);
    }

    public static SqlTextDraft ResetToBuilder(SqlBuilderDraft draft, SqlConnectionProfile profile)
    {
        return new SqlTextDraft(SqlAuthoringBuilderCompiler.BuildSql(draft, profile), false);
    }
}
