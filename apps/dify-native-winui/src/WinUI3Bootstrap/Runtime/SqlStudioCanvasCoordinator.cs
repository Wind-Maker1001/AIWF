namespace AIWF.Native.Runtime;

public sealed record SqlStudioCanvasWorkflowResult(
    WorkflowGraphDocument Document,
    SqlTextDraft EffectiveTextDraft,
    string StatusMessage);

public sealed class SqlStudioCanvasCoordinator
{
    public SqlStudioCanvasWorkflowResult BuildWorkflowDocument(
        SqlConnectionProfile profile,
        SqlBuilderDraft draft,
        SqlTextDraft textDraft,
        string selectedTableName)
    {
        var effectiveTextDraft = SqlStudioDraftController.SyncGeneratedSql(draft, profile, textDraft);
        var workflowId = string.IsNullOrWhiteSpace(selectedTableName)
            ? "native_sql_workflow"
            : $"sql_{selectedTableName.Trim().Replace(' ', '_')}";
        var document = SqlStudioWorkflowMapper.Build(profile, draft, effectiveTextDraft, workflowId);
        return new SqlStudioCanvasWorkflowResult(
            document,
            effectiveTextDraft,
            "SQL 草稿已发送到画布。");
    }
}
