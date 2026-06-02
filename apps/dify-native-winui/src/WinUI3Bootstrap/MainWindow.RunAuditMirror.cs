using System.Text.Json.Nodes;

namespace AIWF.Native;

public sealed partial class MainWindow
{
    private void TryMirrorWorkflowRun(
        string body,
        string fallbackRunId,
        JsonObject? payload = null,
        string? workflowId = null,
        string? runRequestKind = null,
        string? versionId = null,
        string? publishedVersionId = null,
        string? workflowDefinitionSource = null)
    {
        try
        {
            _workflowRunAuditStoreService.AppendFromResponse(
                body,
                fallbackRunId,
                payload,
                workflowId,
                runRequestKind,
                versionId,
                publishedVersionId,
                workflowDefinitionSource);
        }
        catch
        {
        }
    }
}
