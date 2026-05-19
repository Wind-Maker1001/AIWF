using System.Text;
using System.Text.Json;
using AIWF.Native.CanvasRuntime;

namespace AIWF.Native.Runtime;

internal sealed record CanvasSnapshotPersistenceLoadResult(bool Exists, string? Json, CanvasSnapshot? Snapshot);

internal sealed class CanvasAuthoringPersistenceService
{
    public CanvasAuthoringPersistenceService(string canvasSnapshotPath, string workflowGraphPath)
    {
        CanvasSnapshotPath = canvasSnapshotPath;
        WorkflowGraphPath = workflowGraphPath;
    }

    public string CanvasSnapshotPath { get; }

    public string WorkflowGraphPath { get; }

    public bool SaveSnapshot(CanvasSnapshot snapshot, JsonSerializerOptions options, string? previousJson)
    {
        var json = JsonSerializer.Serialize(snapshot, options);
        var dir = Path.GetDirectoryName(CanvasSnapshotPath) ?? ".";
        Directory.CreateDirectory(dir);
        var shouldWrite = CanvasSnapshotWriteDecider.ShouldWrite(
            previousJson,
            json,
            File.Exists(CanvasSnapshotPath));
        if (shouldWrite)
        {
            File.WriteAllText(CanvasSnapshotPath, json, Encoding.UTF8);
        }

        return shouldWrite;
    }

    public CanvasSnapshotPersistenceLoadResult LoadSnapshot(JsonSerializerOptions options)
    {
        if (!File.Exists(CanvasSnapshotPath))
        {
            return new CanvasSnapshotPersistenceLoadResult(false, null, null);
        }

        var json = File.ReadAllText(CanvasSnapshotPath, Encoding.UTF8);
        var snapshot = JsonSerializer.Deserialize<CanvasSnapshot>(json, options);
        return new CanvasSnapshotPersistenceLoadResult(true, json, snapshot);
    }

    public void SaveWorkflowGraph(WorkflowGraphDocument? document, JsonSerializerOptions options)
    {
        var dir = Path.GetDirectoryName(WorkflowGraphPath) ?? ".";
        Directory.CreateDirectory(dir);
        if (document is null)
        {
            if (File.Exists(WorkflowGraphPath))
            {
                File.Delete(WorkflowGraphPath);
            }
            return;
        }

        File.WriteAllText(
            WorkflowGraphPath,
            JsonSerializer.Serialize(document, options),
            Encoding.UTF8);
    }

    public WorkflowGraphDocument? LoadWorkflowGraph(JsonSerializerOptions options)
    {
        if (!File.Exists(WorkflowGraphPath))
        {
            return null;
        }

        return JsonSerializer.Deserialize<WorkflowGraphDocument>(
            File.ReadAllText(WorkflowGraphPath, Encoding.UTF8),
            options);
    }
}
