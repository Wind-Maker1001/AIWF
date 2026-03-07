using System.IO;

namespace AIWF.Native.Runtime;

public sealed record ArtifactCanvasNodePlacement(
    string NodeKey,
    string Title,
    string Subtitle,
    double Left,
    double Top,
    string ArtifactPath,
    string ArtifactKind);

public static class ArtifactPresentationMapper
{
    public static string FormatListDisplay(RunArtifactItem artifact)
    {
        return FormatListDisplay(artifact.Kind, artifact.Path, artifact.ArtifactId);
    }

    public static string FormatListDisplay(string kind, string path, string fallbackId)
    {
        var fileName = string.IsNullOrWhiteSpace(path) ? string.Empty : Path.GetFileName(path);
        if (string.IsNullOrWhiteSpace(fileName))
        {
            fileName = fallbackId;
        }

        var kindLabel = kind switch
        {
            "csv" => "数据表 CSV",
            "parquet" => "分析文件 Parquet",
            "xlsx" => "Excel 报表",
            "docx" => "Word 审计文档",
            "pptx" => "PPT 演示稿",
            "json" => "JSON 资料",
            _ => "文件"
        };

        return $"{kindLabel} - {fileName}";
    }

    public static string ToCanvasTitle(string kind)
    {
        return kind switch
        {
            "csv" => "CSV",
            "parquet" => "Parquet",
            "xlsx" => "Excel",
            "docx" => "Word",
            "pptx" => "PPT",
            "json" => "JSON",
            _ => "文件"
        };
    }

    public static IReadOnlyList<ArtifactCanvasNodePlacement> BuildCanvasPlacements(
        IReadOnlyList<RunArtifactItem> artifacts,
        double startX = 1060.0,
        double startY = 100.0,
        int rows = 4,
        double gapX = 250.0,
        double gapY = 118.0)
    {
        var placements = new List<ArtifactCanvasNodePlacement>(artifacts.Count);
        for (var i = 0; i < artifacts.Count; i++)
        {
            var artifact = artifacts[i];
            var fileName = string.IsNullOrWhiteSpace(artifact.Path) ? artifact.ArtifactId : Path.GetFileName(artifact.Path);
            if (string.IsNullOrWhiteSpace(fileName))
            {
                fileName = artifact.ArtifactId;
            }

            var col = i / rows;
            var row = i % rows;
            placements.Add(new ArtifactCanvasNodePlacement(
                $"artifact-{i}",
                ToCanvasTitle(artifact.Kind),
                fileName,
                startX + (col * gapX),
                startY + (row * gapY),
                artifact.Path,
                artifact.Kind));
        }

        return placements;
    }
}
