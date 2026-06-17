using Xunit;

namespace AIWF.Native.Tests;

public sealed class WorkflowTemplateSurfaceAssetTests
{
    [Fact]
    public void MainWindowXaml_DeclaresWorkflowTemplateSurfaceInCanvasPropertyPane()
    {
        var xamlPath = ResolveSourcePath("MainWindow.xaml");
        var xaml = File.ReadAllText(xamlPath);

        Assert.Contains("Text=\"Workflow Templates\"", xaml, StringComparison.Ordinal);
        Assert.Contains("x:Name=\"WorkflowTemplateSelectComboBox\"", xaml, StringComparison.Ordinal);
        Assert.Contains("AutomationProperties.AutomationId=\"WorkflowTemplateSelectComboBox\"", xaml, StringComparison.Ordinal);
        Assert.Contains("x:Name=\"RefreshWorkflowTemplatesButton\"", xaml, StringComparison.Ordinal);
        Assert.Contains("x:Name=\"ApplySelectedWorkflowTemplateButton\"", xaml, StringComparison.Ordinal);
        Assert.Contains("x:Name=\"SaveCurrentWorkflowAsTemplateButton\"", xaml, StringComparison.Ordinal);
        Assert.Contains("x:Name=\"InstallWorkflowTemplatePackButton\"", xaml, StringComparison.Ordinal);
        Assert.Contains("x:Name=\"RemoveWorkflowTemplatePackButton\"", xaml, StringComparison.Ordinal);
        Assert.Contains("x:Name=\"ExportWorkflowTemplatePackButton\"", xaml, StringComparison.Ordinal);
        Assert.Contains("x:Name=\"WorkflowTemplateOriginTextBlock\"", xaml, StringComparison.Ordinal);
        Assert.Contains("x:Name=\"WorkflowTemplateParamsFormHost\"", xaml, StringComparison.Ordinal);
        Assert.Contains("x:Name=\"WorkflowTemplateParamsJsonTextBox\"", xaml, StringComparison.Ordinal);
        Assert.Contains("x:Name=\"SyncWorkflowTemplateParamsJsonButton\"", xaml, StringComparison.Ordinal);
        Assert.Contains("x:Name=\"LoadWorkflowTemplateParamsJsonButton\"", xaml, StringComparison.Ordinal);
        Assert.Contains("x:Name=\"TemplateRequirePreflightCheckBox\"", xaml, StringComparison.Ordinal);
        Assert.Contains("x:Name=\"WorkflowTemplateStatusTextBlock\"", xaml, StringComparison.Ordinal);

        var deleteConnectionIndex = xaml.IndexOf("x:Name=\"DeleteConnectionButton\"", StringComparison.Ordinal);
        var workflowTemplatesIndex = xaml.IndexOf("Text=\"Workflow Templates\"", StringComparison.Ordinal);
        Assert.True(deleteConnectionIndex >= 0, "DeleteConnectionButton should exist before the template section.");
        Assert.True(workflowTemplatesIndex > deleteConnectionIndex, "Workflow Templates section should appear after delete node/connection controls.");
    }

    [Fact]
    public void WorkflowTemplatePartials_InitializeSectionAndSyncPublishDefaults()
    {
        var setupPath = ResolveSourcePath("MainWindow.Canvas.Setup.cs");
        var templatePath = ResolveSourcePath("MainWindow.WorkflowTemplates.cs");
        var workflowAppsPath = ResolveSourcePath("MainWindow.WorkflowApps.cs");
        var windowCodeBehindPath = ResolveSourcePath("MainWindow.xaml.cs");
        var projectPath = ResolveProjectPath("WinUI3Bootstrap.csproj");

        var setup = File.ReadAllText(setupPath);
        var template = File.ReadAllText(templatePath);
        var workflowApps = File.ReadAllText(workflowAppsPath);
        var windowCodeBehind = File.ReadAllText(windowCodeBehindPath);
        var project = File.ReadAllText(projectPath);

        Assert.Contains("InitializeWorkflowTemplateSection();", setup, StringComparison.Ordinal);
        Assert.Contains("_workflowTemplateAuthoringCoordinator.RefreshTemplatesAsync()", template, StringComparison.Ordinal);
        Assert.Contains("_workflowTemplateAuthoringCoordinator.ApplySelectedTemplateAsync(", template, StringComparison.Ordinal);
        Assert.Contains("_workflowTemplateAuthoringCoordinator.SaveCurrentAsTemplateAsync(", template, StringComparison.Ordinal);
        Assert.Contains("_workflowTemplateAuthoringCoordinator.InstallTemplatePackAsync(", template, StringComparison.Ordinal);
        Assert.Contains("_workflowTemplateAuthoringCoordinator.RemoveTemplatePackAsync(", template, StringComparison.Ordinal);
        Assert.Contains("_workflowTemplateAuthoringCoordinator.ExportTemplatePackAsync(", template, StringComparison.Ordinal);
        Assert.Contains("SyncPublishSurfaceFromTemplateDefaults(", template, StringComparison.Ordinal);
        Assert.Contains("UpdateWorkflowAppTemplatePolicyPreviewFromCurrentState();", template, StringComparison.Ordinal);
        Assert.DoesNotContain("_governanceClient", template, StringComparison.Ordinal);
        Assert.DoesNotContain("HttpClient", template, StringComparison.Ordinal);
        Assert.Contains("if (!IsWorkflowAppPublishSurfaceReady())", workflowApps, StringComparison.Ordinal);
        Assert.Contains("private bool IsWorkflowAppPublishSurfaceReady()", workflowApps, StringComparison.Ordinal);
        Assert.Contains("UpdateWorkflowAppTemplatePolicyPreviewFromCurrentState();", windowCodeBehind, StringComparison.Ordinal);

        Assert.Contains("contracts\\desktop\\workflow_builtin_templates.v1.json", project, StringComparison.Ordinal);
        Assert.Contains("<CopyToOutputDirectory>PreserveNewest</CopyToOutputDirectory>", project, StringComparison.Ordinal);
    }

    private static string ResolveSourcePath(string fileName)
    {
        return Path.GetFullPath(Path.Combine(
            AppContext.BaseDirectory,
            "..",
            "..",
            "..",
            "..",
            "..",
            "src",
            "WinUI3Bootstrap",
            fileName));
    }

    private static string ResolveProjectPath(string fileName)
    {
        return ResolveSourcePath(fileName);
    }
}
