using AIWF.Native.CanvasRuntime;
using Xunit;

namespace AIWF.Native.Tests;

public sealed class CanvasWorkflowScaffoldTests
{
    [Fact]
    public void CreateDefaultNodesAndEdges_ReturnsExpectedWorkflowScaffold()
    {
        var nodes = CanvasWorkflowScaffold.CreateDefaultNodes();
        var edges = CanvasWorkflowScaffold.CreateDefaultEdges();

        Assert.Collection(
            nodes,
            node =>
            {
                Assert.Equal(CanvasWorkflowScaffold.InputNodeKey, node.NodeKey);
                Assert.Equal("输入", node.Title);
                Assert.Equal("源数据准备", node.Subtitle);
                Assert.False(node.IsUserNode);
                Assert.False(node.IsArtifactNode);
            },
            node =>
            {
                Assert.Equal(CanvasWorkflowScaffold.CleanNodeKey, node.NodeKey);
                Assert.Equal("清洗", node.Title);
                Assert.Equal("规则处理", node.Subtitle);
                Assert.False(node.IsUserNode);
                Assert.False(node.IsArtifactNode);
            },
            node =>
            {
                Assert.Equal(CanvasWorkflowScaffold.OutputNodeKey, node.NodeKey);
                Assert.Equal("输出", node.Title);
                Assert.Equal("等待运行结果", node.Subtitle);
                Assert.False(node.IsUserNode);
                Assert.False(node.IsArtifactNode);
            });

        Assert.Collection(
            edges,
            edge =>
            {
                Assert.Equal(CanvasWorkflowScaffold.InputNodeKey, edge.SourceKey);
                Assert.Equal(CanvasWorkflowScaffold.CleanNodeKey, edge.TargetKey);
            },
            edge =>
            {
                Assert.Equal(CanvasWorkflowScaffold.CleanNodeKey, edge.SourceKey);
                Assert.Equal(CanvasWorkflowScaffold.OutputNodeKey, edge.TargetKey);
            });
    }
}
