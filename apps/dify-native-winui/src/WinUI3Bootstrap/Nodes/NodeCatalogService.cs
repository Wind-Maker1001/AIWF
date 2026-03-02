using Microsoft.UI.Xaml.Controls;

namespace AIWF.Native.Nodes;

public sealed record NodeCatalogItem(
    string KeyPrefix,
    string Title,
    string Subtitle,
    Symbol Icon,
    string Group);

public sealed class NodeCatalogService
{
    public IReadOnlyList<NodeCatalogItem> GetQuickTemplates()
    {
        return
        [
            new NodeCatalogItem("create-job", "新建任务", "开始一次新的处理", Symbol.Add, "基础操作"),
            new NodeCatalogItem("run-cleaning", "开始清洗", "执行清洗流程", Symbol.Play, "基础操作"),
            new NodeCatalogItem("check-steps", "查看进度", "看每一步执行状态", Symbol.Clock, "基础操作"),
            new NodeCatalogItem("check-files", "查看文件", "看生成了哪些结果文件", Symbol.Document, "基础操作"),
            new NodeCatalogItem("check-job", "查看状态", "看任务当前状态", Symbol.View, "基础操作"),
            new NodeCatalogItem("one-click", "一键清洗", "一键执行并返回结果", Symbol.Accept, "基础操作"),
            new NodeCatalogItem("process", "数据处理", "整理和清洗字段", Symbol.Switch, "数据处理"),
            new NodeCatalogItem("load", "数据加载", "读取或写入数据", Symbol.Upload, "数据处理"),
            new NodeCatalogItem("join", "数据合并", "按条件合并两组数据", Symbol.Shuffle, "数据处理"),
            new NodeCatalogItem("summary", "数据汇总", "分组统计结果", Symbol.AllApps, "数据处理"),
            new NodeCatalogItem("quality", "质量检查", "检查异常和质量问题", Symbol.Important, "数据处理"),
            new NodeCatalogItem("custom", "空白节点", "自定义名称和说明", Symbol.Page, "其他")
        ];
    }
}
