using System.Linq;
using System.Numerics;
using System.Text.Json.Nodes;
using AIWF.Native.Runtime;
using Microsoft.UI.Xaml;
using Microsoft.UI.Xaml.Controls;
using Microsoft.UI.Xaml.Input;
using Microsoft.UI.Xaml.Media;
using Microsoft.UI.Xaml.Shapes;

namespace AIWF.Native;

public sealed partial class MainWindow
{
    private Border CreateCanvasNode(
        string nodeKey,
        string title,
        string subtitle,
        double left,
        double top,
        string? artifactPath = null,
        string? artifactKind = null,
        string? workflowNodeType = null,
        JsonObject? workflowConfig = null,
        bool isUserNode = false,
        bool isArtifactNode = false)
    {
        var titleBlock = new TextBlock
        {
            Text = title,
            FontSize = 16,
            FontWeight = Microsoft.UI.Text.FontWeights.SemiBold,
            Foreground = new SolidColorBrush(Windows.UI.Color.FromArgb(0xFF, 0x11, 0x11, 0x11))
        };
        var subtitleBlock = new TextBlock
        {
            Text = subtitle,
            Foreground = new SolidColorBrush(Windows.UI.Color.FromArgb(0xFF, 0x6B, 0x72, 0x80))
        };

        var card = new Border
        {
            Tag = new CanvasNodeTag
            {
                NodeKey = nodeKey,
                ArtifactPath = artifactPath,
                ArtifactKind = artifactKind,
                WorkflowNodeType = workflowNodeType,
                WorkflowConfig = CloneJsonObject(workflowConfig),
                IsUserNode = isUserNode,
                IsArtifactNode = isArtifactNode,
                TitleBlock = titleBlock,
                SubtitleBlock = subtitleBlock
            },
            Width = 220,
            MinHeight = 96,
            BorderBrush = new SolidColorBrush(Windows.UI.Color.FromArgb(0x66, 0xC6, 0x28, 0x28)),
            BorderThickness = new Thickness(1),
            CornerRadius = new CornerRadius(14),
            Background = TryGetResourceBrush("CardAcrylicBrush") ?? new SolidColorBrush(Windows.UI.Color.FromArgb(0xCC, 0xFF, 0xFF, 0xFF)),
            Padding = new Thickness(12)
        };
        card.Shadow = new ThemeShadow();
        card.Translation = new Vector3(0, 0, 14);

        var inputConnector = new Ellipse
        {
            Width = 12,
            Height = 12,
            Fill = new SolidColorBrush(Windows.UI.Color.FromArgb(0xFF, 0x6B, 0x72, 0x80)),
            HorizontalAlignment = HorizontalAlignment.Left,
            VerticalAlignment = VerticalAlignment.Center,
            Margin = new Thickness(0, 0, 6, 0),
            Tag = new ConnectorTag
            {
                Node = card,
                Kind = "in"
            }
        };
        var outputConnector = new Ellipse
        {
            Width = 12,
            Height = 12,
            Fill = new SolidColorBrush(Windows.UI.Color.FromArgb(0xFF, 0xC6, 0x28, 0x28)),
            HorizontalAlignment = HorizontalAlignment.Right,
            VerticalAlignment = VerticalAlignment.Center,
            Margin = new Thickness(6, 0, 0, 0),
            Tag = new ConnectorTag
            {
                Node = card,
                Kind = "out"
            }
        };

        inputConnector.PointerPressed += OnConnectorPointerPressed;
        outputConnector.PointerPressed += OnConnectorPointerPressed;

        var contentStack = new StackPanel
        {
            Spacing = 4,
            Children =
            {
                titleBlock,
                subtitleBlock
            }
        };
        var nodeGrid = new Grid
        {
            Children =
            {
                inputConnector,
                contentStack,
                outputConnector
            }
        };
        nodeGrid.ColumnDefinitions.Add(new ColumnDefinition { Width = GridLength.Auto });
        nodeGrid.ColumnDefinitions.Add(new ColumnDefinition { Width = new GridLength(1, GridUnitType.Star) });
        nodeGrid.ColumnDefinitions.Add(new ColumnDefinition { Width = GridLength.Auto });
        Grid.SetColumn(contentStack, 1);
        Grid.SetColumn(outputConnector, 2);
        card.Child = nodeGrid;

        card.PointerPressed += OnCanvasNodePointerPressed;
        card.PointerMoved += OnCanvasNodePointerMoved;
        card.PointerReleased += OnCanvasNodePointerReleased;
        card.PointerCanceled += OnCanvasNodePointerReleased;
        card.RightTapped += OnCanvasNodeRightTapped;
        card.PointerEntered += (_, _) =>
        {
            if (_draggingNode == card)
            {
                return;
            }

            card.Translation = new Vector3(0, 0, 20);
        };
        card.PointerExited += (_, _) =>
        {
            if (_draggingNode == card)
            {
                return;
            }

            card.Translation = new Vector3(0, 0, 14);
        };

        Canvas.SetLeft(card, left);
        Canvas.SetTop(card, top);
        Canvas.SetZIndex(card, 10);
        return card;
    }

    private static void SetCanvasNodeSubtitle(Border? node, string subtitle)
    {
        if (node?.Tag is not CanvasNodeTag tag || tag.SubtitleBlock is null)
        {
            return;
        }

        tag.SubtitleBlock.Text = subtitle;
    }

    private void ClearCanvasArtifactNodes()
    {
        if (_artifactNodes.Count == 0)
        {
            return;
        }

        foreach (var node in _artifactNodes.ToList())
        {
            RemoveCanvasNode(node, refreshUi: false);
        }

        _artifactNodes.Clear();
        ApplyNodeSelectionVisuals();
        UpdateConnectionVisuals();
        UpdateNodePropertyPanel();
    }

    private void UpdateCanvasArtifactNodes(IReadOnlyList<RunArtifactItem> artifacts)
    {
        ClearCanvasArtifactNodes();
        if (artifacts.Count == 0)
        {
            SetCanvasNodeSubtitle(_outputNode, "无可用产物");
            return;
        }

        SetCanvasNodeSubtitle(_outputNode, $"已生成 {artifacts.Count} 个产物");
        SetCanvasNodeSubtitle(_cleanNode, "处理完成");

        foreach (var placement in ArtifactPresentationMapper.BuildCanvasPlacements(artifacts))
        {
            var node = AddCanvasNode(
                placement.NodeKey,
                placement.Title,
                placement.Subtitle,
                placement.Left,
                placement.Top,
                placement.ArtifactPath,
                placement.ArtifactKind,
                isArtifactNode: true);

            _artifactNodes.Add(node);
            if (_outputNode is not null)
            {
                AddConnection(_outputNode, node, select: false, requestAutosave: false);
            }
        }

        EnsureCanvasExtentForViewportAndNodes();
    }
}
