using System.Text;
using Microsoft.UI.Xaml;
using Microsoft.UI.Xaml.Automation;
using Microsoft.UI.Xaml.Controls;
using Microsoft.UI.Xaml.Media;

namespace AIWF.Native;

public sealed partial class MainWindow
{
    private Brush? TryGetResourceBrush(string key)
    {
        if (Content is FrameworkElement root
            && root.Resources.TryGetValue(key, out var value)
            && value is Brush brush)
        {
            return brush;
        }

        return null;
    }

    private void InitializeAddNodeFlyout()
    {
        _addNodeFlyoutGroupGrids.Clear();
        var root = new Border
        {
            Width = 560,
            MaxHeight = 720,
            Padding = new Thickness(10, 8, 10, 10),
            CornerRadius = new CornerRadius(14),
            BorderThickness = new Thickness(0),
            BorderBrush = new SolidColorBrush(Windows.UI.Color.FromArgb(0x00, 0x00, 0x00, 0x00)),
            Background = new SolidColorBrush(Windows.UI.Color.FromArgb(0xEE, 0xF3, 0xF4, 0xF6))
        };

        var layoutGrid = new Grid
        {
            RowSpacing = 8
        };
        layoutGrid.RowDefinitions.Add(new RowDefinition { Height = GridLength.Auto });
        layoutGrid.RowDefinitions.Add(new RowDefinition { Height = new GridLength(1, GridUnitType.Star) });

        var headerPanel = new StackPanel
        {
            Spacing = 2
        };
        headerPanel.Children.Add(new TextBlock
        {
            Text = "添加节点",
            FontSize = 21,
            FontWeight = Microsoft.UI.Text.FontWeights.SemiBold,
            Foreground = new SolidColorBrush(Windows.UI.Color.FromArgb(0xFF, 0x11, 0x11, 0x11))
        });
        headerPanel.Children.Add(new TextBlock
        {
            Text = "选择一个动作放到当前画布",
            FontSize = 12,
            Foreground = new SolidColorBrush(Windows.UI.Color.FromArgb(0xFF, 0x4B, 0x55, 0x63))
        });

        var stack = new StackPanel
        {
            Spacing = 4,
            HorizontalAlignment = HorizontalAlignment.Stretch
        };

        var groups = _quickNodeTemplates.GroupBy(static t => t.Group ?? string.Empty).ToList();
        for (var i = 0; i < groups.Count; i++)
        {
            var group = groups[i];
            if (i > 0)
            {
                stack.Children.Add(new Border
                {
                    Height = 1,
                    Margin = new Thickness(0, 6, 0, 6),
                    Background = new SolidColorBrush(Windows.UI.Color.FromArgb(0x22, 0x11, 0x11, 0x11))
                });
            }

            stack.Children.Add(new Border
            {
                Padding = new Thickness(2, 0, 0, 0),
                Child = new TextBlock
                {
                    Text = string.IsNullOrWhiteSpace(group.Key) ? "其他" : group.Key,
                    FontSize = 14,
                    FontWeight = Microsoft.UI.Text.FontWeights.SemiBold,
                    Foreground = new SolidColorBrush(Windows.UI.Color.FromArgb(0xFF, 0x6B, 0x72, 0x80))
                }
            });

            var grid = new Grid
            {
                ColumnSpacing = 2,
                RowSpacing = 4,
                HorizontalAlignment = HorizontalAlignment.Stretch
            };
            grid.ColumnDefinitions.Add(new ColumnDefinition { Width = new GridLength(1, GridUnitType.Star) });
            grid.ColumnDefinitions.Add(new ColumnDefinition { Width = new GridLength(1, GridUnitType.Star) });
            _addNodeFlyoutGroupGrids.Add(grid);

            var idx = 0;
            foreach (var template in group)
            {
                if (idx % 2 == 0)
                {
                    grid.RowDefinitions.Add(new RowDefinition { Height = GridLength.Auto });
                }

                var btn = CreateAddNodePanelButton(template);
                Grid.SetRow(btn, idx / 2);
                Grid.SetColumn(btn, idx % 2);
                grid.Children.Add(btn);
                idx++;
            }

            while ((idx % 2) != 0)
            {
                var spacer = new Border { Opacity = 0, Height = 68 };
                Grid.SetRow(spacer, idx / 2);
                Grid.SetColumn(spacer, idx % 2);
                grid.Children.Add(spacer);
                idx++;
            }
            stack.Children.Add(grid);
        }

        var scroller = new ScrollViewer
        {
            HorizontalScrollMode = ScrollMode.Disabled,
            HorizontalScrollBarVisibility = ScrollBarVisibility.Disabled,
            VerticalScrollMode = ScrollMode.Auto,
            VerticalScrollBarVisibility = ScrollBarVisibility.Auto,
            ZoomMode = ZoomMode.Disabled,
            IsHorizontalRailEnabled = false,
            HorizontalAlignment = HorizontalAlignment.Stretch,
            Content = stack
        };

        Grid.SetRow(headerPanel, 0);
        Grid.SetRow(scroller, 1);
        layoutGrid.Children.Add(headerPanel);
        layoutGrid.Children.Add(scroller);
        root.Child = layoutGrid;

        _addNodeFlyoutRoot = root;
        _addNodeFlyoutStack = stack;
        _addNodeFlyoutScroller = scroller;
        _addNodeFlyout.Content = root;
        _addNodeFlyout.Placement = Microsoft.UI.Xaml.Controls.Primitives.FlyoutPlacementMode.BottomEdgeAlignedLeft;
        _addNodeFlyout.Opened += (_, _) => ForceHideAddNodeFlyoutHorizontalBar();
        _addNodeFlyout.FlyoutPresenterStyle = new Style(typeof(FlyoutPresenter))
        {
            Setters =
            {
                new Setter(Control.BackgroundProperty, new SolidColorBrush(Windows.UI.Color.FromArgb(0x00, 0x00, 0x00, 0x00))),
                new Setter(Control.BorderBrushProperty, new SolidColorBrush(Windows.UI.Color.FromArgb(0x00, 0x00, 0x00, 0x00))),
                new Setter(Control.BorderThicknessProperty, new Thickness(0)),
                new Setter(Control.PaddingProperty, new Thickness(0))
            }
        };
    }

    private Button CreateAddNodePanelButton(NodeTemplate template)
    {
        var title = new TextBlock
        {
            Text = template.Title,
            FontSize = 16,
            FontWeight = Microsoft.UI.Text.FontWeights.SemiBold,
            Foreground = new SolidColorBrush(Windows.UI.Color.FromArgb(0xFF, 0x11, 0x11, 0x11)),
            TextWrapping = TextWrapping.WrapWholeWords,
            MaxLines = 1
        };
        var subtitle = new TextBlock
        {
            Text = template.Subtitle,
            FontSize = 13,
            Foreground = new SolidColorBrush(Windows.UI.Color.FromArgb(0xFF, 0x6B, 0x72, 0x80)),
            TextWrapping = TextWrapping.WrapWholeWords,
            MaxLines = 2
        };
        var content = new StackPanel { Spacing = 2 };
        content.Children.Add(title);
        content.Children.Add(subtitle);

        var btn = new Button
        {
            Content = content,
            Tag = template,
            HorizontalContentAlignment = HorizontalAlignment.Left,
            HorizontalAlignment = HorizontalAlignment.Stretch,
            Padding = new Thickness(12, 6, 12, 6),
            MinHeight = 72,
            Height = 72,
            CornerRadius = new CornerRadius(10),
            Background = new SolidColorBrush(Windows.UI.Color.FromArgb(0xCC, 0xFF, 0xFF, 0xFF)),
            BorderBrush = new SolidColorBrush(Windows.UI.Color.FromArgb(0x66, 0xD1, 0xD5, 0xDB))
        };
        AutomationProperties.SetAutomationId(btn, BuildAddNodeTemplateAutomationId(template.KeyPrefix));
        btn.Click += OnAddNodeTemplateClick;
        return btn;
    }

    private static string BuildAddNodeTemplateAutomationId(string? keyPrefix)
    {
        var raw = string.IsNullOrWhiteSpace(keyPrefix) ? "unknown" : keyPrefix.Trim();
        var builder = new StringBuilder(raw.Length);
        foreach (var ch in raw)
        {
            builder.Append(char.IsLetterOrDigit(ch) || ch is '-' or '_' ? ch : '-');
        }

        var suffix = builder.Length == 0 ? "unknown" : builder.ToString();
        return $"AddNodeTemplate-{suffix}";
    }

    private void ForceHideAddNodeFlyoutHorizontalBar()
    {
        if (_addNodeFlyoutScroller is null)
        {
            return;
        }

        CollapseHorizontalScrollBars(_addNodeFlyoutScroller);
    }

    private static void CollapseHorizontalScrollBars(DependencyObject root)
    {
        if (root is Microsoft.UI.Xaml.Controls.Primitives.ScrollBar bar
            && bar.Orientation == Orientation.Horizontal)
        {
            bar.Visibility = Visibility.Collapsed;
            bar.IsHitTestVisible = false;
            bar.Height = 0;
            return;
        }

        var count = VisualTreeHelper.GetChildrenCount(root);
        for (var i = 0; i < count; i++)
        {
            var child = VisualTreeHelper.GetChild(root, i);
            CollapseHorizontalScrollBars(child);
        }
    }

    private void ConfigureSystemBackdrop()
    {
        try
        {
            SystemBackdrop = new MicaBackdrop();
            return;
        }
        catch
        {
            // Fall through to Acrylic on devices where Mica is unavailable.
        }

        try
        {
            SystemBackdrop = new DesktopAcrylicBackdrop();
        }
        catch
        {
            // Keep default background if system backdrop is unavailable.
        }
    }
}
