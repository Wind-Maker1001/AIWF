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

    private void InitializeNodeLibraryDrawer()
    {
        RebuildNodeLibraryItems();
    }

    private void RebuildNodeLibraryItems(string? filter = null)
    {
        if (NodeLibraryItemsPanel is null)
        {
            return;
        }

        NodeLibraryItemsPanel.Children.Clear();

        var query = (filter ?? string.Empty).Trim();
        var groups = _quickNodeTemplates
            .Where(template =>
                string.IsNullOrWhiteSpace(query)
                || template.Title.Contains(query, StringComparison.OrdinalIgnoreCase)
                || template.Subtitle.Contains(query, StringComparison.OrdinalIgnoreCase)
                || (template.Group?.Contains(query, StringComparison.OrdinalIgnoreCase) ?? false))
            .GroupBy(static template => template.Group ?? string.Empty)
            .ToList();

            if (groups.Count == 0)
            {
                NodeLibraryItemsPanel.Children.Add(new TextBlock
                {
                    Text = "未找到匹配节点",
                    Foreground = new SolidColorBrush(Windows.UI.Color.FromArgb(0xFF, 0x7A, 0x82, 0x8D)),
                    Margin = new Thickness(2, 8, 0, 0)
                });
                return;
            }

        for (var i = 0; i < groups.Count; i++)
        {
            var group = groups[i];
            if (i > 0)
            {
                NodeLibraryItemsPanel.Children.Add(new Border
                {
                    Height = 1,
                    Margin = new Thickness(0, 8, 0, 2),
                    Background = new SolidColorBrush(Windows.UI.Color.FromArgb(0x22, 0x20, 0x24, 0x2B))
                });
            }

            NodeLibraryItemsPanel.Children.Add(new TextBlock
            {
                Text = string.IsNullOrWhiteSpace(group.Key) ? "其他" : group.Key,
                FontSize = 12,
                FontWeight = Microsoft.UI.Text.FontWeights.SemiBold,
                Foreground = new SolidColorBrush(Windows.UI.Color.FromArgb(0xFF, 0x52, 0x5A, 0x66)),
                CharacterSpacing = 30,
                Margin = new Thickness(2, 8, 0, 2)
            });

            foreach (var template in group)
            {
                NodeLibraryItemsPanel.Children.Add(CreateNodeLibraryItemButton(template));
            }
        }
    }

    private Button CreateNodeLibraryItemButton(NodeTemplate template)
    {
        var title = new TextBlock
        {
            Text = template.Title,
            FontSize = 15,
            FontWeight = Microsoft.UI.Text.FontWeights.SemiBold,
            Foreground = new SolidColorBrush(Windows.UI.Color.FromArgb(0xFF, 0x11, 0x13, 0x17)),
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
            Padding = new Thickness(10, 6, 10, 6),
            MinHeight = 50,
            Height = 50,
            CornerRadius = new CornerRadius(8),
            Background = new SolidColorBrush(Windows.UI.Color.FromArgb(0x00, 0xFF, 0xFF, 0xFF)),
            BorderBrush = new SolidColorBrush(Windows.UI.Color.FromArgb(0x00, 0x00, 0x00, 0x00)),
            BorderThickness = new Thickness(0)
        };
        btn.Resources["ButtonBackgroundPointerOver"] = new SolidColorBrush(Windows.UI.Color.FromArgb(0xFF, 0xF3, 0xF5, 0xF7));
        btn.Resources["ButtonBorderBrushPointerOver"] = new SolidColorBrush(Windows.UI.Color.FromArgb(0x00, 0x00, 0x00, 0x00));
        btn.Resources["ButtonBackgroundPressed"] = new SolidColorBrush(Windows.UI.Color.FromArgb(0xFF, 0xEA, 0xED, 0xF0));
        btn.Resources["ButtonBorderBrushPressed"] = new SolidColorBrush(Windows.UI.Color.FromArgb(0x00, 0x00, 0x00, 0x00));
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

    private void ConfigureSystemBackdrop()
    {
        try
        {
            SystemBackdrop = new MicaBackdrop();
            return;
        }
        catch
        {
        }

        try
        {
            SystemBackdrop = new DesktopAcrylicBackdrop();
        }
        catch
        {
        }
    }
}
