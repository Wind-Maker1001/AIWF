using Microsoft.UI;
using Microsoft.UI.Xaml;
using Microsoft.UI.Xaml.Controls;
using Microsoft.UI.Xaml.Media;
using Microsoft.UI.Xaml.Shapes;

namespace AIWF.Native.Runtime;

public static class SqlChartRenderer
{
    private static readonly Windows.UI.Color[] Palette =
    [
        Windows.UI.Color.FromArgb(255, 59, 130, 246),   // blue
        Windows.UI.Color.FromArgb(255, 239, 68, 68),    // red
        Windows.UI.Color.FromArgb(255, 34, 197, 94),    // green
        Windows.UI.Color.FromArgb(255, 249, 115, 22),   // orange
        Windows.UI.Color.FromArgb(255, 168, 85, 247),   // purple
        Windows.UI.Color.FromArgb(255, 20, 184, 166),   // teal
        Windows.UI.Color.FromArgb(255, 236, 72, 153),   // pink
        Windows.UI.Color.FromArgb(255, 245, 158, 11),   // amber
    ];

    public static void Render(Canvas canvas, SqlChartData data, double width, double height)
    {
        canvas.Children.Clear();
        canvas.Width = width;
        canvas.Height = height;

        if (data.Categories.Count == 0 || data.Series.Count == 0)
        {
            var empty = new TextBlock
            {
                Text = "No chart data.",
                Foreground = new SolidColorBrush(Windows.UI.Color.FromArgb(255, 107, 114, 128)),
                FontSize = 14,
            };
            Canvas.SetLeft(empty, width / 2 - 50);
            Canvas.SetTop(empty, height / 2 - 10);
            canvas.Children.Add(empty);
            return;
        }

        switch (data.ChartType.ToLowerInvariant())
        {
            case "pie":
                RenderPie(canvas, data, width, height);
                break;
            case "line":
                RenderLine(canvas, data, width, height);
                break;
            default:
                RenderBar(canvas, data, width, height);
                break;
        }
    }

    private static void RenderBar(Canvas canvas, SqlChartData data, double width, double height)
    {
        const double marginLeft = 60;
        const double marginRight = 20;
        const double marginTop = 30;
        const double marginBottom = 60;
        const double barGap = 4;

        var chartW = width - marginLeft - marginRight;
        var chartH = height - marginTop - marginBottom;
        var maxVal = data.MaxValue;
        if (maxVal <= 0) maxVal = 1;
        var catCount = data.Categories.Count;
        var seriesCount = data.Series.Count;
        var groupWidth = chartW / catCount;
        var barWidth = Math.Max(4, (groupWidth - barGap * (seriesCount + 1)) / seriesCount);

        // Y-axis line
        canvas.Children.Add(MakeLine(marginLeft, marginTop, marginLeft, marginTop + chartH,
            Windows.UI.Color.FromArgb(255, 209, 213, 219)));
        // X-axis line
        canvas.Children.Add(MakeLine(marginLeft, marginTop + chartH, marginLeft + chartW, marginTop + chartH,
            Windows.UI.Color.FromArgb(255, 209, 213, 219)));

        // Y-axis labels (5 ticks)
        for (var i = 0; i <= 4; i++)
        {
            var yVal = maxVal * (4 - i) / 4;
            var yPos = marginTop + chartH * i / 4;
            var label = new TextBlock
            {
                Text = FormatNumber(yVal),
                FontSize = 10,
                Foreground = new SolidColorBrush(Windows.UI.Color.FromArgb(255, 107, 114, 128)),
            };
            Canvas.SetLeft(label, 4);
            Canvas.SetTop(label, yPos - 7);
            canvas.Children.Add(label);

            if (i > 0 && i < 4)
            {
                canvas.Children.Add(MakeLine(marginLeft, yPos, marginLeft + chartW, yPos,
                    Windows.UI.Color.FromArgb(255, 243, 244, 246)));
            }
        }

        // Bars
        for (var catIdx = 0; catIdx < catCount; catIdx++)
        {
            var groupX = marginLeft + catIdx * groupWidth;

            // Category label
            var catLabel = new TextBlock
            {
                Text = TruncateLabel(data.Categories[catIdx], 10),
                FontSize = 10,
                Foreground = new SolidColorBrush(Windows.UI.Color.FromArgb(255, 55, 65, 81)),
                TextAlignment = TextAlignment.Center,
                Width = groupWidth,
            };
            Canvas.SetLeft(catLabel, groupX);
            Canvas.SetTop(catLabel, marginTop + chartH + 6);
            canvas.Children.Add(catLabel);

            for (var sIdx = 0; sIdx < seriesCount; sIdx++)
            {
                var val = sIdx < data.Series[sIdx].Data.Count && catIdx < data.Series[sIdx].Data.Count
                    ? data.Series[sIdx].Data[catIdx]
                    : 0;
                var barH = Math.Max(1, val / maxVal * chartH);
                var barX = groupX + barGap + sIdx * (barWidth + barGap);
                var barY = marginTop + chartH - barH;

                var rect = new Rectangle
                {
                    Width = barWidth,
                    Height = barH,
                    Fill = new SolidColorBrush(Palette[sIdx % Palette.Length]),
                    RadiusX = 2,
                    RadiusY = 2,
                };
                Canvas.SetLeft(rect, barX);
                Canvas.SetTop(rect, barY);
                canvas.Children.Add(rect);
            }
        }

        RenderLegend(canvas, data, marginLeft, 4);
    }

    private static void RenderLine(Canvas canvas, SqlChartData data, double width, double height)
    {
        const double marginLeft = 60;
        const double marginRight = 20;
        const double marginTop = 30;
        const double marginBottom = 60;

        var chartW = width - marginLeft - marginRight;
        var chartH = height - marginTop - marginBottom;
        var maxVal = data.MaxValue;
        if (maxVal <= 0) maxVal = 1;
        var catCount = data.Categories.Count;

        // Axes
        canvas.Children.Add(MakeLine(marginLeft, marginTop, marginLeft, marginTop + chartH,
            Windows.UI.Color.FromArgb(255, 209, 213, 219)));
        canvas.Children.Add(MakeLine(marginLeft, marginTop + chartH, marginLeft + chartW, marginTop + chartH,
            Windows.UI.Color.FromArgb(255, 209, 213, 219)));

        // Y-axis labels
        for (var i = 0; i <= 4; i++)
        {
            var yVal = maxVal * (4 - i) / 4;
            var yPos = marginTop + chartH * i / 4;
            var label = new TextBlock
            {
                Text = FormatNumber(yVal),
                FontSize = 10,
                Foreground = new SolidColorBrush(Windows.UI.Color.FromArgb(255, 107, 114, 128)),
            };
            Canvas.SetLeft(label, 4);
            Canvas.SetTop(label, yPos - 7);
            canvas.Children.Add(label);
        }

        // X-axis labels
        var stepW = catCount > 1 ? chartW / (catCount - 1) : chartW;
        for (var i = 0; i < catCount; i++)
        {
            var xPos = marginLeft + (catCount > 1 ? i * stepW : stepW / 2);
            var label = new TextBlock
            {
                Text = TruncateLabel(data.Categories[i], 8),
                FontSize = 10,
                Foreground = new SolidColorBrush(Windows.UI.Color.FromArgb(255, 55, 65, 81)),
            };
            Canvas.SetLeft(label, xPos - 15);
            Canvas.SetTop(label, marginTop + chartH + 6);
            canvas.Children.Add(label);
        }

        // Lines + dots
        for (var sIdx = 0; sIdx < data.Series.Count; sIdx++)
        {
            var color = Palette[sIdx % Palette.Length];
            var series = data.Series[sIdx];
            for (var i = 0; i < catCount && i < series.Data.Count; i++)
            {
                var x = marginLeft + (catCount > 1 ? i * stepW : stepW / 2);
                var y = marginTop + chartH - series.Data[i] / maxVal * chartH;

                // Dot
                var dot = new Ellipse
                {
                    Width = 6,
                    Height = 6,
                    Fill = new SolidColorBrush(color),
                };
                Canvas.SetLeft(dot, x - 3);
                Canvas.SetTop(dot, y - 3);
                canvas.Children.Add(dot);

                // Line to previous
                if (i > 0 && i - 1 < series.Data.Count)
                {
                    var prevX = marginLeft + (i - 1) * stepW;
                    var prevY = marginTop + chartH - series.Data[i - 1] / maxVal * chartH;
                    canvas.Children.Add(MakeLine(prevX, prevY, x, y, color, 2));
                }
            }
        }

        RenderLegend(canvas, data, marginLeft, 4);
    }

    private static void RenderPie(Canvas canvas, SqlChartData data, double width, double height)
    {
        const double marginTop = 30;
        var radius = Math.Min(width, height - marginTop - 40) / 2 - 20;
        var centerX = width / 2;
        var centerY = marginTop + radius + 10;

        if (data.Series.Count == 0 || data.Series[0].Data.Count == 0)
        {
            return;
        }

        var values = data.Series[0].Data;
        var total = values.Sum();
        if (total <= 0) total = 1;

        var startAngle = 0.0;
        var sliceCount = Math.Min(values.Count, data.Categories.Count);
        for (var i = 0; i < sliceCount; i++)
        {
            var sweepAngle = values[i] / total * 360;
            if (sweepAngle < 0.5) { startAngle += sweepAngle; continue; }

            var color = Palette[i % Palette.Length];
            var midAngle = (startAngle + sweepAngle / 2) * Math.PI / 180;

            // Simplified: draw a filled sector using a Path approximation with a thick arc line
            // For WinUI Canvas without Path, approximate with a filled polygon of wedge lines
            var segments = Math.Max(2, (int)(sweepAngle / 5));
            for (var s = 0; s < segments; s++)
            {
                var a1 = (startAngle + sweepAngle * s / segments) * Math.PI / 180;
                var a2 = (startAngle + sweepAngle * (s + 1) / segments) * Math.PI / 180;
                var x1 = centerX + radius * Math.Cos(a1);
                var y1 = centerY + radius * Math.Sin(a1);
                var x2 = centerX + radius * Math.Cos(a2);
                var y2 = centerY + radius * Math.Sin(a2);

                var triangle = new Polygon
                {
                    Points =
                    {
                        new Windows.Foundation.Point(centerX, centerY),
                        new Windows.Foundation.Point(x1, y1),
                        new Windows.Foundation.Point(x2, y2),
                    },
                    Fill = new SolidColorBrush(color),
                    Stroke = new SolidColorBrush(Colors.White),
                    StrokeThickness = 0.5,
                };
                canvas.Children.Add(triangle);
            }

            // Label
            var labelX = centerX + (radius * 0.65) * Math.Cos(midAngle);
            var labelY = centerY + (radius * 0.65) * Math.Sin(midAngle);
            var pct = values[i] / total * 100;
            if (pct >= 3)
            {
                var label = new TextBlock
                {
                    Text = $"{pct:F0}%",
                    FontSize = 10,
                    Foreground = new SolidColorBrush(Colors.White),
                    FontWeight = Microsoft.UI.Text.FontWeights.Bold,
                };
                Canvas.SetLeft(label, labelX - 12);
                Canvas.SetTop(label, labelY - 7);
                canvas.Children.Add(label);
            }

            startAngle += sweepAngle;
        }

        // Legend below pie
        var legendY = centerY + radius + 16;
        for (var i = 0; i < sliceCount && i < 8; i++)
        {
            var color = Palette[i % Palette.Length];
            var legendX = 20 + i * (width / Math.Min(sliceCount, 8));
            var swatch = new Rectangle
            {
                Width = 10,
                Height = 10,
                Fill = new SolidColorBrush(color),
                RadiusX = 2,
                RadiusY = 2,
            };
            Canvas.SetLeft(swatch, legendX);
            Canvas.SetTop(swatch, legendY);
            canvas.Children.Add(swatch);

            var label = new TextBlock
            {
                Text = TruncateLabel(data.Categories[i], 12),
                FontSize = 10,
                Foreground = new SolidColorBrush(Windows.UI.Color.FromArgb(255, 55, 65, 81)),
            };
            Canvas.SetLeft(label, legendX + 14);
            Canvas.SetTop(label, legendY - 1);
            canvas.Children.Add(label);
        }
    }

    private static void RenderLegend(Canvas canvas, SqlChartData data, double x, double y)
    {
        if (data.Series.Count <= 1)
        {
            return;
        }

        for (var i = 0; i < data.Series.Count && i < Palette.Length; i++)
        {
            var legendX = x + i * 120;
            var swatch = new Rectangle
            {
                Width = 10,
                Height = 10,
                Fill = new SolidColorBrush(Palette[i % Palette.Length]),
                RadiusX = 2,
                RadiusY = 2,
            };
            Canvas.SetLeft(swatch, legendX);
            Canvas.SetTop(swatch, y);
            canvas.Children.Add(swatch);

            var label = new TextBlock
            {
                Text = TruncateLabel(data.Series[i].Name, 15),
                FontSize = 10,
                Foreground = new SolidColorBrush(Windows.UI.Color.FromArgb(255, 55, 65, 81)),
            };
            Canvas.SetLeft(label, legendX + 14);
            Canvas.SetTop(label, y - 1);
            canvas.Children.Add(label);
        }
    }

    private static Line MakeLine(double x1, double y1, double x2, double y2, Windows.UI.Color color, double thickness = 1)
    {
        return new Line
        {
            X1 = x1, Y1 = y1, X2 = x2, Y2 = y2,
            Stroke = new SolidColorBrush(color),
            StrokeThickness = thickness,
        };
    }

    private static string FormatNumber(double value)
    {
        return value >= 1_000_000 ? $"{value / 1_000_000:F1}M"
            : value >= 1_000 ? $"{value / 1_000:F1}K"
            : value == Math.Floor(value) ? $"{value:F0}"
            : $"{value:F1}";
    }

    private static string TruncateLabel(string value, int maxLen)
    {
        return value.Length <= maxLen ? value : value[..(maxLen - 1)] + "…";
    }
}
