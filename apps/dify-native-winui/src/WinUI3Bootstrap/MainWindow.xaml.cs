using Microsoft.UI.Xaml;
using Microsoft.UI.Xaml.Controls;
using Microsoft.UI.Xaml.Media;
using Microsoft.UI.Xaml.Media.Animation;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace AIWF.Native;

public sealed partial class MainWindow : Window
{
    private enum NavSection
    {
        Workspace,
        Results
    }
    private enum InlineStatusTone
    {
        Neutral,
        Busy,
        Success,
        Error
    }

    private readonly HttpClient _http = new();
    private NavSection _activeSection = NavSection.Workspace;
    private bool _didPlayIntroAnimation;

    public MainWindow()
    {
        InitializeComponent();
        ConfigureSystemBackdrop();
        SetActiveSection(_activeSection);
        ApplyCommandButtonState();
        Activated += OnWindowActivated;
        SizeChanged += OnWindowSizeChanged;
        ApplyResponsiveLayout();
        try
        {
            AppWindow.Resize(new Windows.Graphics.SizeInt32(1180, 760));
        }
        catch
        {
            // Keep startup resilient if window sizing APIs are unavailable.
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

    private async void OnHealthClick(object sender, RoutedEventArgs e)
    {
        await SetBusyAsync(true, "正在检查桥接服务健康状态...", InlineStatusTone.Busy);
        try
        {
            using var request = CreateRequest(HttpMethod.Get, "/health");
            using var response = await _http.SendAsync(request);
            var text = await response.Content.ReadAsStringAsync();
            RawResponseTextBox.Text = PrettyJson(text);
            SetInlineStatus(
                response.IsSuccessStatusCode
                    ? "桥接服务健康检查通过。"
                    : $"桥接服务健康检查失败：{(int)response.StatusCode}",
                response.IsSuccessStatusCode ? InlineStatusTone.Success : InlineStatusTone.Error);
        }
        catch (Exception ex)
        {
            SetInlineStatus($"健康检查异常：{ex.Message}", InlineStatusTone.Error);
        }
        finally
        {
            await SetBusyAsync(false, StatusTextBlock.Text, InferToneFromStatus());
        }
    }

    private async void OnRunCleaningClick(object sender, RoutedEventArgs e)
    {
        if (!ValidateRunInputs(out var validationMessage))
        {
            SetInlineStatus(validationMessage, InlineStatusTone.Error);
            return;
        }

        await SetBusyAsync(true, "正在提交运行清洗请求...", InlineStatusTone.Busy);
        try
        {
            var payload = BuildRunCleaningPayload();
            using var request = CreateRequest(HttpMethod.Post, "/run-cleaning");
            request.Content = new StringContent(payload.ToJsonString(), Encoding.UTF8, "application/json");

            using var response = await _http.SendAsync(request);
            var text = await response.Content.ReadAsStringAsync();
            RawResponseTextBox.Text = PrettyJson(text);

            if (!response.IsSuccessStatusCode)
            {
                SetInlineStatus($"运行失败：{(int)response.StatusCode}", InlineStatusTone.Error);
                return;
            }

            BindRunResult(text);
            SetInlineStatus("运行清洗请求已完成。", InlineStatusTone.Success);
            SetActiveSection(NavSection.Results);
        }
        catch (Exception ex)
        {
            SetInlineStatus($"运行请求异常：{ex.Message}", InlineStatusTone.Error);
        }
        finally
        {
            await SetBusyAsync(false, StatusTextBlock.Text, InferToneFromStatus());
        }
    }

    private JsonObject BuildRunCleaningPayload()
    {
        var paramsObj = new JsonObject
        {
            ["office_theme"] = ReadComboValue(OfficeThemeComboBox),
            ["office_lang"] = ReadComboValue(OfficeLangComboBox),
            ["report_title"] = ReportTitleTextBox.Text.Trim()
        };

        var inputCsvPath = InputCsvTextBox.Text.Trim();
        if (!string.IsNullOrWhiteSpace(inputCsvPath))
        {
            paramsObj["input_csv_path"] = inputCsvPath;
        }

        return new JsonObject
        {
            ["owner"] = OwnerTextBox.Text.Trim(),
            ["actor"] = ActorTextBox.Text.Trim(),
            ["ruleset_version"] = "v1",
            ["params"] = paramsObj
        };
    }

    private HttpRequestMessage CreateRequest(HttpMethod method, string endpointPath)
    {
        ResetValidationVisuals();
        var baseUrl = BridgeUrlTextBox.Text.Trim().TrimEnd('/');
        if (string.IsNullOrWhiteSpace(baseUrl))
        {
            SetInputError(BridgeUrlTextBox, true);
            throw new InvalidOperationException("桥接地址不能为空。");
        }

        var request = new HttpRequestMessage(method, $"{baseUrl}{endpointPath}");
        var apiKey = ApiKeyTextBox.Text.Trim();
        if (!string.IsNullOrWhiteSpace(apiKey))
        {
            request.Headers.Add("X-API-Key", apiKey);
        }
        request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
        return request;
    }

    private void BindRunResult(string json)
    {
        ArtifactsListView.Items.Clear();
        ArtifactsCountTextBlock.Text = "0 项";
        JobIdTextBlock.Text = "-";
        RunResultTextBlock.Text = "-";
        RunModeTextBlock.Text = "-";
        DurationTextBlock.Text = "-";
        OkMetricTextBlock.Text = "-";
        ModeMetricTextBlock.Text = "-";
        DurationMetricTextBlock.Text = "-";
        ApplyMetricVisuals(null, "-", null);
        ApplyRunStatusBadge(null);

        JsonNode? root;
        try
        {
            root = JsonNode.Parse(json);
        }
        catch
        {
            return;
        }

        if (root is null)
        {
            return;
        }

        var ok = root["ok"]?.GetValue<bool?>();
        var jobId = root["job_id"]?.GetValue<string?>() ?? "-";
        RunResultTextBlock.Text = ok == true ? "执行成功。" : ok == false ? "执行失败。" : "状态未知。";
        JobIdTextBlock.Text = jobId;
        OkMetricTextBlock.Text = ok?.ToString() ?? "-";
        ApplyRunStatusBadge(ok);

        var data = root["data"];
        var mode = data?["mode"]?.GetValue<string?>() ?? root["run_mode"]?.GetValue<string?>() ?? "-";
        var duration = data?["duration_ms"]?.GetValue<int?>() ?? root["duration_ms"]?.GetValue<int?>();
        RunModeTextBlock.Text = mode;
        DurationTextBlock.Text = duration?.ToString() ?? "-";
        ModeMetricTextBlock.Text = mode;
        DurationMetricTextBlock.Text = duration is null ? "-" : $"{duration} ms";
        ApplyMetricVisuals(ok, mode, duration);

        var artifacts = root["artifacts"] as JsonArray ?? data?["artifacts"] as JsonArray;
        if (artifacts is null)
        {
            return;
        }

        foreach (var artifact in artifacts)
        {
            if (artifact is null)
            {
                continue;
            }

            var id = artifact["artifact_id"]?.GetValue<string?>() ?? "-";
            var kind = artifact["kind"]?.GetValue<string?>() ?? "-";
            var path = artifact["path"]?.GetValue<string?>() ?? "-";
            ArtifactsListView.Items.Add($"{id} | {kind} | {path}");
        }

        ArtifactsCountTextBlock.Text = $"{ArtifactsListView.Items.Count} 项";
    }

    private static string ReadComboValue(ComboBox comboBox)
    {
        if (comboBox.SelectedItem is ComboBoxItem item && item.Content is string value)
        {
            return value;
        }

        return comboBox.SelectedValue?.ToString() ?? "zh";
    }

    private static string PrettyJson(string text)
    {
        try
        {
            using var doc = JsonDocument.Parse(text);
            return JsonSerializer.Serialize(doc.RootElement, new JsonSerializerOptions
            {
                WriteIndented = true
            });
        }
        catch
        {
            return text;
        }
    }

    private Task SetBusyAsync(bool busy, string message, InlineStatusTone tone)
    {
        HealthButton.IsEnabled = !busy;
        RunCleaningButton.IsEnabled = !busy;
        QuickHealthButton.IsEnabled = !busy;
        QuickRunButton.IsEnabled = !busy;
        SetInlineStatus(message, tone);
        return Task.CompletedTask;
    }

    private void OnWorkspaceNavClick(object sender, RoutedEventArgs e)
    {
        SetActiveSection(NavSection.Workspace);
    }

    private void OnResultNavClick(object sender, RoutedEventArgs e)
    {
        SetActiveSection(NavSection.Results);
    }

    private void SetActiveSection(NavSection section)
    {
        _activeSection = section;
        WorkspaceSectionGrid.Visibility = section == NavSection.Workspace
            ? Visibility.Visible
            : Visibility.Collapsed;
        ResultsSectionGrid.Visibility = section == NavSection.Results
            ? Visibility.Visible
            : Visibility.Collapsed;

        ApplyNavButtonState(WorkspaceNavButton, section == NavSection.Workspace);
        ApplyNavButtonState(ResultNavButton, section == NavSection.Results);
        PlaySectionEntrance(section == NavSection.Workspace ? WorkspaceSectionGrid : ResultsSectionGrid);
    }

    private static void ApplyNavButtonState(Button button, bool active)
    {
        button.FontWeight = active ? Microsoft.UI.Text.FontWeights.SemiBold : Microsoft.UI.Text.FontWeights.Normal;
        button.Background = new SolidColorBrush(active ? Windows.UI.Color.FromArgb(0xFF, 0xC6, 0x28, 0x28) : Windows.UI.Color.FromArgb(0x00, 0x00, 0x00, 0x00));
        button.Foreground = new SolidColorBrush(active ? Windows.UI.Color.FromArgb(0xFF, 0xFF, 0xFF, 0xFF) : Windows.UI.Color.FromArgb(0xFF, 0x11, 0x11, 0x11));
        button.BorderBrush = new SolidColorBrush(active ? Windows.UI.Color.FromArgb(0xFF, 0xC6, 0x28, 0x28) : Windows.UI.Color.FromArgb(0x33, 0x22, 0x22, 0x22));
        button.BorderThickness = new Thickness(1);
        button.CornerRadius = new CornerRadius(8);
    }

    private void ApplyCommandButtonState()
    {
        QuickRunButton.FontWeight = Microsoft.UI.Text.FontWeights.SemiBold;
        QuickRunButton.Background = new SolidColorBrush(Windows.UI.Color.FromArgb(0xFF, 0xC6, 0x28, 0x28));
        QuickRunButton.Foreground = new SolidColorBrush(Windows.UI.Color.FromArgb(0xFF, 0xFF, 0xFF, 0xFF));
        QuickRunButton.BorderBrush = new SolidColorBrush(Windows.UI.Color.FromArgb(0xFF, 0xC6, 0x28, 0x28));
        QuickRunButton.BorderThickness = new Thickness(1);
        QuickRunButton.CornerRadius = new CornerRadius(8);

        QuickHealthButton.FontWeight = Microsoft.UI.Text.FontWeights.Normal;
        QuickHealthButton.Background = new SolidColorBrush(Windows.UI.Color.FromArgb(0x00, 0x00, 0x00, 0x00));
        QuickHealthButton.Foreground = new SolidColorBrush(Windows.UI.Color.FromArgb(0xFF, 0x11, 0x11, 0x11));
        QuickHealthButton.BorderBrush = new SolidColorBrush(Windows.UI.Color.FromArgb(0x33, 0x22, 0x22, 0x22));
        QuickHealthButton.BorderThickness = new Thickness(1);
        QuickHealthButton.CornerRadius = new CornerRadius(8);
    }

    private void ResetValidationVisuals()
    {
        SetInputError(BridgeUrlTextBox, false);
        SetInputError(OwnerTextBox, false);
        SetInputError(ActorTextBox, false);
        SetInputError(ReportTitleTextBox, false);
    }

    private static void SetInputError(Control control, bool hasError)
    {
        control.BorderBrush = new SolidColorBrush(
            hasError
                ? Windows.UI.Color.FromArgb(0xFF, 0xC6, 0x28, 0x28)
                : Windows.UI.Color.FromArgb(0x66, 0x55, 0x55, 0x55));
    }

    private bool ValidateRunInputs(out string message)
    {
        ResetValidationVisuals();
        var valid = true;
        var missing = new List<string>();

        if (string.IsNullOrWhiteSpace(BridgeUrlTextBox.Text))
        {
            SetInputError(BridgeUrlTextBox, true);
            valid = false;
            missing.Add("桥接地址");
        }

        if (string.IsNullOrWhiteSpace(OwnerTextBox.Text))
        {
            SetInputError(OwnerTextBox, true);
            valid = false;
            missing.Add("所有者");
        }

        if (string.IsNullOrWhiteSpace(ActorTextBox.Text))
        {
            SetInputError(ActorTextBox, true);
            valid = false;
            missing.Add("执行者");
        }

        if (string.IsNullOrWhiteSpace(ReportTitleTextBox.Text))
        {
            SetInputError(ReportTitleTextBox, true);
            valid = false;
            missing.Add("报告标题");
        }

        message = valid
            ? "校验通过。"
            : $"请填写必填项：{string.Join("、", missing)}。";
        return valid;
    }

    private void ApplyMetricVisuals(bool? ok, string mode, int? durationMs)
    {
        OkMetricTextBlock.Foreground = new SolidColorBrush(
            ok switch
            {
                true => Windows.UI.Color.FromArgb(0xFF, 0x11, 0x11, 0x11),
                false => Windows.UI.Color.FromArgb(0xFF, 0xC6, 0x28, 0x28),
                _ => Windows.UI.Color.FromArgb(0xFF, 0x6B, 0x72, 0x80)
            });

        ModeMetricTextBlock.Foreground = new SolidColorBrush(
            string.IsNullOrWhiteSpace(mode) || mode == "-"
                ? Windows.UI.Color.FromArgb(0xFF, 0x6B, 0x72, 0x80)
                : Windows.UI.Color.FromArgb(0xFF, 0x11, 0x11, 0x11));

        DurationMetricTextBlock.Foreground = new SolidColorBrush(
            durationMs switch
            {
                null => Windows.UI.Color.FromArgb(0xFF, 0x6B, 0x72, 0x80),
                <= 1500 => Windows.UI.Color.FromArgb(0xFF, 0x11, 0x11, 0x11),
                <= 4000 => Windows.UI.Color.FromArgb(0xFF, 0x6B, 0x72, 0x80),
                _ => Windows.UI.Color.FromArgb(0xFF, 0xC6, 0x28, 0x28)
            });
    }

    private void ApplyRunStatusBadge(bool? ok)
    {
        if (ok == true)
        {
            RunStatusBadgeText.Text = "成功";
            RunStatusBadgeText.Foreground = new SolidColorBrush(Windows.UI.Color.FromArgb(0xFF, 0x11, 0x11, 0x11));
            RunStatusBadgeBorder.BorderBrush = new SolidColorBrush(Windows.UI.Color.FromArgb(0x66, 0x11, 0x11, 0x11));
            RunStatusBadgeBorder.Background = new SolidColorBrush(Windows.UI.Color.FromArgb(0x33, 0xE5, 0xE7, 0xEB));
            return;
        }

        if (ok == false)
        {
            RunStatusBadgeText.Text = "失败";
            RunStatusBadgeText.Foreground = new SolidColorBrush(Windows.UI.Color.FromArgb(0xFF, 0xC6, 0x28, 0x28));
            RunStatusBadgeBorder.BorderBrush = new SolidColorBrush(Windows.UI.Color.FromArgb(0x66, 0xC6, 0x28, 0x28));
            RunStatusBadgeBorder.Background = new SolidColorBrush(Windows.UI.Color.FromArgb(0x33, 0xFE, 0xE2, 0xE2));
            return;
        }

        RunStatusBadgeText.Text = "待运行";
        RunStatusBadgeText.Foreground = new SolidColorBrush(Windows.UI.Color.FromArgb(0xFF, 0x6B, 0x72, 0x80));
        RunStatusBadgeBorder.BorderBrush = new SolidColorBrush(Windows.UI.Color.FromArgb(0x66, 0x6B, 0x72, 0x80));
        RunStatusBadgeBorder.Background = new SolidColorBrush(Windows.UI.Color.FromArgb(0x33, 0xE5, 0xE7, 0xEB));
    }

    private void SetInlineStatus(string message, InlineStatusTone tone)
    {
        StatusTextBlock.Text = message;
        StatusTextBlock.Foreground = new SolidColorBrush(
            tone switch
            {
                InlineStatusTone.Success => Windows.UI.Color.FromArgb(0xFF, 0x11, 0x11, 0x11),
                InlineStatusTone.Error => Windows.UI.Color.FromArgb(0xFF, 0xC6, 0x28, 0x28),
                InlineStatusTone.Busy => Windows.UI.Color.FromArgb(0xFF, 0x6B, 0x72, 0x80),
                _ => Windows.UI.Color.FromArgb(0xFF, 0x4B, 0x55, 0x63)
            });
    }

    private InlineStatusTone InferToneFromStatus()
    {
        var text = StatusTextBlock.Text ?? string.Empty;
        if (text.Contains("失败") || text.Contains("异常") || text.Contains("请填写"))
        {
            return InlineStatusTone.Error;
        }

        if (text.Contains("完成") || text.Contains("通过"))
        {
            return InlineStatusTone.Success;
        }

        if (text.Contains("正在"))
        {
            return InlineStatusTone.Busy;
        }

        return InlineStatusTone.Neutral;
    }
    private void OnWindowActivated(object sender, WindowActivatedEventArgs args)
    {
        if (_didPlayIntroAnimation)
        {
            return;
        }

        _didPlayIntroAnimation = true;
        PlayFadeIn(HeroHeaderBorder, 0, 220);
        PlayFadeIn(NavShellBorder, 90, 220);
        PlayFadeIn(ContentHostGrid, 180, 260);
        PlaySectionEntrance(_activeSection == NavSection.Workspace ? WorkspaceSectionGrid : ResultsSectionGrid);
    }

    private static void PlaySectionEntrance(UIElement element)
    {
        PlayFadeIn(element, 0, 180);
    }

    private static void PlayFadeIn(UIElement element, int delayMs, int durationMs)
    {
        element.Opacity = 0;
        var storyboard = new Storyboard();
        var fade = new DoubleAnimation
        {
            From = 0,
            To = 1,
            Duration = TimeSpan.FromMilliseconds(durationMs),
            BeginTime = TimeSpan.FromMilliseconds(delayMs),
            EnableDependentAnimation = true
        };
        Storyboard.SetTarget(fade, element);
        Storyboard.SetTargetProperty(fade, "Opacity");
        storyboard.Children.Add(fade);
        storyboard.Begin();
    }

    private void OnWindowSizeChanged(object sender, WindowSizeChangedEventArgs args)
    {
        ApplyResponsiveLayout();
    }

    private void ApplyResponsiveLayout()
    {
        var width = (Content as FrameworkElement)?.ActualWidth ?? 1180;

        if (width < 980)
        {
            // Workspace: stack cards vertically.
            WorkspaceCol0.Width = new GridLength(1, GridUnitType.Star);
            WorkspaceCol1.Width = new GridLength(0);
            WorkspaceCol2.Width = new GridLength(0);

            Grid.SetColumn(ConnectionCard, 0);
            Grid.SetRow(ConnectionCard, 0);
            Grid.SetColumnSpan(ConnectionCard, 1);

            Grid.SetColumn(ParamsCard, 0);
            Grid.SetRow(ParamsCard, 1);
            Grid.SetColumnSpan(ParamsCard, 1);

            Grid.SetColumn(ActionsCard, 0);
            Grid.SetRow(ActionsCard, 2);
            Grid.SetColumnSpan(ActionsCard, 1);

            // Results: stack vertically.
            ResultsCol0.Width = new GridLength(1, GridUnitType.Star);
            ResultsCol1.Width = new GridLength(0);

            Grid.SetColumn(ArtifactsCard, 0);
            Grid.SetRow(ArtifactsCard, 0);
            Grid.SetColumnSpan(ArtifactsCard, 1);

            Grid.SetColumn(RunResultCard, 0);
            Grid.SetRow(RunResultCard, 1);
            Grid.SetColumnSpan(RunResultCard, 1);
            return;
        }

        if (width < 1280)
        {
            // Workspace: two-column with actions full width at bottom.
            WorkspaceCol0.Width = new GridLength(1, GridUnitType.Star);
            WorkspaceCol1.Width = new GridLength(1, GridUnitType.Star);
            WorkspaceCol2.Width = new GridLength(0);

            Grid.SetColumn(ConnectionCard, 0);
            Grid.SetRow(ConnectionCard, 0);
            Grid.SetColumnSpan(ConnectionCard, 1);

            Grid.SetColumn(ParamsCard, 1);
            Grid.SetRow(ParamsCard, 0);
            Grid.SetColumnSpan(ParamsCard, 1);

            Grid.SetColumn(ActionsCard, 0);
            Grid.SetRow(ActionsCard, 1);
            Grid.SetColumnSpan(ActionsCard, 2);

            // Results: keep two-column layout.
            ResultsCol0.Width = new GridLength(2, GridUnitType.Star);
            ResultsCol1.Width = new GridLength(1, GridUnitType.Star);
            Grid.SetColumn(ArtifactsCard, 0);
            Grid.SetRow(ArtifactsCard, 0);
            Grid.SetColumnSpan(ArtifactsCard, 1);
            Grid.SetColumn(RunResultCard, 1);
            Grid.SetRow(RunResultCard, 0);
            Grid.SetColumnSpan(RunResultCard, 1);
            return;
        }

        // Wide: restore original desktop layout.
        WorkspaceCol0.Width = new GridLength(2, GridUnitType.Star);
        WorkspaceCol1.Width = new GridLength(2, GridUnitType.Star);
        WorkspaceCol2.Width = new GridLength(1, GridUnitType.Star);

        Grid.SetColumn(ConnectionCard, 0);
        Grid.SetRow(ConnectionCard, 0);
        Grid.SetColumnSpan(ConnectionCard, 1);

        Grid.SetColumn(ParamsCard, 1);
        Grid.SetRow(ParamsCard, 0);
        Grid.SetColumnSpan(ParamsCard, 1);

        Grid.SetColumn(ActionsCard, 2);
        Grid.SetRow(ActionsCard, 0);
        Grid.SetColumnSpan(ActionsCard, 1);

        ResultsCol0.Width = new GridLength(2, GridUnitType.Star);
        ResultsCol1.Width = new GridLength(1, GridUnitType.Star);
        Grid.SetColumn(ArtifactsCard, 0);
        Grid.SetRow(ArtifactsCard, 0);
        Grid.SetColumnSpan(ArtifactsCard, 1);
        Grid.SetColumn(RunResultCard, 1);
        Grid.SetRow(RunResultCard, 0);
        Grid.SetColumnSpan(RunResultCard, 1);
    }
}

