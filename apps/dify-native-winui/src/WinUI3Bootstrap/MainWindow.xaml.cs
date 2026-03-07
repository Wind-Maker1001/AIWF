using Microsoft.UI.Xaml;
using Microsoft.UI.Xaml.Controls;
using Microsoft.UI.Xaml.Input;
using Microsoft.UI.Xaml.Media;
using Microsoft.UI.Xaml.Media.Animation;
using Microsoft.UI.Xaml.Shapes;
using Microsoft.UI.Dispatching;
using Microsoft.UI.Input;
using AIWF.Native.Nodes;
using AIWF.Native.Runtime;
using AIWF.Native.ViewModels;
using CanvasRuntime = AIWF.Native.CanvasRuntime;
using System.Numerics;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Windows.Foundation;

namespace AIWF.Native;

public sealed partial class MainWindow : Window
{
    private enum NavSection
    {
        Workspace,
        Canvas,
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
    private readonly MainViewModel _viewModel = new();
    private readonly NodeCatalogService _nodeCatalog = new();
    private readonly WorkflowRunnerAdapter _runnerAdapter;
    private readonly RunFlowCoordinator _runFlowCoordinator;
    private readonly CanvasRuntime.CanvasViewportEngine _canvasViewportEngine = new(CanvasMinScale, CanvasMaxScale);
    private readonly List<NodeTemplate> _quickNodeTemplates;
    private NavSection _activeSection = NavSection.Workspace;
    private bool _didPlayIntroAnimation;
    private bool _didSetInitialCanvasView;
    private bool _isCanvasStacked;
    private bool _isCanvasPanning;
    private bool _isMarqueeSelecting;
    private bool _isSpaceHeld;
    private bool _isPointerPanningMode;
    private long _lastSpaceKeyTickMs;
    private uint _marqueePointerId;
    private Point _marqueeStartPoint;
    private Point _panStartPoint;
    private double _panStartTranslateX;
    private double _panStartTranslateY;
    private Border? _draggingNode;
    private uint _draggingNodePointerId;
    private Point _dragStartPoint;
    private double _dragStartNodeLeft;
    private double _dragStartNodeTop;
    private bool _nodeMovedDuringDrag;
    private bool _isCreatingConnection;
    private Border? _connectionSourceNode;
    private uint _connectionPointerId;
    private Line? _connectionPreviewLine;
    private bool _isResizingCanvasPanels;
    private uint _resizePointerId;
    private double _resizeStartX;
    private double _resizeStartLeftWidth;
    private double _resizeStartRightWidth;
    private bool _isResizingCanvasRows;
    private uint _resizeRowPointerId;
    private double _resizeStartY;
    private double _resizeStartTopHeight;
    private double _resizeStartBottomHeight;
    private Border? _inputNode;
    private Border? _cleanNode;
    private Border? _outputNode;
    private Border? _selectedNode;
    private readonly HashSet<Border> _multiSelectedNodes = new();
    private ConnectionEdge? _selectedConnection;
    private readonly List<Border> _artifactNodes = new();
    private readonly List<ConnectionEdge> _connections = new();
    private int _customNodeCounter = 1;
    private readonly MenuFlyout _canvasBlankFlyout = new();
    private readonly MenuFlyout _canvasNodeFlyout = new();
    private readonly MenuFlyout _canvasConnectionFlyout = new();
    private readonly Flyout _addNodeFlyout = new();
    private Border? _addNodeFlyoutRoot;
    private StackPanel? _addNodeFlyoutStack;
    private ScrollViewer? _addNodeFlyoutScroller;
    private readonly List<Grid> _addNodeFlyoutGroupGrids = new();
    private Border? _contextNode;
    private ConnectionEdge? _contextConnection;
    private Point _contextCanvasPoint;
    private CanvasNodeDto? _copiedNodeTemplate;
    private readonly DispatcherQueueTimer? _canvasAutosaveTimer;
    private readonly DispatcherQueueTimer? _statusDecayTimer;
    private readonly DispatcherQueueTimer? _canvasInteractionSettleTimer;
    private bool _hasPendingPanelSplit;
    private double _pendingLeftWidth;
    private double _pendingRightWidth;
    private bool _hasPendingRowSplit;
    private double _pendingTopHeight;
    private double _pendingBottomHeight;
    private bool _suppressCanvasAutosave;
    private InlineStatusTone _currentInlineTone = InlineStatusTone.Neutral;
    private bool _isZoomInlineEditing;
    private bool _isEnforcingWindowMinSize;
    private IntPtr _windowHandle;
    private IntPtr _previousWndProc;
    private WndProcDelegate? _wndProcDelegate;
    private Point _lastNodeSpawnPoint;

    private const double CanvasMinScale = 0.6;
    private const double CanvasMaxScale = 2.4;
    private const double CanvasGridSize = 20;
    private const double CanvasExtendChunk = 2400;
    private const double CanvasExtendThreshold = 800;
    private const double DefaultCanvasWidth = 3200;
    private const double DefaultCanvasHeight = 2200;
    private const double MaxCanvasWidth = 14000;
    private const double MaxCanvasHeight = 10000;
    private readonly bool _showCanvasGrid = false;
    private const int MinWindowWidth = 900;
    private const int MinWindowHeight = 620;
    private const string DefaultInlineStatusText = "就绪";
    private static readonly TimeSpan SuccessStatusDuration = TimeSpan.FromMilliseconds(1800);
    private static readonly TimeSpan NeutralStatusDuration = TimeSpan.FromMilliseconds(1500);
    private double _canvasWidth = DefaultCanvasWidth;
    private double _canvasHeight = DefaultCanvasHeight;
    private double _gridBuiltWidth = -1;
    private double _gridBuiltHeight = -1;
    private bool _didPrewarmCanvasSection;
    private static readonly string CanvasStateFilePath = System.IO.Path.Combine(
        Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
        "AIWF",
        "canvas-workflow.json");
    private const int GwlWndProc = -4;
    private const uint WmGetMinMaxInfo = 0x0024;

    private delegate IntPtr WndProcDelegate(IntPtr hWnd, uint msg, IntPtr wParam, IntPtr lParam);

    [StructLayout(LayoutKind.Sequential)]
    private struct Win32Point
    {
        public int X;
        public int Y;
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct MinMaxInfo
    {
        public Win32Point ptReserved;
        public Win32Point ptMaxSize;
        public Win32Point ptMaxPosition;
        public Win32Point ptMinTrackSize;
        public Win32Point ptMaxTrackSize;
    }

    private sealed class CanvasNodeTag
    {
        public required string NodeKey { get; init; }
        public string? ArtifactPath { get; set; }
        public string? ArtifactKind { get; init; }
        public bool IsUserNode { get; init; }
        public TextBlock? TitleBlock { get; set; }
        public TextBlock? SubtitleBlock { get; set; }
    }

    private sealed class ConnectorTag
    {
        public required Border Node { get; init; }
        public required string Kind { get; init; } // "in" or "out"
    }

    private sealed class ConnectionEdge
    {
        public required Border Source { get; init; }
        public required Border Target { get; init; }
        public required Line Line { get; init; }
    }

    private sealed class NodeTemplate
    {
        public required string KeyPrefix { get; init; }
        public required string Title { get; init; }
        public required string Subtitle { get; init; }
        public Symbol Icon { get; init; } = Symbol.Page;
        public string? Group { get; init; }
    }

    private sealed class CanvasSnapshot
    {
        public double CanvasWidth { get; set; }
        public double CanvasHeight { get; set; }
        public double ViewScale { get; set; } = 1;
        public double ViewTranslateX { get; set; }
        public double ViewTranslateY { get; set; }
        public List<CanvasNodeDto> Nodes { get; set; } = new();
        public List<CanvasEdgeDto> Edges { get; set; } = new();
    }

    private sealed class CanvasNodeDto
    {
        public string NodeKey { get; set; } = string.Empty;
        public string Title { get; set; } = string.Empty;
        public string Subtitle { get; set; } = string.Empty;
        public double X { get; set; }
        public double Y { get; set; }
        public bool IsUserNode { get; set; }
    }

    private sealed class CanvasEdgeDto
    {
        public string SourceKey { get; set; } = string.Empty;
        public string TargetKey { get; set; } = string.Empty;
    }

    public MainWindow()
    {
        NativePerfRecorder.Mark("main_window_ctor_enter");
        InitializeComponent();
        _runnerAdapter = new WorkflowRunnerAdapter(_http);
        _runFlowCoordinator = new RunFlowCoordinator(_http, _runnerAdapter);
        _quickNodeTemplates = _nodeCatalog
            .GetQuickTemplates()
            .Select(static x => new NodeTemplate
            {
                KeyPrefix = x.KeyPrefix,
                Title = x.Title,
                Subtitle = x.Subtitle,
                Icon = x.Icon,
                Group = x.Group
            })
            .ToList();
        SyncViewModelFromInputs();
        InitializeWindowMinimumTrackingSize();
        InitializeCanvasContextMenus();
        InitializeAddNodeFlyout();
        InitializeKeyboardAccelerators();
        InitializeCanvasKeyStateTracking();
        var dispatcherQueue = DispatcherQueue.GetForCurrentThread();
        if (dispatcherQueue is not null)
        {
            _canvasAutosaveTimer = dispatcherQueue.CreateTimer();
            _canvasAutosaveTimer.Interval = TimeSpan.FromMilliseconds(650);
            _canvasAutosaveTimer.IsRepeating = false;
            _canvasAutosaveTimer.Tick += (_, _) =>
            {
                _canvasAutosaveTimer.Stop();
                SaveCanvasSnapshot(showStatus: false);
            };

            _statusDecayTimer = dispatcherQueue.CreateTimer();
            _statusDecayTimer.IsRepeating = false;
            _statusDecayTimer.Tick += (_, _) =>
            {
                _statusDecayTimer.Stop();
                if (_currentInlineTone is InlineStatusTone.Busy or InlineStatusTone.Error)
                {
                    return;
                }

                StatusTextBlock.Text = DefaultInlineStatusText;
                StatusTextBlock.Foreground = new SolidColorBrush(Windows.UI.Color.FromArgb(0xFF, 0x4B, 0x55, 0x63));
                _currentInlineTone = InlineStatusTone.Neutral;
            };

            _canvasInteractionSettleTimer = dispatcherQueue.CreateTimer();
            _canvasInteractionSettleTimer.Interval = TimeSpan.FromMilliseconds(140);
            _canvasInteractionSettleTimer.IsRepeating = false;
            _canvasInteractionSettleTimer.Tick += (_, _) =>
            {
                _canvasInteractionSettleTimer.Stop();
                CanvasGridLayer.Visibility = Visibility.Visible;
            };

        }
        ConfigureSystemBackdrop();
        SetActiveSection(_activeSection);
        ApplyCommandButtonState();
        Activated += OnWindowActivated;
        SizeChanged += OnWindowSizeChanged;
        Closed += OnWindowClosed;
        ApplyResponsiveLayout();
        try
        {
            InitializeCanvasWorkspace();
            PrewarmCanvasSection();
        }
        catch (Exception ex)
        {
            SetInlineStatus($"画布初始化失败：{ex.Message}", InlineStatusTone.Error);
        }
        try
        {
            AppWindow.Resize(new Windows.Graphics.SizeInt32(
                Math.Max(1180, MinWindowWidth),
                Math.Max(760, MinWindowHeight)));
        }
        catch
        {
            // Keep startup resilient if window sizing APIs are unavailable.
        }

        NativePerfRecorder.Mark("main_window_ctor_exit");
    }

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
        btn.Click += OnAddNodeTemplateClick;
        return btn;
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

    private async void OnHealthClick(object sender, RoutedEventArgs e)
    {
        SyncViewModelFromInputs();
        await SetBusyAsync(true, "正在检查桥接服务健康状态...", InlineStatusTone.Busy);
        try
        {
            var baseUrl = GetBridgeBaseUrlOrThrow();
            var (response, text) = await _runnerAdapter.CheckHealthAsync(baseUrl, ApiKeyTextBox.Text.Trim());
            RawResponseTextBox.Text = PrettyJson(text);
            RunReferenceTextBlock.Text = response.IsSuccessStatusCode
                ? "连接正常，可直接运行。"
                : "连接异常，请检查服务是否启动。";
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

    private string GetBridgeBaseUrlOrThrow()
    {
        ResetValidationVisuals();
        var baseUrl = BridgeUrlTextBox.Text.Trim().TrimEnd('/');
        if (string.IsNullOrWhiteSpace(baseUrl))
        {
            SetInputError(BridgeUrlTextBox, true);
            throw new InvalidOperationException("桥接地址不能为空。");
        }

        return baseUrl;
    }

    private sealed record RunRequestInput(
        string BaseUrl,
        string ApiKey,
        string Owner,
        string JobId,
        string Flow,
        JsonObject Payload);

    private RunRequestInput CollectRunRequestInput()
    {
        return new RunRequestInput(
            GetBridgeBaseUrlOrThrow(),
            ApiKeyTextBox.Text.Trim(),
            OwnerTextBox.Text.Trim(),
            JobIdTextBox.Text.Trim(),
            FlowTextBox.Text.Trim(),
            BuildRunCleaningPayload());
    }

    private async Task<RunFlowExecutionResult> ExecuteRunRequestAsync(RunRequestInput input)
    {
        return await _runFlowCoordinator.ExecuteAsync(
            input.BaseUrl,
            input.ApiKey,
            input.Owner,
            input.JobId,
            input.Flow,
            input.Payload);
    }

    private bool TryApplyRunExecutionResult(RunFlowExecutionResult exec)
    {
        if (!string.IsNullOrWhiteSpace(exec.EffectiveJobId))
        {
            JobIdTextBox.Text = exec.EffectiveJobId;
        }

        if (exec.RetryInfo.StartsWith("预检创建作业：", StringComparison.Ordinal))
        {
            RunReferenceTextBlock.Text = "已自动准备可用任务。";
        }

        if (exec.RetriedAfterServerError)
        {
            SetInlineStatus("检测到服务端 500，已自动创建新作业并重试一次...", InlineStatusTone.Busy);
            RunReferenceTextBlock.Text = "已自动重试一次。";
        }

        RawResponseTextBox.Text = PrettyJson(exec.Body);

        if (!exec.Response.IsSuccessStatusCode)
        {
            RunReferenceTextBlock.Text = "运行失败，请稍后重试。";
            SetInlineStatus($"运行失败：{(int)exec.Response.StatusCode}", InlineStatusTone.Error);
            return false;
        }

        BindRunResult(exec.Body, exec.RetryInfo);
        RunReferenceTextBlock.Text = "运行成功，结果已更新。";
        SetInlineStatus("流程运行请求已完成。", InlineStatusTone.Success);
        SetActiveSection(NavSection.Results);
        return true;
    }

    private async void OnRunCleaningClick(object sender, RoutedEventArgs e)
    {
        SyncViewModelFromInputs();
        if (!ValidateRunInputs(out var validationMessage))
        {
            SetInlineStatus(validationMessage, InlineStatusTone.Error);
            return;
        }

        await SetBusyAsync(true, "正在提交流程运行请求...", InlineStatusTone.Busy);
        try
        {
            RunReferenceTextBlock.Text = "正在准备运行...";
            var input = CollectRunRequestInput();
            var exec = await ExecuteRunRequestAsync(input);
            TryApplyRunExecutionResult(exec);
        }
        catch (Exception ex)
        {
            RunReferenceTextBlock.Text = "运行异常，请检查服务状态。";
            SetInlineStatus($"运行请求异常：{ex.Message}", InlineStatusTone.Error);
        }
        finally
        {
            await SetBusyAsync(false, StatusTextBlock.Text, InferToneFromStatus());
        }
    }

    private void SyncViewModelFromInputs()
    {
        _viewModel.BridgeUrl = BridgeUrlTextBox.Text.Trim();
        _viewModel.Actor = ActorTextBox.Text.Trim();
        _viewModel.Owner = OwnerTextBox.Text.Trim();
        _viewModel.JobId = JobIdTextBox.Text.Trim();
        _viewModel.Flow = FlowTextBox.Text.Trim();
    }

    private JsonObject BuildRunCleaningPayload()
    {
        var input = new RunPayloadInput(
            ActorTextBox.Text.Trim(),
            ReadComboValue(OfficeThemeComboBox),
            ReadComboValue(OfficeLangComboBox),
            ReportTitleTextBox.Text.Trim(),
            InputCsvTextBox.Text.Trim());
        return RunPayloadBuilder.BuildCleaningPayload(input);
    }

    private static string ReadComboValue(ComboBox comboBox)
    {
        if (comboBox.SelectedItem is ComboBoxItem item && item.Content is string value)
        {
            return value;
        }

        return comboBox.SelectedValue?.ToString() ?? "zh";
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

    private void OnCanvasNavClick(object sender, RoutedEventArgs e)
    {
        SetActiveSection(NavSection.Canvas);
    }

    private void SetActiveSection(NavSection section)
    {
        _activeSection = section;
        WorkspaceSectionGrid.Visibility = section == NavSection.Workspace
            ? Visibility.Visible
            : Visibility.Collapsed;
        CanvasSectionGrid.Visibility = section == NavSection.Canvas
            ? Visibility.Visible
            : Visibility.Collapsed;
        ResultsSectionGrid.Visibility = section == NavSection.Results
            ? Visibility.Visible
            : Visibility.Collapsed;

        ApplyNavButtonState(WorkspaceNavButton, section == NavSection.Workspace);
        ApplyNavButtonState(CanvasNavButton, section == NavSection.Canvas);
        ApplyNavButtonState(ResultNavButton, section == NavSection.Results);
        var activeElement = section switch
        {
            NavSection.Workspace => WorkspaceSectionGrid,
            NavSection.Canvas => CanvasSectionGrid,
            _ => ResultsSectionGrid
        };
        PlaySectionEntrance(activeElement);
    }

    private void PrewarmCanvasSection()
    {
        if (_didPrewarmCanvasSection)
        {
            return;
        }

        _didPrewarmCanvasSection = true;
        var shouldRestoreCollapsed = _activeSection != NavSection.Canvas;
        var oldOpacity = CanvasSectionGrid.Opacity;
        CanvasSectionGrid.Opacity = 0;
        CanvasSectionGrid.Visibility = Visibility.Visible;
        CanvasSectionGrid.UpdateLayout();
        CanvasSectionGrid.Opacity = oldOpacity;
        if (shouldRestoreCollapsed)
        {
            CanvasSectionGrid.Visibility = Visibility.Collapsed;
        }
    }

    private static void ApplyNavButtonState(Button button, bool active)
    {
        ApplyButtonVisual(button, NavigationStylePresenter.NavButton(active));
    }

    private void ApplyCommandButtonState()
    {
        var visuals = NavigationStylePresenter.CommandButtons();
        ApplyButtonVisual(QuickRunButton, visuals.RunButton);
        ApplyButtonVisual(QuickHealthButton, visuals.HealthButton);
    }

    private static void ApplyButtonVisual(Button button, ButtonVisual visual)
    {
        button.FontWeight = visual.IsActive ? Microsoft.UI.Text.FontWeights.SemiBold : Microsoft.UI.Text.FontWeights.Normal;
        button.Background = new SolidColorBrush(visual.Background);
        button.Foreground = new SolidColorBrush(visual.Foreground);
        button.BorderBrush = new SolidColorBrush(visual.Border);
        button.BorderThickness = new Thickness(visual.BorderThickness);
        button.CornerRadius = new CornerRadius(visual.CornerRadius);
    }

    private void ResetValidationVisuals()
    {
        foreach (var control in GetValidationControls())
        {
            SetInputError(control, false);
        }
    }

    private static void SetInputError(Control control, bool hasError)
    {
        control.BorderBrush = new SolidColorBrush(InputFieldPresenter.ResolveBorderColor(hasError));
    }

    private IEnumerable<Control> GetValidationControls()
    {
        yield return BridgeUrlTextBox;
        yield return ActorTextBox;
        yield return JobIdTextBox;
        yield return FlowTextBox;
        yield return ReportTitleTextBox;
    }

    private bool ValidateRunInputs(out string message)
    {
        ResetValidationVisuals();
        var result = RunInputValidator.Validate(new RunInputData(
            BridgeUrlTextBox.Text,
            ActorTextBox.Text,
            JobIdTextBox.Text,
            FlowTextBox.Text,
            ReportTitleTextBox.Text));

        if (result.MissingKeys.Contains("bridge_url"))
        {
            SetInputError(BridgeUrlTextBox, true);
        }
        if (result.MissingKeys.Contains("actor"))
        {
            SetInputError(ActorTextBox, true);
        }
        if (result.MissingKeys.Contains("job_id"))
        {
            SetInputError(JobIdTextBox, true);
        }
        if (result.MissingKeys.Contains("flow"))
        {
            SetInputError(FlowTextBox, true);
        }
        if (result.MissingKeys.Contains("report_title"))
        {
            SetInputError(ReportTitleTextBox, true);
        }

        message = result.Message;
        return result.IsValid;
    }

    private void SetInlineStatus(string message, InlineStatusTone tone)
    {
        StatusTextBlock.Text = StatusPresenter.NormalizeMessage(message, DefaultInlineStatusText);
        _currentInlineTone = tone;
        var toneKey = tone switch
        {
            InlineStatusTone.Success => StatusPresenter.ToneSuccess,
            InlineStatusTone.Error => StatusPresenter.ToneError,
            InlineStatusTone.Busy => StatusPresenter.ToneBusy,
            _ => StatusPresenter.ToneNeutral
        };
        StatusTextBlock.Foreground = new SolidColorBrush(StatusPresenter.ResolveForeground(toneKey));

        if (_statusDecayTimer is null)
        {
            return;
        }

        _statusDecayTimer.Stop();
        if (tone == InlineStatusTone.Success)
        {
            _statusDecayTimer.Interval = SuccessStatusDuration;
            _statusDecayTimer.Start();
        }
        else if (tone == InlineStatusTone.Neutral)
        {
            _statusDecayTimer.Interval = NeutralStatusDuration;
            _statusDecayTimer.Start();
        }
    }

    private InlineStatusTone InferToneFromStatus()
    {
        var toneKey = StatusPresenter.InferTone(StatusTextBlock.Text ?? string.Empty);
        return toneKey switch
        {
            StatusPresenter.ToneSuccess => InlineStatusTone.Success,
            StatusPresenter.ToneError => InlineStatusTone.Error,
            StatusPresenter.ToneBusy => InlineStatusTone.Busy,
            _ => InlineStatusTone.Neutral
        };
    }

    private void InitializeCanvasWorkspace()
    {
        ResetCanvasView();
        CanvasViewport.SizeChanged += OnCanvasViewportSizeChanged;
        WorkspaceCanvas.RightTapped += OnWorkspaceCanvasRightTapped;
        BuildCanvasGrid();
        SeedCanvasNodes();
        TryLoadCanvasSnapshot(showStatus: false, missingIsError: false);
        UpdateNodePropertyPanel();
        UpdateCanvasZoomIndicator();
    }

    private void OnCanvasViewportSizeChanged(object sender, SizeChangedEventArgs e)
    {
        if (!_didSetInitialCanvasView && CanvasViewport.ActualWidth > 0 && CanvasViewport.ActualHeight > 0)
        {
            ResetCanvasView();
            _didSetInitialCanvasView = true;
        }

        ClampCanvasTransform();
        UpdateCanvasZoomIndicator();
    }

    private void BuildCanvasGrid(bool force = false)
    {
        WorkspaceCanvas.Width = _canvasWidth;
        WorkspaceCanvas.Height = _canvasHeight;
        CanvasGridLayer.Width = _canvasWidth;
        CanvasGridLayer.Height = _canvasHeight;
        CanvasGridLayer.Visibility = _showCanvasGrid ? Visibility.Visible : Visibility.Collapsed;

        var sameExtent = Math.Abs(_gridBuiltWidth - _canvasWidth) < 0.1
            && Math.Abs(_gridBuiltHeight - _canvasHeight) < 0.1;
        if (!_showCanvasGrid)
        {
            if (CanvasGridLayer.Children.Count > 0)
            {
                CanvasGridLayer.Children.Clear();
            }
            _gridBuiltWidth = _canvasWidth;
            _gridBuiltHeight = _canvasHeight;
            return;
        }

        if (!force && sameExtent && CanvasGridLayer.Children.Count > 0)
        {
            return;
        }

        CanvasGridLayer.Children.Clear();
        var gridStep = CanvasGridSize;
        while ((_canvasWidth / gridStep) > 700 || (_canvasHeight / gridStep) > 500)
        {
            gridStep *= 2;
        }
        var majorEvery = gridStep * 5;
        var majorBrush = new SolidColorBrush(Windows.UI.Color.FromArgb(0x33, 0x55, 0x55, 0x55));
        var minorBrush = new SolidColorBrush(Windows.UI.Color.FromArgb(0x14, 0x55, 0x55, 0x55));
        for (var x = 0.0; x <= _canvasWidth; x += gridStep)
        {
            var isMajor = (x % majorEvery) == 0;
            CanvasGridLayer.Children.Add(new Line
            {
                X1 = x,
                Y1 = 0,
                X2 = x,
                Y2 = _canvasHeight,
                StrokeThickness = isMajor ? 1.1 : 1,
                Stroke = isMajor ? majorBrush : minorBrush
            });
        }

        for (var y = 0.0; y <= _canvasHeight; y += gridStep)
        {
            var isMajor = (y % majorEvery) == 0;
            CanvasGridLayer.Children.Add(new Line
            {
                X1 = 0,
                Y1 = y,
                X2 = _canvasWidth,
                Y2 = y,
                StrokeThickness = isMajor ? 1.1 : 1,
                Stroke = isMajor ? majorBrush : minorBrush
            });
        }

        _gridBuiltWidth = _canvasWidth;
        _gridBuiltHeight = _canvasHeight;
    }

    private void SeedCanvasNodes()
    {
        WorkspaceCanvas.Children.Clear();
        _artifactNodes.Clear();
        _connections.Clear();
        _inputNode = null;
        _cleanNode = null;
        _outputNode = null;
        _selectedNode = null;
        _multiSelectedNodes.Clear();
        _selectedConnection = null;
    }

}
