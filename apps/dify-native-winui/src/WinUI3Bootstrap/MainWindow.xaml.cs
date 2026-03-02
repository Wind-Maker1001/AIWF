using Microsoft.UI.Xaml;
using Microsoft.UI.Xaml.Controls;
using Microsoft.UI.Xaml.Input;
using Microsoft.UI.Xaml.Media;
using Microsoft.UI.Xaml.Media.Animation;
using Microsoft.UI.Xaml.Shapes;
using Microsoft.UI.Dispatching;
using Microsoft.UI.Input;
using System.Numerics;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Http.Headers;
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

    private static readonly List<NodeTemplate> QuickNodeTemplates = new()
    {
        new NodeTemplate { KeyPrefix = "create-job", Title = "新建任务", Subtitle = "开始一次新的处理", Icon = Symbol.Add, Group = "基础操作" },
        new NodeTemplate { KeyPrefix = "run-cleaning", Title = "开始清洗", Subtitle = "执行清洗流程", Icon = Symbol.Play, Group = "基础操作" },
        new NodeTemplate { KeyPrefix = "check-steps", Title = "查看进度", Subtitle = "看每一步执行状态", Icon = Symbol.Clock, Group = "基础操作" },
        new NodeTemplate { KeyPrefix = "check-files", Title = "查看文件", Subtitle = "看生成了哪些结果文件", Icon = Symbol.Document, Group = "基础操作" },
        new NodeTemplate { KeyPrefix = "check-job", Title = "查看状态", Subtitle = "看任务当前状态", Icon = Symbol.View, Group = "基础操作" },
        new NodeTemplate { KeyPrefix = "one-click", Title = "一键清洗", Subtitle = "一键执行并返回结果", Icon = Symbol.Accept, Group = "基础操作" },

        new NodeTemplate { KeyPrefix = "process", Title = "数据处理", Subtitle = "整理和清洗字段", Icon = Symbol.Switch, Group = "数据处理" },
        new NodeTemplate { KeyPrefix = "load", Title = "数据加载", Subtitle = "读取或写入数据", Icon = Symbol.Upload, Group = "数据处理" },
        new NodeTemplate { KeyPrefix = "join", Title = "数据合并", Subtitle = "按条件合并两组数据", Icon = Symbol.Shuffle, Group = "数据处理" },
        new NodeTemplate { KeyPrefix = "summary", Title = "数据汇总", Subtitle = "分组统计结果", Icon = Symbol.AllApps, Group = "数据处理" },
        new NodeTemplate { KeyPrefix = "quality", Title = "质量检查", Subtitle = "检查异常和质量问题", Icon = Symbol.Important, Group = "数据处理" },

        new NodeTemplate { KeyPrefix = "custom", Title = "空白节点", Subtitle = "自定义名称和说明", Icon = Symbol.Page, Group = "其他" }
    };

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
        InitializeComponent();
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
    }

    private void InitializeWindowMinimumTrackingSize()
    {
        try
        {
            _windowHandle = WinRT.Interop.WindowNative.GetWindowHandle(this);
            if (_windowHandle == IntPtr.Zero)
            {
                return;
            }

            _wndProcDelegate = WindowMessageHandler;
            var handlerPtr = Marshal.GetFunctionPointerForDelegate(_wndProcDelegate);
            _previousWndProc = SetWindowLongPtr(_windowHandle, GwlWndProc, handlerPtr);
        }
        catch
        {
            // Fallback to managed resize guard only if hook is unavailable.
        }
    }

    private void OnWindowClosed(object sender, WindowEventArgs args)
    {
        _isSpaceHeld = false;
        _isPointerPanningMode = false;
        if (_windowHandle == IntPtr.Zero || _previousWndProc == IntPtr.Zero)
        {
            return;
        }

        try
        {
            SetWindowLongPtr(_windowHandle, GwlWndProc, _previousWndProc);
        }
        catch
        {
            // Ignore shutdown cleanup failures.
        }
    }

    private IntPtr WindowMessageHandler(IntPtr hWnd, uint msg, IntPtr wParam, IntPtr lParam)
    {
        if (msg == WmGetMinMaxInfo)
        {
            var info = Marshal.PtrToStructure<MinMaxInfo>(lParam);
            var dpi = GetDpiForWindow(hWnd);
            if (dpi <= 0)
            {
                dpi = 96;
            }

            var scale = dpi / 96.0;
            info.ptMinTrackSize.X = (int)Math.Ceiling(MinWindowWidth * scale);
            info.ptMinTrackSize.Y = (int)Math.Ceiling(MinWindowHeight * scale);
            Marshal.StructureToPtr(info, lParam, fDeleteOld: false);
            return IntPtr.Zero;
        }

        return CallWindowProc(_previousWndProc, hWnd, msg, wParam, lParam);
    }

    private void InitializeKeyboardAccelerators()
    {
        if (Content is not UIElement rootElement)
        {
            return;
        }

        AddShortcut(rootElement, Windows.System.VirtualKey.S, Windows.System.VirtualKeyModifiers.Control, (_, args) =>
        {
            if (_activeSection != NavSection.Canvas)
            {
                return;
            }

            SaveCanvasSnapshot(showStatus: true);
            args.Handled = true;
        });

        AddShortcut(rootElement, Windows.System.VirtualKey.O, Windows.System.VirtualKeyModifiers.Control, (_, args) =>
        {
            if (_activeSection != NavSection.Canvas)
            {
                return;
            }

            TryLoadCanvasSnapshot(showStatus: true, missingIsError: true);
            args.Handled = true;
        });

        AddShortcut(rootElement, Windows.System.VirtualKey.N, Windows.System.VirtualKeyModifiers.Control, (_, args) =>
        {
            if (_activeSection != NavSection.Canvas)
            {
                return;
            }

            CreateNewCanvas();
            args.Handled = true;
        });

        AddShortcut(rootElement, Windows.System.VirtualKey.Number0, Windows.System.VirtualKeyModifiers.Control, (_, args) =>
        {
            if (_activeSection != NavSection.Canvas)
            {
                return;
            }

            ResetCanvasView();
            ClampCanvasTransform();
            SetInlineStatus("已重置画布视图。", InlineStatusTone.Success);
            args.Handled = true;
        });

        AddShortcut(rootElement, Windows.System.VirtualKey.Delete, Windows.System.VirtualKeyModifiers.None, (_, args) =>
        {
            if (_activeSection != NavSection.Canvas || IsTextInputFocused())
            {
                return;
            }

            if (!DeleteSelectedConnection())
            {
                DeleteSelectedUserNode();
            }
            args.Handled = true;
        });
    }

    private static void AddShortcut(
        UIElement rootElement,
        Windows.System.VirtualKey key,
        Windows.System.VirtualKeyModifiers modifiers,
        TypedEventHandler<KeyboardAccelerator, KeyboardAcceleratorInvokedEventArgs> invoked)
    {
        var accelerator = new KeyboardAccelerator
        {
            Key = key,
            Modifiers = modifiers
        };
        accelerator.Invoked += invoked;
        rootElement.KeyboardAccelerators.Add(accelerator);
    }

    private void InitializeCanvasKeyStateTracking()
    {
        if (Content is not FrameworkElement rootElement)
        {
            return;
        }

        rootElement.KeyDown += (_, args) =>
        {
            if (args.Key == Windows.System.VirtualKey.Space)
            {
                _isSpaceHeld = true;
                _lastSpaceKeyTickMs = Environment.TickCount64;
                if (_activeSection == NavSection.Canvas && !IsTextInputFocused())
                {
                    args.Handled = true;
                }
            }
        };
        rootElement.KeyUp += (_, args) =>
        {
            if (args.Key == Windows.System.VirtualKey.Space)
            {
                _isSpaceHeld = false;
                _lastSpaceKeyTickMs = Environment.TickCount64;
                if (_activeSection == NavSection.Canvas && !IsTextInputFocused())
                {
                    args.Handled = true;
                }
            }
        };
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

    private bool IsTextInputFocused()
    {
        var root = Content as FrameworkElement;
        var xamlRoot = root?.XamlRoot;
        if (xamlRoot is null)
        {
            return false;
        }

        var focused = FocusManager.GetFocusedElement(xamlRoot);
        return focused is TextBox
            || focused is PasswordBox
            || focused is AutoSuggestBox
            || focused is ComboBox;
    }

    private void InitializeCanvasContextMenus()
    {
        var centerItem = new MenuFlyoutItem { Text = "视图居中到此处" };
        centerItem.Click += OnCenterViewHereClick;
        var pasteItem = new MenuFlyoutItem { Text = "粘贴节点" };
        pasteItem.Click += OnPasteNodeClick;
        _canvasBlankFlyout.Items.Add(centerItem);
        _canvasBlankFlyout.Items.Add(new MenuFlyoutSeparator());
        _canvasBlankFlyout.Items.Add(pasteItem);
        _canvasBlankFlyout.Opening += (_, _) =>
        {
            pasteItem.IsEnabled = _copiedNodeTemplate is not null;
        };

        var copyNodeItem = new MenuFlyoutItem { Text = "复制节点" };
        copyNodeItem.Click += OnCopyNodeClick;
        var pasteNodeItem = new MenuFlyoutItem { Text = "粘贴节点" };
        pasteNodeItem.Click += OnPasteNodeClick;
        var deleteNodeItem = new MenuFlyoutItem { Text = "删除节点" };
        deleteNodeItem.Click += OnDeleteNodeFromContextClick;
        _canvasNodeFlyout.Items.Add(copyNodeItem);
        _canvasNodeFlyout.Items.Add(pasteNodeItem);
        _canvasNodeFlyout.Items.Add(new MenuFlyoutSeparator());
        _canvasNodeFlyout.Items.Add(deleteNodeItem);
        _canvasNodeFlyout.Opening += (_, _) =>
        {
            var canDelete = _contextNode?.Tag is CanvasNodeTag tag && tag.IsUserNode;
            pasteNodeItem.IsEnabled = _copiedNodeTemplate is not null;
            deleteNodeItem.IsEnabled = canDelete;
        };

        var deleteEdgeItem = new MenuFlyoutItem { Text = "删除连线" };
        deleteEdgeItem.Click += OnDeleteConnectionFromContextClick;
        _canvasConnectionFlyout.Items.Add(deleteEdgeItem);
        _canvasConnectionFlyout.Opening += (_, _) =>
        {
            deleteEdgeItem.IsEnabled = _contextConnection is not null;
        };
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

        var groups = QuickNodeTemplates.GroupBy(static t => t.Group ?? string.Empty).ToList();
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
        await SetBusyAsync(true, "正在检查桥接服务健康状态...", InlineStatusTone.Busy);
        try
        {
            using var request = CreateRequest(HttpMethod.Get, "/health");
            using var response = await _http.SendAsync(request);
            var text = await response.Content.ReadAsStringAsync();
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

    private async void OnRunCleaningClick(object sender, RoutedEventArgs e)
    {
        if (!ValidateRunInputs(out var validationMessage))
        {
            SetInlineStatus(validationMessage, InlineStatusTone.Error);
            return;
        }

        await SetBusyAsync(true, "正在提交流程运行请求...", InlineStatusTone.Busy);
        try
        {
            var payload = BuildRunCleaningPayload();
            var jobIdRaw = JobIdTextBox.Text.Trim();
            var flowRaw = FlowTextBox.Text.Trim();
            var retryInfo = "未重试";
            RunReferenceTextBlock.Text = "正在准备运行...";
            var ensuredJobId = await EnsureJobIdAsync(jobIdRaw);
            if (!string.IsNullOrWhiteSpace(ensuredJobId) && ensuredJobId != jobIdRaw)
            {
                JobIdTextBox.Text = ensuredJobId;
                jobIdRaw = ensuredJobId;
                retryInfo = $"预检创建作业：{ensuredJobId}";
                RunReferenceTextBlock.Text = "已自动准备可用任务。";
            }
            var (response, text) = await SendRunFlowAsync(jobIdRaw, flowRaw, payload);

            if (!response.IsSuccessStatusCode && response.StatusCode == System.Net.HttpStatusCode.InternalServerError)
            {
                var forcedJobId = await TryCreateJobAsync();
                if (!string.IsNullOrWhiteSpace(forcedJobId))
                {
                    JobIdTextBox.Text = forcedJobId;
                    SetInlineStatus("检测到服务端 500，已自动创建新作业并重试一次...", InlineStatusTone.Busy);
                    (response, text) = await SendRunFlowAsync(forcedJobId, flowRaw, payload);
                    retryInfo = $"已重试（新作业）：{forcedJobId}";
                    RunReferenceTextBlock.Text = "已自动重试一次。";
                }
            }

            RawResponseTextBox.Text = PrettyJson(text);
            RetryInfoTextBlock.Text = retryInfo;

            if (!response.IsSuccessStatusCode)
            {
                RunReferenceTextBlock.Text = "运行失败，请稍后重试。";
                SetInlineStatus($"运行失败：{(int)response.StatusCode}", InlineStatusTone.Error);
                return;
            }

            BindRunResult(text);
            RunReferenceTextBlock.Text = "运行成功，结果已更新。";
            SetInlineStatus("流程运行请求已完成。", InlineStatusTone.Success);
            SetActiveSection(NavSection.Results);
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

    private async Task<(HttpResponseMessage response, string text)> SendRunFlowAsync(string jobId, string flow, JsonObject payload)
    {
        var encodedJobId = Uri.EscapeDataString(jobId.Trim());
        var encodedFlow = Uri.EscapeDataString(flow.Trim());
        using var request = CreateRequest(HttpMethod.Post, $"/jobs/{encodedJobId}/run/{encodedFlow}");
        request.Content = new StringContent(payload.ToJsonString(), Encoding.UTF8, "application/json");
        var response = await _http.SendAsync(request);
        var text = await response.Content.ReadAsStringAsync();
        return (response, text);
    }

    private async Task<string?> TryCreateJobAsync()
    {
        var baseUrl = ResolveBaseUrlFromBridge();
        if (string.IsNullOrWhiteSpace(baseUrl))
        {
            return null;
        }

        var owner = string.IsNullOrWhiteSpace(OwnerTextBox.Text) ? "native" : OwnerTextBox.Text.Trim();
        var uri = $"{baseUrl}/api/v1/jobs/create?owner={Uri.EscapeDataString(owner)}";
        using var request = new HttpRequestMessage(HttpMethod.Post, uri);
        request.Content = new StringContent("{}", Encoding.UTF8, "application/json");
        request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
        var apiKey = ApiKeyTextBox.Text.Trim();
        if (!string.IsNullOrWhiteSpace(apiKey))
        {
            request.Headers.Add("X-API-Key", apiKey);
        }

        using var response = await _http.SendAsync(request);
        if (!response.IsSuccessStatusCode)
        {
            return null;
        }

        var text = await response.Content.ReadAsStringAsync();
        try
        {
            var root = JsonNode.Parse(text);
            return root?["job_id"]?.GetValue<string?>();
        }
        catch
        {
            return null;
        }
    }

    private async Task<string> EnsureJobIdAsync(string jobId)
    {
        var trimmed = (jobId ?? string.Empty).Trim();
        var baseUrl = ResolveBaseUrlFromBridge();
        if (string.IsNullOrWhiteSpace(baseUrl))
        {
            return trimmed;
        }

        if (!string.IsNullOrWhiteSpace(trimmed))
        {
            try
            {
                using var getReq = new HttpRequestMessage(HttpMethod.Get, $"{baseUrl}/api/v1/jobs/{Uri.EscapeDataString(trimmed)}");
                var apiKey = ApiKeyTextBox.Text.Trim();
                if (!string.IsNullOrWhiteSpace(apiKey))
                {
                    getReq.Headers.Add("X-API-Key", apiKey);
                }

                using var getResp = await _http.SendAsync(getReq);
                if (getResp.IsSuccessStatusCode)
                {
                    return trimmed;
                }
            }
            catch
            {
                // Fall through and attempt create.
            }
        }

        var created = await TryCreateJobAsync();
        return string.IsNullOrWhiteSpace(created) ? trimmed : created;
    }

    private string? ResolveBaseUrlFromBridge()
    {
        var bridge = BridgeUrlTextBox.Text.Trim().TrimEnd('/');
        if (string.IsNullOrWhiteSpace(bridge))
        {
            return null;
        }

        if (!Uri.TryCreate(bridge, UriKind.Absolute, out var uri))
        {
            return null;
        }

        var builder = new UriBuilder(uri);
        if (builder.Port == 18081)
        {
            builder.Port = 18080;
        }
        else if (builder.Port <= 0)
        {
            builder.Port = 18080;
        }

        builder.Path = string.Empty;
        builder.Query = string.Empty;
        return builder.Uri.ToString().TrimEnd('/');
    }

    private void BindRunResult(string json)
    {
        ArtifactsListView.Items.Clear();
        ClearCanvasArtifactNodes();
        SetCanvasNodeSubtitle(_inputNode, "源数据准备");
        SetCanvasNodeSubtitle(_cleanNode, "规则处理");
        SetCanvasNodeSubtitle(_outputNode, "等待运行结果");
        ArtifactsCountTextBlock.Text = "0 项";
        JobIdTextBlock.Text = "-";
        RetryInfoTextBlock.Text = "未重试";
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

        var artifactItems = new List<(string id, string kind, string path)>();
        foreach (var artifact in artifacts)
        {
            if (artifact is null)
            {
                continue;
            }

            var id = artifact["artifact_id"]?.GetValue<string?>() ?? "-";
            var kind = artifact["kind"]?.GetValue<string?>() ?? "-";
            var path = artifact["path"]?.GetValue<string?>() ?? "-";
            ArtifactsListView.Items.Add(FormatArtifactDisplay(kind, path, id));
            artifactItems.Add((id, kind, path));
        }

        ArtifactsCountTextBlock.Text = $"{ArtifactsListView.Items.Count} 项";
        UpdateCanvasArtifactNodes(artifactItems);
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

    private static string FormatArtifactDisplay(string kind, string path, string fallbackId)
    {
        var fileName = string.IsNullOrWhiteSpace(path) ? string.Empty : System.IO.Path.GetFileName(path);
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
        SetInputError(ActorTextBox, false);
        SetInputError(JobIdTextBox, false);
        SetInputError(FlowTextBox, false);
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

        if (string.IsNullOrWhiteSpace(ActorTextBox.Text))
        {
            SetInputError(ActorTextBox, true);
            valid = false;
            missing.Add("执行者");
        }

        if (string.IsNullOrWhiteSpace(JobIdTextBox.Text))
        {
            SetInputError(JobIdTextBox, true);
            valid = false;
            missing.Add("Job ID");
        }

        if (string.IsNullOrWhiteSpace(FlowTextBox.Text))
        {
            SetInputError(FlowTextBox, true);
            valid = false;
            missing.Add("Flow");
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
        StatusTextBlock.Text = string.IsNullOrWhiteSpace(message) ? DefaultInlineStatusText : message.Trim();
        _currentInlineTone = tone;
        StatusTextBlock.Foreground = new SolidColorBrush(
            tone switch
            {
                InlineStatusTone.Success => Windows.UI.Color.FromArgb(0xFF, 0x11, 0x11, 0x11),
                InlineStatusTone.Error => Windows.UI.Color.FromArgb(0xFF, 0xC6, 0x28, 0x28),
                InlineStatusTone.Busy => Windows.UI.Color.FromArgb(0xFF, 0x6B, 0x72, 0x80),
                _ => Windows.UI.Color.FromArgb(0xFF, 0x4B, 0x55, 0x63)
            });

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

    private Border CreateCanvasNode(
        string nodeKey,
        string title,
        string subtitle,
        double left,
        double top,
        string? artifactPath = null,
        string? artifactKind = null,
        bool isUserNode = false)
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
                IsUserNode = isUserNode,
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
        foreach (var node in _artifactNodes)
        {
            RemoveConnectionsForNode(node);
            WorkspaceCanvas.Children.Remove(node);
        }

        _artifactNodes.Clear();
    }

    private void UpdateCanvasArtifactNodes(List<(string id, string kind, string path)> artifacts)
    {
        ClearCanvasArtifactNodes();
        if (artifacts.Count == 0)
        {
            SetCanvasNodeSubtitle(_outputNode, "无可用产物");
            return;
        }

        SetCanvasNodeSubtitle(_outputNode, $"已生成 {artifacts.Count} 个产物");
        SetCanvasNodeSubtitle(_cleanNode, "处理完成");

        var startX = 1060.0;
        var startY = 100.0;
        const int rows = 4;
        const double gapX = 250;
        const double gapY = 118;

        for (var i = 0; i < artifacts.Count; i++)
        {
            var item = artifacts[i];
            var fileName = string.IsNullOrWhiteSpace(item.path) ? item.id : System.IO.Path.GetFileName(item.path);
            if (string.IsNullOrWhiteSpace(fileName))
            {
                fileName = item.id;
            }

            var col = i / rows;
            var row = i % rows;
            var x = startX + (col * gapX);
            var y = startY + (row * gapY);
            var node = CreateCanvasNode(
                $"artifact-{i}",
                KindToShortTitle(item.kind),
                fileName,
                x,
                y,
                item.path,
                item.kind);

            _artifactNodes.Add(node);
            WorkspaceCanvas.Children.Add(node);
        }
    }

    private static string KindToShortTitle(string kind)
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

    private void OnConnectorPointerPressed(object sender, PointerRoutedEventArgs e)
    {
        if (sender is not Ellipse ellipse || ellipse.Tag is not ConnectorTag connectorTag)
        {
            return;
        }

        if (connectorTag.Kind != "out")
        {
            return;
        }

        BeginConnection(connectorTag.Node, e);
    }

    private void BeginConnection(Border sourceNode, PointerRoutedEventArgs e)
    {
        CancelConnectionPreview();
        _isCreatingConnection = true;
        _connectionSourceNode = sourceNode;
        _connectionPointerId = e.Pointer.PointerId;
        SelectNode(sourceNode);
        DismissCanvasHint();

        var start = GetNodeOutputPoint(sourceNode);
        var pointer = e.GetCurrentPoint(WorkspaceCanvas).Position;
        _connectionPreviewLine = new Line
        {
            X1 = start.X,
            Y1 = start.Y,
            X2 = pointer.X,
            Y2 = pointer.Y,
            StrokeThickness = 2,
            Stroke = new SolidColorBrush(Windows.UI.Color.FromArgb(0xFF, 0xC6, 0x28, 0x28)),
            StrokeDashArray = new DoubleCollection { 4, 3 },
            IsHitTestVisible = false
        };
        Canvas.SetZIndex(_connectionPreviewLine, 2);
        WorkspaceCanvas.Children.Add(_connectionPreviewLine);
        CanvasViewport.CapturePointer(e.Pointer);
        e.Handled = true;
    }

    private void TryCompleteConnection(PointerRoutedEventArgs e)
    {
        if (!_isCreatingConnection || _connectionSourceNode is null)
        {
            return;
        }

        var target = ResolveNodeFromSource(e.OriginalSource as DependencyObject);
        if (target is not null && target != _connectionSourceNode)
        {
            AddConnection(_connectionSourceNode, target);
        }

        CancelConnectionPreview();
        e.Handled = true;
    }

    private void CancelConnectionPreview()
    {
        if (_connectionPreviewLine is not null)
        {
            WorkspaceCanvas.Children.Remove(_connectionPreviewLine);
            _connectionPreviewLine = null;
        }

        _isCreatingConnection = false;
        _connectionSourceNode = null;
        _connectionPointerId = 0;
    }

    private void AddConnection(Border source, Border target, bool select = true)
    {
        foreach (var edge in _connections)
        {
            if (edge.Source == source && edge.Target == target)
            {
                return;
            }
        }

        var line = new Line
        {
            StrokeThickness = 2,
            Stroke = new SolidColorBrush(Windows.UI.Color.FromArgb(0xCC, 0x11, 0x11, 0x11)),
            IsHitTestVisible = true
        };
        line.PointerPressed += OnConnectionPointerPressed;
        line.RightTapped += OnConnectionRightTapped;
        Canvas.SetZIndex(line, 1);
        WorkspaceCanvas.Children.Add(line);
        var edgeItem = new ConnectionEdge
        {
            Source = source,
            Target = target,
            Line = line
        };
        _connections.Add(edgeItem);
        if (select)
        {
            SelectConnection(edgeItem);
        }
        UpdateAllConnections();
        RequestCanvasAutosave();
    }

    private void RemoveConnectionsForNode(Border node)
    {
        var removedAny = false;
        for (var i = _connections.Count - 1; i >= 0; i--)
        {
            var edge = _connections[i];
            if (edge.Source != node && edge.Target != node)
            {
                continue;
            }

            if (_selectedConnection == edge)
            {
                _selectedConnection = null;
            }

            WorkspaceCanvas.Children.Remove(edge.Line);
            _connections.RemoveAt(i);
            removedAny = true;
        }

        UpdateConnectionVisuals();
        UpdateNodePropertyPanel();
        if (removedAny)
        {
            RequestCanvasAutosave();
        }
    }

    private void UpdateAllConnections()
    {
        foreach (var edge in _connections)
        {
            var start = GetNodeOutputPoint(edge.Source);
            var end = GetNodeInputPoint(edge.Target);
            edge.Line.X1 = start.X;
            edge.Line.Y1 = start.Y;
            edge.Line.X2 = end.X;
            edge.Line.Y2 = end.Y;
        }
    }

    private static Point GetNodeOutputPoint(Border node)
    {
        var left = Canvas.GetLeft(node);
        var top = Canvas.GetTop(node);
        var width = node.ActualWidth > 0 ? node.ActualWidth : node.Width;
        var height = node.ActualHeight > 0 ? node.ActualHeight : Math.Max(node.MinHeight, 96);
        return new Point(left + width, top + (height * 0.5));
    }

    private static Point GetNodeInputPoint(Border node)
    {
        var left = Canvas.GetLeft(node);
        var top = Canvas.GetTop(node);
        var height = node.ActualHeight > 0 ? node.ActualHeight : Math.Max(node.MinHeight, 96);
        return new Point(left, top + (height * 0.5));
    }

    private static Border? ResolveNodeFromSource(DependencyObject? source)
    {
        var current = source;
        while (current is not null)
        {
            if (current is Border border && border.Tag is CanvasNodeTag)
            {
                return border;
            }

            current = VisualTreeHelper.GetParent(current);
        }

        return null;
    }

    private void SelectNode(Border? node)
    {
        _selectedNode = node;
        _multiSelectedNodes.Clear();
        _selectedConnection = null;
        ApplyNodeSelectionVisuals();
        UpdateConnectionVisuals();
        UpdateNodePropertyPanel();
    }

    private void ApplyNodeSelectionVisuals()
    {
        foreach (var child in WorkspaceCanvas.Children)
        {
            if (child is not Border border || border.Tag is not CanvasNodeTag)
            {
                continue;
            }

            var isActive = border == _selectedNode || _multiSelectedNodes.Contains(border);
            border.BorderThickness = isActive ? new Thickness(2) : new Thickness(1);
            border.BorderBrush = new SolidColorBrush(
                isActive
                    ? Windows.UI.Color.FromArgb(0xFF, 0xC6, 0x28, 0x28)
                    : Windows.UI.Color.FromArgb(0x66, 0xC6, 0x28, 0x28));
        }
    }

    private void SelectConnection(ConnectionEdge? edge)
    {
        _selectedConnection = edge;
        _selectedNode = null;
        _multiSelectedNodes.Clear();
        ApplyNodeSelectionVisuals();
        UpdateConnectionVisuals();
        UpdateNodePropertyPanel();
    }

    private void UpdateConnectionVisuals()
    {
        foreach (var edge in _connections)
        {
            var isActive = edge == _selectedConnection;
            edge.Line.StrokeThickness = isActive ? 3 : 2;
            edge.Line.Stroke = new SolidColorBrush(
                isActive
                    ? Windows.UI.Color.FromArgb(0xFF, 0xC6, 0x28, 0x28)
                    : Windows.UI.Color.FromArgb(0xCC, 0x11, 0x11, 0x11));
        }
    }

    private void UpdateNodePropertyPanel()
    {
        if (_selectedConnection is not null)
        {
            CanvasSelectionInfoTextBlock.Text = "已选中连线";
            NodeTitleTextBox.Text = string.Empty;
            NodeSubtitleTextBox.Text = string.Empty;
            NodeTitleTextBox.IsEnabled = false;
            NodeSubtitleTextBox.IsEnabled = false;
            DeleteNodeButton.IsEnabled = false;
            DeleteConnectionButton.IsEnabled = true;
            return;
        }

        if (_multiSelectedNodes.Count > 0)
        {
            CanvasSelectionInfoTextBlock.Text = $"已框选：{_multiSelectedNodes.Count} 个节点";
            NodeTitleTextBox.Text = string.Empty;
            NodeSubtitleTextBox.Text = string.Empty;
            NodeTitleTextBox.IsEnabled = false;
            NodeSubtitleTextBox.IsEnabled = false;
            DeleteNodeButton.IsEnabled = true;
            DeleteConnectionButton.IsEnabled = false;
            return;
        }

        if (_selectedNode?.Tag is not CanvasNodeTag tag)
        {
            CanvasSelectionInfoTextBlock.Text = "未选中内容";
            NodeTitleTextBox.Text = string.Empty;
            NodeSubtitleTextBox.Text = string.Empty;
            NodeTitleTextBox.IsEnabled = false;
            NodeSubtitleTextBox.IsEnabled = false;
            DeleteNodeButton.IsEnabled = false;
            DeleteConnectionButton.IsEnabled = false;
            return;
        }

        CanvasSelectionInfoTextBlock.Text = $"已选中：{tag.NodeKey}";
        NodeTitleTextBox.Text = tag.TitleBlock?.Text ?? string.Empty;
        NodeSubtitleTextBox.Text = tag.SubtitleBlock?.Text ?? string.Empty;
        NodeTitleTextBox.IsEnabled = true;
        NodeSubtitleTextBox.IsEnabled = true;
        DeleteNodeButton.IsEnabled = tag.IsUserNode;
        DeleteConnectionButton.IsEnabled = false;
    }

    private void OnAddNodeClick(object sender, RoutedEventArgs e)
    {
        PrepareAddNodeFlyoutLayout();
        var viewportWidth = Math.Max(CanvasViewport.ActualWidth, 360);
        var viewportHeight = Math.Max(CanvasViewport.ActualHeight, 280);
        var popupWidth = _addNodeFlyoutRoot?.Width ?? 500;
        var popupHeight = _addNodeFlyoutRoot?.Height > 0 ? _addNodeFlyoutRoot.Height : (_addNodeFlyoutRoot?.MaxHeight ?? 560);
        var anchor = new Point(12, 12);
        if (sender is FrameworkElement element)
        {
            try
            {
                var transform = element.TransformToVisual(CanvasViewport);
                anchor = transform.TransformPoint(new Point(0, element.ActualHeight + 6));
            }
            catch
            {
                anchor = new Point(12, 12);
            }
        }

        var x = Math.Clamp(anchor.X, 8, Math.Max(8, viewportWidth - popupWidth - 8));
        var y = Math.Clamp(anchor.Y, 8, Math.Max(8, viewportHeight - popupHeight - 8));
        _addNodeFlyout.ShowAt(CanvasViewport, new Microsoft.UI.Xaml.Controls.Primitives.FlyoutShowOptions
        {
            Position = new Point(x, y)
        });
    }

    private void PrepareAddNodeFlyoutLayout()
    {
        if (_addNodeFlyoutRoot is null)
        {
            return;
        }

        var viewportWidth = Math.Max(CanvasViewport.ActualWidth, 360);
        var viewportHeight = Math.Max(CanvasViewport.ActualHeight, 280);
        var targetWidth = Math.Clamp(viewportWidth - 16, 420, 620);
        var targetHeight = Math.Clamp(viewportHeight - 16, 260, 760);
        _addNodeFlyoutRoot.Width = targetWidth;
        _addNodeFlyoutRoot.MaxHeight = targetHeight;
        _addNodeFlyoutRoot.Height = targetHeight;

        var stackWidth = Math.Max(320, targetWidth - 44);
        if (_addNodeFlyoutStack is not null)
        {
            _addNodeFlyoutStack.Width = stackWidth;
        }

        if (_addNodeFlyoutScroller is not null)
        {
            _addNodeFlyoutScroller.Width = Math.Max(320, targetWidth - 20);
            _addNodeFlyoutScroller.Height = Math.Max(140, targetHeight - 86);
        }

        for (var i = 0; i < _addNodeFlyoutGroupGrids.Count; i++)
        {
            _addNodeFlyoutGroupGrids[i].Width = stackWidth;
        }
    }

    private void OnAddNodeTemplateClick(object sender, RoutedEventArgs e)
    {
        if (sender is not FrameworkElement { Tag: NodeTemplate template })
        {
            return;
        }
        _addNodeFlyout.Hide();
        SpawnNodeFromTemplate(template);
    }

    private void SpawnNodeFromTemplate(NodeTemplate template)
    {
        var (centerX, centerY) = GetCanvasViewportCenter();
        var x = SnapToGrid(centerX - 110);
        var y = SnapToGrid(centerY - 48);

        if (Math.Abs(_lastNodeSpawnPoint.X - x) < 1 && Math.Abs(_lastNodeSpawnPoint.Y - y) < 1)
        {
            x = SnapToGrid(x + 40);
            y = SnapToGrid(y + 40);
        }

        _lastNodeSpawnPoint = new Point(x, y);
        var isBlank = string.Equals(template.KeyPrefix, "custom", StringComparison.OrdinalIgnoreCase);
        var title = isBlank ? $"节点 {_customNodeCounter++}" : template.Title;
        var subtitle = isBlank ? "请输入说明" : template.Subtitle;
        var keyPrefix = string.IsNullOrWhiteSpace(template.KeyPrefix) ? "node" : template.KeyPrefix;
        var node = CreateCanvasNode(
            $"{keyPrefix}-{Guid.NewGuid():N}",
            title,
            subtitle,
            x,
            y,
            isUserNode: true);
        WorkspaceCanvas.Children.Add(node);
        SelectNode(node);
        DismissCanvasHint();
        EnsureCanvasExtentForViewportAndNodes();
        RequestCanvasAutosave();
    }

    private (double x, double y) GetCanvasViewportCenter()
    {
        var viewportWidth = Math.Max(CanvasViewport.ActualWidth, 600);
        var viewportHeight = Math.Max(CanvasViewport.ActualHeight, 420);
        var scale = Math.Max(CanvasTransform.ScaleX, 0.001);
        var centerX = ((viewportWidth * 0.5) - CanvasTransform.TranslateX) / scale;
        var centerY = ((viewportHeight * 0.5) - CanvasTransform.TranslateY) / scale;
        return (centerX, centerY);
    }

    private void OnSaveCanvasClick(object sender, RoutedEventArgs e)
    {
        SaveCanvasSnapshot(showStatus: true);
    }

    private void OnLoadCanvasClick(object sender, RoutedEventArgs e)
    {
        TryLoadCanvasSnapshot(showStatus: true, missingIsError: true);
    }

    private void OnNewCanvasClick(object sender, RoutedEventArgs e)
    {
        CreateNewCanvas();
    }

    private void OnClearCanvasClick(object sender, RoutedEventArgs e)
    {
        CreateNewCanvas();
        SetInlineStatus("画布已清空。", InlineStatusTone.Success);
    }

    private void CreateNewCanvas()
    {
        _suppressCanvasAutosave = true;
        try
        {
            ClearCanvasWorkspaceState();
            _canvasWidth = DefaultCanvasWidth;
            _canvasHeight = DefaultCanvasHeight;
            BuildCanvasGrid();
            ResetCanvasView();
            ClampCanvasTransform();
            EnsureCanvasExtentForViewportAndNodes();
            UpdateNodePropertyPanel();
            SetInlineStatus("已新建空白画布。", InlineStatusTone.Success);
        }
        finally
        {
            _suppressCanvasAutosave = false;
        }

        SaveCanvasSnapshot(showStatus: false);
    }

    private void RequestCanvasAutosave()
    {
        if (_suppressCanvasAutosave)
        {
            return;
        }

        if (_canvasAutosaveTimer is null)
        {
            SaveCanvasSnapshot(showStatus: false);
            return;
        }

        _canvasAutosaveTimer.Stop();
        _canvasAutosaveTimer.Start();
    }

    private bool SaveCanvasSnapshot(bool showStatus)
    {
        if (_suppressCanvasAutosave)
        {
            return false;
        }

        try
        {
            var snapshot = BuildCanvasSnapshot();
            var dir = System.IO.Path.GetDirectoryName(CanvasStateFilePath) ?? ".";
            Directory.CreateDirectory(dir);
            var json = JsonSerializer.Serialize(snapshot, new JsonSerializerOptions { WriteIndented = true });
            File.WriteAllText(CanvasStateFilePath, json, Encoding.UTF8);
            if (showStatus)
            {
                SetInlineStatus($"画布已保存：{CanvasStateFilePath}", InlineStatusTone.Success);
            }
            return true;
        }
        catch (Exception ex)
        {
            if (showStatus)
            {
                SetInlineStatus($"保存画布失败：{ex.Message}", InlineStatusTone.Error);
            }
            return false;
        }
    }

    private CanvasSnapshot BuildCanvasSnapshot()
    {
        var snapshot = new CanvasSnapshot
        {
            CanvasWidth = _canvasWidth,
            CanvasHeight = _canvasHeight,
            ViewScale = Math.Clamp(CanvasTransform.ScaleX, CanvasMinScale, CanvasMaxScale),
            ViewTranslateX = CanvasTransform.TranslateX,
            ViewTranslateY = CanvasTransform.TranslateY
        };
        foreach (var node in GetCanvasNodeBorders())
        {
            if (node.Tag is not CanvasNodeTag tag)
            {
                continue;
            }

            snapshot.Nodes.Add(new CanvasNodeDto
            {
                NodeKey = tag.NodeKey,
                Title = tag.TitleBlock?.Text ?? string.Empty,
                Subtitle = tag.SubtitleBlock?.Text ?? string.Empty,
                X = Canvas.GetLeft(node),
                Y = Canvas.GetTop(node),
                IsUserNode = tag.IsUserNode
            });
        }

        var keys = snapshot.Nodes.Select(x => x.NodeKey).ToHashSet(StringComparer.Ordinal);
        foreach (var edge in _connections)
        {
            if (edge.Source.Tag is not CanvasNodeTag sourceTag || edge.Target.Tag is not CanvasNodeTag targetTag)
            {
                continue;
            }

            if (!keys.Contains(sourceTag.NodeKey) || !keys.Contains(targetTag.NodeKey))
            {
                continue;
            }

            snapshot.Edges.Add(new CanvasEdgeDto
            {
                SourceKey = sourceTag.NodeKey,
                TargetKey = targetTag.NodeKey
            });
        }

        return snapshot;
    }

    private bool TryLoadCanvasSnapshot(bool showStatus, bool missingIsError)
    {
        try
        {
            if (!File.Exists(CanvasStateFilePath))
            {
                if (showStatus || missingIsError)
                {
                    SetInlineStatus("未找到已保存的画布。", missingIsError ? InlineStatusTone.Error : InlineStatusTone.Neutral);
                }
                return false;
            }

            var json = File.ReadAllText(CanvasStateFilePath, Encoding.UTF8);
            var snapshot = JsonSerializer.Deserialize<CanvasSnapshot>(json);
            if (snapshot is null)
            {
                if (showStatus)
                {
                    SetInlineStatus("加载画布失败：文件内容为空。", InlineStatusTone.Error);
                }
                return false;
            }

            _suppressCanvasAutosave = true;
            try
            {
                ApplyCanvasSnapshot(snapshot);
            }
            finally
            {
                _suppressCanvasAutosave = false;
            }

            if (showStatus)
            {
                SetInlineStatus($"画布已加载：{CanvasStateFilePath}", InlineStatusTone.Success);
            }
            return true;
        }
        catch (Exception ex)
        {
            if (showStatus)
            {
                SetInlineStatus($"加载画布失败：{ex.Message}", InlineStatusTone.Error);
            }
            return false;
        }
    }

    private void ApplyCanvasSnapshot(CanvasSnapshot snapshot)
    {
        ClearCanvasWorkspaceState();

        var map = new Dictionary<string, Border>(StringComparer.Ordinal);
        var maxX = Math.Max(DefaultCanvasWidth, snapshot.CanvasWidth);
        var maxY = Math.Max(DefaultCanvasHeight, snapshot.CanvasHeight);
        foreach (var node in snapshot.Nodes)
        {
            if (string.IsNullOrWhiteSpace(node.NodeKey))
            {
                continue;
            }

            var x = Math.Max(0, node.X);
            var y = Math.Max(0, node.Y);
            maxX = Math.Max(maxX, x + 320);
            maxY = Math.Max(maxY, y + 220);
            var border = CreateCanvasNode(
                node.NodeKey,
                string.IsNullOrWhiteSpace(node.Title) ? "节点" : node.Title,
                node.Subtitle ?? string.Empty,
                x,
                y,
                isUserNode: node.IsUserNode);
            WorkspaceCanvas.Children.Add(border);
            map[node.NodeKey] = border;
        }

        _canvasWidth = Math.Ceiling(Math.Max(DefaultCanvasWidth, maxX) / CanvasGridSize) * CanvasGridSize;
        _canvasHeight = Math.Ceiling(Math.Max(DefaultCanvasHeight, maxY) / CanvasGridSize) * CanvasGridSize;
        BuildCanvasGrid();

        foreach (var edge in snapshot.Edges)
        {
            if (string.IsNullOrWhiteSpace(edge.SourceKey) || string.IsNullOrWhiteSpace(edge.TargetKey))
            {
                continue;
            }

            if (!map.TryGetValue(edge.SourceKey, out var source) || !map.TryGetValue(edge.TargetKey, out var target))
            {
                continue;
            }

            AddConnection(source, target, select: false);
        }

        var userNodes = snapshot.Nodes.Count(x => x.IsUserNode);
        _customNodeCounter = Math.Max(1, userNodes + 1);
        UpdateAllConnections();
        CanvasTransform.ScaleX = Math.Clamp(snapshot.ViewScale, CanvasMinScale, CanvasMaxScale);
        CanvasTransform.ScaleY = Math.Clamp(snapshot.ViewScale, CanvasMinScale, CanvasMaxScale);
        CanvasTransform.TranslateX = snapshot.ViewTranslateX;
        CanvasTransform.TranslateY = snapshot.ViewTranslateY;
        ClampCanvasTransform();
        UpdateNodePropertyPanel();
        EnsureCanvasExtentForViewportAndNodes();
    }

    private void ClearCanvasWorkspaceState()
    {
        CancelConnectionPreview();
        _connections.Clear();
        _artifactNodes.Clear();
        _selectedConnection = null;
        _selectedNode = null;
        _inputNode = null;
        _cleanNode = null;
        _outputNode = null;
        _customNodeCounter = 1;
        WorkspaceCanvas.Children.Clear();
        _canvasAutosaveTimer?.Stop();
    }

    private IEnumerable<Border> GetCanvasNodeBorders()
    {
        foreach (var child in WorkspaceCanvas.Children)
        {
            if (child is Border border && border.Tag is CanvasNodeTag)
            {
                yield return border;
            }
        }
    }

    private void OnWorkspaceCanvasRightTapped(object sender, RightTappedRoutedEventArgs e)
    {
        if (IsCanvasNodeSource(e.OriginalSource as DependencyObject) || e.OriginalSource is Line)
        {
            return;
        }

        _contextNode = null;
        _contextConnection = null;
        _contextCanvasPoint = e.GetPosition(WorkspaceCanvas);
        _canvasBlankFlyout.ShowAt(CanvasViewport, new Microsoft.UI.Xaml.Controls.Primitives.FlyoutShowOptions
        {
            Position = e.GetPosition(CanvasViewport)
        });
        e.Handled = true;
    }

    private void OnCanvasNodeRightTapped(object sender, RightTappedRoutedEventArgs e)
    {
        if (sender is not Border node)
        {
            return;
        }

        _contextConnection = null;
        _contextNode = node;
        _contextCanvasPoint = e.GetPosition(WorkspaceCanvas);
        SelectNode(node);
        _canvasNodeFlyout.ShowAt(CanvasViewport, new Microsoft.UI.Xaml.Controls.Primitives.FlyoutShowOptions
        {
            Position = e.GetPosition(CanvasViewport)
        });
        e.Handled = true;
    }

    private void OnConnectionRightTapped(object sender, RightTappedRoutedEventArgs e)
    {
        if (sender is not Line line)
        {
            return;
        }

        var edge = _connections.FirstOrDefault(x => x.Line == line);
        if (edge is null)
        {
            return;
        }

        _contextNode = null;
        _contextConnection = edge;
        _contextCanvasPoint = e.GetPosition(WorkspaceCanvas);
        SelectConnection(edge);
        _canvasConnectionFlyout.ShowAt(CanvasViewport, new Microsoft.UI.Xaml.Controls.Primitives.FlyoutShowOptions
        {
            Position = e.GetPosition(CanvasViewport)
        });
        e.Handled = true;
    }

    private void OnCenterViewHereClick(object sender, RoutedEventArgs e)
    {
        CenterCanvasViewOn(_contextCanvasPoint);
    }

    private void CenterCanvasViewOn(Point targetCanvasPoint)
    {
        var scale = Math.Max(CanvasTransform.ScaleX, 0.001);
        var viewportWidth = Math.Max(CanvasViewport.ActualWidth, 1);
        var viewportHeight = Math.Max(CanvasViewport.ActualHeight, 1);
        CanvasTransform.TranslateX = (viewportWidth * 0.5) - (targetCanvasPoint.X * scale);
        CanvasTransform.TranslateY = (viewportHeight * 0.5) - (targetCanvasPoint.Y * scale);
        ClampCanvasTransform();
    }

    private void OnCopyNodeClick(object sender, RoutedEventArgs e)
    {
        if (_contextNode?.Tag is not CanvasNodeTag tag)
        {
            return;
        }

        _copiedNodeTemplate = new CanvasNodeDto
        {
            NodeKey = tag.NodeKey,
            Title = tag.TitleBlock?.Text ?? string.Empty,
            Subtitle = tag.SubtitleBlock?.Text ?? string.Empty,
            IsUserNode = true
        };
        SetInlineStatus("已复制节点。", InlineStatusTone.Success);
    }

    private void OnPasteNodeClick(object sender, RoutedEventArgs e)
    {
        if (_copiedNodeTemplate is null)
        {
            return;
        }

        var title = string.IsNullOrWhiteSpace(_copiedNodeTemplate.Title) ? $"节点 {_customNodeCounter}" : _copiedNodeTemplate.Title;
        var subtitle = _copiedNodeTemplate.Subtitle ?? string.Empty;
        var node = CreateCanvasNode(
            $"node-{Guid.NewGuid():N}",
            title,
            subtitle,
            Math.Max(0, SnapToGrid(_contextCanvasPoint.X - 110)),
            Math.Max(0, SnapToGrid(_contextCanvasPoint.Y - 48)),
            isUserNode: true);
        _customNodeCounter++;
        WorkspaceCanvas.Children.Add(node);
        SelectNode(node);
        DismissCanvasHint();
        EnsureCanvasExtentForViewportAndNodes();
        RequestCanvasAutosave();
        SetInlineStatus("已粘贴节点。", InlineStatusTone.Success);
    }

    private void OnDeleteNodeFromContextClick(object sender, RoutedEventArgs e)
    {
        if (_contextNode is null)
        {
            return;
        }

        SelectNode(_contextNode);
        DeleteSelectedUserNode();
    }

    private void OnDeleteConnectionFromContextClick(object sender, RoutedEventArgs e)
    {
        if (_contextConnection is null)
        {
            return;
        }

        SelectConnection(_contextConnection);
        DeleteSelectedConnection();
    }

    private void OnDeleteNodeClick(object sender, RoutedEventArgs e)
    {
        DeleteSelectedUserNode();
    }

    private void OnDeleteConnectionClick(object sender, RoutedEventArgs e)
    {
        DeleteSelectedConnection();
    }

    private void OnNodeTitleChanged(object sender, TextChangedEventArgs e)
    {
        if (_selectedNode?.Tag is not CanvasNodeTag tag || tag.TitleBlock is null)
        {
            return;
        }

        tag.TitleBlock.Text = NodeTitleTextBox.Text.Trim();
        RequestCanvasAutosave();
    }

    private void OnNodeSubtitleChanged(object sender, TextChangedEventArgs e)
    {
        if (_selectedNode?.Tag is not CanvasNodeTag tag || tag.SubtitleBlock is null)
        {
            return;
        }

        tag.SubtitleBlock.Text = NodeSubtitleTextBox.Text.Trim();
        RequestCanvasAutosave();
    }

    private void OnCanvasNodePointerPressed(object sender, PointerRoutedEventArgs e)
    {
        if (sender is not Border node)
        {
            return;
        }
        if (e.OriginalSource is Ellipse && (e.OriginalSource as FrameworkElement)?.Tag is ConnectorTag)
        {
            return;
        }

        SelectNode(node);
        _draggingNode = node;
        _draggingNodePointerId = e.Pointer.PointerId;
        _dragStartPoint = e.GetCurrentPoint(WorkspaceCanvas).Position;
        _dragStartNodeLeft = Canvas.GetLeft(node);
        _dragStartNodeTop = Canvas.GetTop(node);
        _nodeMovedDuringDrag = false;
        SetCanvasManipulationEnabled(false);
        node.CapturePointer(e.Pointer);
        DismissCanvasHint();
        e.Handled = true;
    }

    private void OnCanvasNodePointerMoved(object sender, PointerRoutedEventArgs e)
    {
        if (_draggingNode is null || sender is not Border node || _draggingNodePointerId != e.Pointer.PointerId)
        {
            return;
        }

        var point = e.GetCurrentPoint(WorkspaceCanvas).Position;
        var dx = point.X - _dragStartPoint.X;
        var dy = point.Y - _dragStartPoint.Y;
        if (Math.Abs(dx) > 2 || Math.Abs(dy) > 2)
        {
            _nodeMovedDuringDrag = true;
        }

        var left = Math.Max(0, _dragStartNodeLeft + dx);
        var top = Math.Max(0, _dragStartNodeTop + dy);
        Canvas.SetLeft(node, left);
        Canvas.SetTop(node, top);
        UpdateAllConnections();
        e.Handled = true;
    }

    private void OnCanvasNodePointerReleased(object sender, PointerRoutedEventArgs e)
    {
        if (_draggingNode is null || sender is not Border node || _draggingNodePointerId != e.Pointer.PointerId)
        {
            return;
        }

        node.ReleasePointerCapture(e.Pointer);
        // Snap only when drag ends to keep movement smooth.
        Canvas.SetLeft(node, Math.Max(0, SnapToGrid(Canvas.GetLeft(node))));
        Canvas.SetTop(node, Math.Max(0, SnapToGrid(Canvas.GetTop(node))));
        UpdateAllConnections();
        EnsureCanvasExtentForViewportAndNodes();
        if (!_nodeMovedDuringDrag)
        {
            TryOpenArtifactFromNode(node);
        }
        else
        {
            RequestCanvasAutosave();
        }

        _draggingNode = null;
        _draggingNodePointerId = 0;
        _nodeMovedDuringDrag = false;
        SetCanvasManipulationEnabled(true);
        e.Handled = true;
    }

    private void OnCanvasPointerPressed(object sender, PointerRoutedEventArgs e)
    {
        if (_isCreatingConnection)
        {
            return;
        }

        if (_draggingNode is not null || IsCanvasNodeSource(e.OriginalSource as DependencyObject))
        {
            return;
        }

        var point = e.GetCurrentPoint(CanvasViewport);
        var properties = point.Properties;
        var deviceType = point.PointerDeviceType;
        if (!properties.IsLeftButtonPressed && !properties.IsMiddleButtonPressed && !properties.IsRightButtonPressed)
        {
            return;
        }

        var isTouch = string.Equals(deviceType.ToString(), "Touch", StringComparison.OrdinalIgnoreCase);
        if (isTouch)
        {
            // Touch pan/zoom is handled by manipulation events.
            return;
        }

        var spacePressed = IsSpacePressed();
        if (properties.IsLeftButtonPressed && !properties.IsMiddleButtonPressed && !properties.IsRightButtonPressed)
        {
            if (spacePressed)
            {
                BeginCanvasPanning(e);
                return;
            }

            BeginMarqueeSelection(e);
            e.Handled = true;
            return;
        }

        if (properties.IsMiddleButtonPressed)
        {
            BeginCanvasPanning(e);
            return;
        }

        if (properties.IsRightButtonPressed)
        {
            SelectConnection(null);
        }
    }

    private bool IsSpacePressed()
    {
        var state = InputKeyboardSource.GetKeyStateForCurrentThread(Windows.System.VirtualKey.Space);
        var isDown = (state & Windows.UI.Core.CoreVirtualKeyStates.Down) == Windows.UI.Core.CoreVirtualKeyStates.Down;
        if (!isDown && _isSpaceHeld)
        {
            // Recover from missed KeyUp when focus moved.
            _isSpaceHeld = false;
        }

        return isDown || _isSpaceHeld;
    }

    private bool IsSpaceClickSuppressed()
    {
        if (_activeSection != NavSection.Canvas)
        {
            return false;
        }

        var elapsed = Environment.TickCount64 - _lastSpaceKeyTickMs;
        return elapsed >= 0 && elapsed <= 220;
    }

    private void SetCanvasManipulationEnabled(bool enabled)
    {
        CanvasViewport.ManipulationMode = enabled
            ? ManipulationModes.TranslateX
              | ManipulationModes.TranslateY
              | ManipulationModes.Scale
              | ManipulationModes.TranslateInertia
              | ManipulationModes.ScaleInertia
            : ManipulationModes.None;
    }

    private void BeginCanvasPanning(PointerRoutedEventArgs e)
    {
        SelectConnection(null);
        _isCanvasPanning = true;
        _isPointerPanningMode = true;
        _panStartPoint = e.GetCurrentPoint(CanvasViewport).Position;
        _panStartTranslateX = CanvasTransform.TranslateX;
        _panStartTranslateY = CanvasTransform.TranslateY;
        SetCanvasManipulationEnabled(false);
        CanvasViewport.CapturePointer(e.Pointer);
        BeginCanvasViewportInteraction();
        DismissCanvasHint();
        e.Handled = true;
    }

    private void OnCanvasPointerMoved(object sender, PointerRoutedEventArgs e)
    {
        if (_isCreatingConnection)
        {
            if (_connectionPreviewLine is not null && _connectionSourceNode is not null && e.Pointer.PointerId == _connectionPointerId)
            {
                var start = GetNodeOutputPoint(_connectionSourceNode);
                var end = e.GetCurrentPoint(WorkspaceCanvas).Position;
                _connectionPreviewLine.X1 = start.X;
                _connectionPreviewLine.Y1 = start.Y;
                _connectionPreviewLine.X2 = end.X;
                _connectionPreviewLine.Y2 = end.Y;
                e.Handled = true;
            }
            return;
        }

        if (_isMarqueeSelecting && e.Pointer.PointerId == _marqueePointerId)
        {
            UpdateMarqueeSelection(e.GetCurrentPoint(CanvasViewport).Position);
            e.Handled = true;
            return;
        }

        if (!_isCanvasPanning)
        {
            return;
        }

        var point = e.GetCurrentPoint(CanvasViewport).Position;
        var dx = point.X - _panStartPoint.X;
        var dy = point.Y - _panStartPoint.Y;
        CanvasTransform.TranslateX = _panStartTranslateX + dx;
        CanvasTransform.TranslateY = _panStartTranslateY + dy;
        e.Handled = true;
    }

    private void OnCanvasPointerReleased(object sender, PointerRoutedEventArgs e)
    {
        if (_isCreatingConnection && e.Pointer.PointerId == _connectionPointerId)
        {
            TryCompleteConnection(e);
            CanvasViewport.ReleasePointerCaptures();
            return;
        }

        if (_isMarqueeSelecting && e.Pointer.PointerId == _marqueePointerId)
        {
            EndMarqueeSelection();
            e.Handled = true;
            return;
        }

        if (!_isCanvasPanning)
        {
            return;
        }

        _isCanvasPanning = false;
        _isPointerPanningMode = false;
        CanvasViewport.ReleasePointerCaptures();
        SetCanvasManipulationEnabled(true);
        EndCanvasViewportInteraction();
        e.Handled = true;
    }

    private void BeginMarqueeSelection(PointerRoutedEventArgs e)
    {
        _isMarqueeSelecting = true;
        _isCanvasPanning = false;
        _isPointerPanningMode = false;
        _marqueePointerId = e.Pointer.PointerId;
        _marqueeStartPoint = e.GetCurrentPoint(CanvasViewport).Position;
        _selectedConnection = null;
        _selectedNode = null;
        _multiSelectedNodes.Clear();
        ApplyNodeSelectionVisuals();
        UpdateConnectionVisuals();
        UpdateNodePropertyPanel();
        CanvasMarqueeSelectionBox.Visibility = Visibility.Visible;
        CanvasMarqueeSelectionBox.Margin = new Thickness(_marqueeStartPoint.X, _marqueeStartPoint.Y, 0, 0);
        CanvasMarqueeSelectionBox.Width = 0;
        CanvasMarqueeSelectionBox.Height = 0;
        SetCanvasManipulationEnabled(false);
        CanvasViewport.CapturePointer(e.Pointer);
        DismissCanvasHint();
    }

    private void UpdateMarqueeSelection(Point currentViewportPoint)
    {
        var left = Math.Min(_marqueeStartPoint.X, currentViewportPoint.X);
        var top = Math.Min(_marqueeStartPoint.Y, currentViewportPoint.Y);
        var width = Math.Abs(currentViewportPoint.X - _marqueeStartPoint.X);
        var height = Math.Abs(currentViewportPoint.Y - _marqueeStartPoint.Y);

        CanvasMarqueeSelectionBox.Margin = new Thickness(left, top, 0, 0);
        CanvasMarqueeSelectionBox.Width = width;
        CanvasMarqueeSelectionBox.Height = height;

        var scale = Math.Max(CanvasTransform.ScaleX, 0.001);
        var contentLeft = (left - CanvasTransform.TranslateX) / scale;
        var contentTop = (top - CanvasTransform.TranslateY) / scale;
        var contentRight = ((left + width) - CanvasTransform.TranslateX) / scale;
        var contentBottom = ((top + height) - CanvasTransform.TranslateY) / scale;

        var rect = new Rect(contentLeft, contentTop, Math.Max(0, contentRight - contentLeft), Math.Max(0, contentBottom - contentTop));
        _multiSelectedNodes.Clear();
        foreach (var node in GetCanvasNodeBorders())
        {
            if (node.Tag is not CanvasNodeTag tag || !tag.IsUserNode)
            {
                continue;
            }

            var nodeLeft = Canvas.GetLeft(node);
            var nodeTop = Canvas.GetTop(node);
            var nodeWidth = node.ActualWidth > 1 ? node.ActualWidth : node.Width;
            var nodeHeight = node.ActualHeight > 1 ? node.ActualHeight : Math.Max(node.MinHeight, 96);
            var nodeRect = new Rect(nodeLeft, nodeTop, Math.Max(1, nodeWidth), Math.Max(1, nodeHeight));
            if (RectIntersects(nodeRect, rect))
            {
                _multiSelectedNodes.Add(node);
            }
        }

        ApplyNodeSelectionVisuals();
        UpdateNodePropertyPanel();
    }

    private static bool RectIntersects(Rect a, Rect b)
    {
        var aRight = a.X + a.Width;
        var aBottom = a.Y + a.Height;
        var bRight = b.X + b.Width;
        var bBottom = b.Y + b.Height;
        return a.X < bRight && aRight > b.X && a.Y < bBottom && aBottom > b.Y;
    }

    private void EndMarqueeSelection()
    {
        _isMarqueeSelecting = false;
        _marqueePointerId = 0;
        CanvasViewport.ReleasePointerCaptures();
        SetCanvasManipulationEnabled(true);
        CanvasMarqueeSelectionBox.Visibility = Visibility.Collapsed;
        CanvasMarqueeSelectionBox.Width = 0;
        CanvasMarqueeSelectionBox.Height = 0;
        if (_multiSelectedNodes.Count > 0)
        {
            SetInlineStatus($"已框选 {_multiSelectedNodes.Count} 个节点，按 Delete 可删除。", InlineStatusTone.Success);
        }
    }

    private void OnCanvasPointerWheelChanged(object sender, PointerRoutedEventArgs e)
    {
        var delta = e.GetCurrentPoint(CanvasViewport).Properties.MouseWheelDelta;
        if (delta == 0)
        {
            return;
        }

        var factor = delta > 0 ? 1.1 : 0.9;
        var center = e.GetCurrentPoint(CanvasViewport).Position;
        BeginCanvasViewportInteraction();
        ApplyCanvasScale(center, factor);
        EndCanvasViewportInteraction();
        DismissCanvasHint();
        e.Handled = true;
    }

    private void OnCanvasManipulationStarted(object sender, ManipulationStartedRoutedEventArgs e)
    {
        if (_isMarqueeSelecting || _draggingNode is not null || _isCreatingConnection || _isPointerPanningMode)
        {
            e.Handled = true;
            return;
        }

        BeginCanvasViewportInteraction();
        DismissCanvasHint();
    }

    private void OnCanvasManipulationDelta(object sender, ManipulationDeltaRoutedEventArgs e)
    {
        if (_isMarqueeSelecting || _draggingNode is not null || _isCreatingConnection || _isPointerPanningMode)
        {
            e.Handled = true;
            return;
        }

        if (e.Delta.Scale != 0 && Math.Abs(e.Delta.Scale - 1) > 0.001)
        {
            ApplyCanvasScale(e.Position, e.Delta.Scale);
        }

        CanvasTransform.TranslateX += e.Delta.Translation.X;
        CanvasTransform.TranslateY += e.Delta.Translation.Y;
        e.Handled = true;
    }

    private void OnCanvasManipulationCompleted(object sender, ManipulationCompletedRoutedEventArgs e)
    {
        if (_isMarqueeSelecting || _draggingNode is not null || _isCreatingConnection || _isPointerPanningMode)
        {
            e.Handled = true;
            return;
        }

        EndCanvasViewportInteraction();
    }

    private void ApplyCanvasScale(Point center, double scaleFactor)
    {
        var oldScale = CanvasTransform.ScaleX;
        var newScale = Math.Clamp(oldScale * scaleFactor, CanvasMinScale, CanvasMaxScale);
        if (Math.Abs(newScale - oldScale) < 0.0001)
        {
            return;
        }

        var contentX = (center.X - CanvasTransform.TranslateX) / oldScale;
        var contentY = (center.Y - CanvasTransform.TranslateY) / oldScale;
        CanvasTransform.ScaleX = newScale;
        CanvasTransform.ScaleY = newScale;
        CanvasTransform.TranslateX = center.X - (contentX * newScale);
        CanvasTransform.TranslateY = center.Y - (contentY * newScale);
        ClampCanvasTransform();
        UpdateCanvasZoomIndicator();
    }

    private void OnCanvasZoomOutClick(object sender, RoutedEventArgs e)
    {
        var center = new Point(Math.Max(CanvasViewport.ActualWidth, 1) * 0.5, Math.Max(CanvasViewport.ActualHeight, 1) * 0.5);
        ApplyCanvasScale(center, 0.9);
        DismissCanvasHint();
    }

    private void OnCanvasZoomInClick(object sender, RoutedEventArgs e)
    {
        var center = new Point(Math.Max(CanvasViewport.ActualWidth, 1) * 0.5, Math.Max(CanvasViewport.ActualHeight, 1) * 0.5);
        ApplyCanvasScale(center, 1.1);
        DismissCanvasHint();
    }

    private void OnCanvasFitClick(object sender, RoutedEventArgs e)
    {
        FitCanvasToNodes();
        DismissCanvasHint();
    }

    private void OnCanvasZoomPercentTapped(object sender, TappedRoutedEventArgs e)
    {
        BeginZoomInlineEdit();
        e.Handled = true;
    }

    private void OnCanvasZoomEditTextBoxKeyDown(object sender, KeyRoutedEventArgs e)
    {
        if (e.Key == Windows.System.VirtualKey.Enter)
        {
            CommitZoomInlineEdit(applyChange: true);
            e.Handled = true;
            return;
        }

        if (e.Key == Windows.System.VirtualKey.Escape)
        {
            CommitZoomInlineEdit(applyChange: false);
            e.Handled = true;
        }
    }

    private void OnCanvasZoomEditTextBoxLostFocus(object sender, RoutedEventArgs e)
    {
        if (!_isZoomInlineEditing)
        {
            return;
        }

        CommitZoomInlineEdit(applyChange: true);
    }

    private void BeginZoomInlineEdit()
    {
        if (_isZoomInlineEditing)
        {
            return;
        }

        _isZoomInlineEditing = true;
        var currentPercent = Math.Round(Math.Max(CanvasTransform.ScaleX, 0.001) * 100);
        CanvasZoomTextBlock.Visibility = Visibility.Collapsed;
        CanvasZoomEditTextBox.Visibility = Visibility.Visible;
        CanvasZoomEditTextBox.Text = currentPercent.ToString("0");
        CanvasZoomEditTextBox.Focus(FocusState.Programmatic);
        CanvasZoomEditTextBox.SelectAll();
    }

    private void CommitZoomInlineEdit(bool applyChange)
    {
        if (!_isZoomInlineEditing)
        {
            return;
        }

        _isZoomInlineEditing = false;
        CanvasZoomEditTextBox.Visibility = Visibility.Collapsed;
        CanvasZoomTextBlock.Visibility = Visibility.Visible;

        if (!applyChange)
        {
            UpdateCanvasZoomIndicator();
            return;
        }

        var text = (CanvasZoomEditTextBox.Text ?? string.Empty).Trim().TrimEnd('%').Trim();
        if (!double.TryParse(text, out var percent) || double.IsNaN(percent) || double.IsInfinity(percent))
        {
            UpdateCanvasZoomIndicator();
            SetInlineStatus("请输入有效缩放值（60-240）。", InlineStatusTone.Error);
            return;
        }

        ApplyZoomPercent(percent);
    }

    private void ApplyZoomPercent(double percent)
    {
        var targetScale = Math.Clamp(percent / 100.0, CanvasMinScale, CanvasMaxScale);
        var center = new Point(
            Math.Max(CanvasViewport.ActualWidth, 1) * 0.5,
            Math.Max(CanvasViewport.ActualHeight, 1) * 0.5);
        var currentScale = Math.Max(CanvasTransform.ScaleX, 0.001);
        var factor = targetScale / currentScale;
        BeginCanvasViewportInteraction();
        ApplyCanvasScale(center, factor);
        EndCanvasViewportInteraction();
        DismissCanvasHint();
        SetInlineStatus($"缩放已设置为 {Math.Round(targetScale * 100):0}%。", InlineStatusTone.Success);
    }

    private void FitCanvasToNodes()
    {
        var allNodes = GetCanvasNodeBorders().ToList();
        var userNodes = allNodes
            .Where(x => x.Tag is CanvasNodeTag tag && tag.IsUserNode)
            .ToList();
        var nodes = userNodes.Count > 0 ? userNodes : allNodes;
        if (nodes.Count == 0)
        {
            ResetCanvasView();
            UpdateCanvasZoomIndicator();
            SetInlineStatus("画布为空，已重置视图。", InlineStatusTone.Neutral);
            return;
        }

        // Ignore extreme outliers so accidental far-away nodes don't fling the camera.
        if (nodes.Count >= 4)
        {
            var centersX = nodes.Select(n =>
            {
                var l = Canvas.GetLeft(n);
                var w = n.ActualWidth > 0 ? n.ActualWidth : n.Width;
                return l + (Math.Max(w, 1) * 0.5);
            }).OrderBy(v => v).ToList();
            var centersY = nodes.Select(n =>
            {
                var t = Canvas.GetTop(n);
                var h = n.ActualHeight > 0 ? n.ActualHeight : Math.Max(n.MinHeight, 96);
                return t + (Math.Max(h, 1) * 0.5);
            }).OrderBy(v => v).ToList();
            var lowIndex = (int)Math.Floor((centersX.Count - 1) * 0.1);
            var highIndex = (int)Math.Ceiling((centersX.Count - 1) * 0.9);
            var minCx = centersX[Math.Clamp(lowIndex, 0, centersX.Count - 1)];
            var maxCx = centersX[Math.Clamp(highIndex, 0, centersX.Count - 1)];
            var minCy = centersY[Math.Clamp(lowIndex, 0, centersY.Count - 1)];
            var maxCy = centersY[Math.Clamp(highIndex, 0, centersY.Count - 1)];
            var filtered = nodes.Where(n =>
            {
                var l = Canvas.GetLeft(n);
                var t = Canvas.GetTop(n);
                var w = n.ActualWidth > 0 ? n.ActualWidth : n.Width;
                var h = n.ActualHeight > 0 ? n.ActualHeight : Math.Max(n.MinHeight, 96);
                var cx = l + (Math.Max(w, 1) * 0.5);
                var cy = t + (Math.Max(h, 1) * 0.5);
                return cx >= minCx && cx <= maxCx && cy >= minCy && cy <= maxCy;
            }).ToList();
            if (filtered.Count >= 2)
            {
                nodes = filtered;
            }
        }

        var minLeft = double.MaxValue;
        var minTop = double.MaxValue;
        var maxRight = 0.0;
        var maxBottom = 0.0;
        foreach (var node in nodes)
        {
            var left = Canvas.GetLeft(node);
            var top = Canvas.GetTop(node);
            var width = node.ActualWidth > 0 ? node.ActualWidth : node.Width;
            var height = node.ActualHeight > 0 ? node.ActualHeight : Math.Max(node.MinHeight, 96);
            minLeft = Math.Min(minLeft, left);
            minTop = Math.Min(minTop, top);
            maxRight = Math.Max(maxRight, left + width);
            maxBottom = Math.Max(maxBottom, top + height);
        }

        var contentWidth = Math.Max(1.0, maxRight - minLeft);
        var contentHeight = Math.Max(1.0, maxBottom - minTop);
        var viewportWidth = Math.Max(CanvasViewport.ActualWidth, 1.0);
        var viewportHeight = Math.Max(CanvasViewport.ActualHeight, 1.0);
        const double padding = 64;
        var availableWidth = Math.Max(80.0, viewportWidth - (padding * 2));
        var availableHeight = Math.Max(80.0, viewportHeight - (padding * 2));
        var scaleX = availableWidth / contentWidth;
        var scaleY = availableHeight / contentHeight;
        var targetScale = Math.Clamp(Math.Min(scaleX, scaleY), CanvasMinScale, CanvasMaxScale);
        CanvasTransform.ScaleX = targetScale;
        CanvasTransform.ScaleY = targetScale;
        var centerX = minLeft + (contentWidth * 0.5);
        var centerY = minTop + (contentHeight * 0.5);
        CanvasTransform.TranslateX = (viewportWidth * 0.5) - (centerX * targetScale);
        CanvasTransform.TranslateY = (viewportHeight * 0.5) - (centerY * targetScale);
        UpdateCanvasZoomIndicator();
        SetInlineStatus("已定位至节点流。", InlineStatusTone.Success);
    }

    private void UpdateCanvasZoomIndicator()
    {
        if (CanvasZoomTextBlock is null)
        {
            return;
        }

        var scale = Math.Max(CanvasTransform.ScaleX, 0.001);
        CanvasZoomTextBlock.Text = $"{Math.Round(scale * 100):0}%";
    }

    private void BeginCanvasViewportInteraction()
    {
        _canvasInteractionSettleTimer?.Stop();
        CanvasGridLayer.Visibility = Visibility.Collapsed;
    }

    private void EndCanvasViewportInteraction()
    {
        if (_canvasInteractionSettleTimer is null)
        {
            CanvasGridLayer.Visibility = Visibility.Visible;
            return;
        }

        _canvasInteractionSettleTimer.Stop();
        _canvasInteractionSettleTimer.Start();
    }

    private static bool IsCanvasNodeSource(DependencyObject? source)
    {
        return ResolveNodeFromSource(source) is not null;
    }

    private void OnConnectionPointerPressed(object sender, PointerRoutedEventArgs e)
    {
        if (sender is not Line line)
        {
            return;
        }

        var edge = _connections.FirstOrDefault(x => x.Line == line);
        if (edge is null)
        {
            return;
        }

        SelectConnection(edge);
        e.Handled = true;
    }

    private void TryOpenArtifactFromNode(Border node)
    {
        if (node.Tag is not CanvasNodeTag tag)
        {
            return;
        }

        if (string.IsNullOrWhiteSpace(tag.ArtifactPath) || !File.Exists(tag.ArtifactPath))
        {
            return;
        }

        try
        {
            Process.Start(new ProcessStartInfo
            {
                FileName = tag.ArtifactPath,
                UseShellExecute = true
            });
            SetInlineStatus("已打开产物文件。", InlineStatusTone.Success);
        }
        catch (Exception ex)
        {
            SetInlineStatus($"打开文件失败：{ex.Message}", InlineStatusTone.Error);
        }
    }

    private static double SnapToGrid(double value)
    {
        return Math.Round(value / CanvasGridSize) * CanvasGridSize;
    }

    private void DismissCanvasHint()
    {
        CanvasHintPanel.Visibility = Visibility.Collapsed;
    }

    private void OnCanvasSplitHandlePointerPressed(object sender, PointerRoutedEventArgs e)
    {
        if (_isCanvasStacked)
        {
            return;
        }

        _isResizingCanvasPanels = true;
        _resizePointerId = e.Pointer.PointerId;
        _resizeStartX = e.GetCurrentPoint(CanvasSplitLayoutGrid).Position.X;
        _resizeStartLeftWidth = CanvasLeftColumn.ActualWidth;
        _resizeStartRightWidth = CanvasRightColumn.ActualWidth;
        CanvasSplitGrip.CapturePointer(e.Pointer);
        BeginSplitResizeInteraction();
        e.Handled = true;
    }

    private void OnCanvasSplitHandlePointerMoved(object sender, PointerRoutedEventArgs e)
    {
        if (!_isResizingCanvasPanels || e.Pointer.PointerId != _resizePointerId)
        {
            return;
        }

        var currentX = e.GetCurrentPoint(CanvasSplitLayoutGrid).Position.X;
        var delta = currentX - _resizeStartX;
        var total = _resizeStartLeftWidth + _resizeStartRightWidth;
        var minLeft = Math.Max(120, CanvasLeftColumn.MinWidth);
        var minRight = Math.Max(120, CanvasRightColumn.MinWidth);
        var maxLeft = total - minRight;
        if (maxLeft < minLeft)
        {
            maxLeft = minLeft;
        }

        var left = Math.Clamp(_resizeStartLeftWidth + delta, minLeft, maxLeft);
        var right = Math.Max(minRight, total - left);
        if (right <= 0 || left <= 0)
        {
            return;
        }

        _pendingLeftWidth = left;
        _pendingRightWidth = right;
        _hasPendingPanelSplit = true;
        CanvasSplitGripTransform.TranslateX = left - _resizeStartLeftWidth;
        e.Handled = true;
    }

    private void OnCanvasSplitHandlePointerReleased(object sender, PointerRoutedEventArgs e)
    {
        if (!_isResizingCanvasPanels || e.Pointer.PointerId != _resizePointerId)
        {
            return;
        }

        _isResizingCanvasPanels = false;
        _resizePointerId = 0;
        CanvasSplitGrip.ReleasePointerCapture(e.Pointer);
        ApplyPendingSplitResize();
        CanvasSplitGripTransform.TranslateX = 0;
        EndSplitResizeInteraction();
        e.Handled = true;
    }

    private void OnCanvasStackSplitHandlePointerPressed(object sender, PointerRoutedEventArgs e)
    {
        if (!_isCanvasStacked)
        {
            return;
        }

        _isResizingCanvasRows = true;
        _resizeRowPointerId = e.Pointer.PointerId;
        _resizeStartY = e.GetCurrentPoint(CanvasSplitLayoutGrid).Position.Y;
        _resizeStartTopHeight = CanvasLayoutRow0.ActualHeight;
        _resizeStartBottomHeight = CanvasLayoutRow2.ActualHeight;
        CanvasStackSplitGrip.CapturePointer(e.Pointer);
        BeginSplitResizeInteraction();
        e.Handled = true;
    }

    private void OnCanvasStackSplitHandlePointerMoved(object sender, PointerRoutedEventArgs e)
    {
        if (!_isResizingCanvasRows || e.Pointer.PointerId != _resizeRowPointerId)
        {
            return;
        }

        var currentY = e.GetCurrentPoint(CanvasSplitLayoutGrid).Position.Y;
        var delta = currentY - _resizeStartY;
        var total = _resizeStartTopHeight + _resizeStartBottomHeight;
        const double minTop = 240;
        const double minBottom = 200;
        var maxTop = total - minBottom;
        if (maxTop < minTop)
        {
            maxTop = minTop;
        }

        var top = Math.Clamp(_resizeStartTopHeight + delta, minTop, maxTop);
        var bottom = Math.Max(minBottom, total - top);
        if (top <= 0 || bottom <= 0)
        {
            return;
        }

        _pendingTopHeight = top;
        _pendingBottomHeight = bottom;
        _hasPendingRowSplit = true;
        CanvasStackSplitGripTransform.TranslateY = top - _resizeStartTopHeight;
        e.Handled = true;
    }

    private void OnCanvasStackSplitHandlePointerReleased(object sender, PointerRoutedEventArgs e)
    {
        if (!_isResizingCanvasRows || e.Pointer.PointerId != _resizeRowPointerId)
        {
            return;
        }

        _isResizingCanvasRows = false;
        _resizeRowPointerId = 0;
        CanvasStackSplitGrip.ReleasePointerCapture(e.Pointer);
        ApplyPendingSplitResize();
        CanvasStackSplitGripTransform.TranslateY = 0;
        EndSplitResizeInteraction();
        e.Handled = true;
    }

    private void BeginSplitResizeInteraction()
    {
        _hasPendingPanelSplit = false;
        _hasPendingRowSplit = false;
        CanvasGridLayer.Visibility = Visibility.Collapsed;
    }

    private void EndSplitResizeInteraction()
    {
        CanvasGridLayer.Visibility = Visibility.Visible;
    }

    private void ApplyPendingSplitResize()
    {
        if (_hasPendingPanelSplit)
        {
            CanvasLeftColumn.Width = new GridLength(_pendingLeftWidth, GridUnitType.Pixel);
            CanvasRightColumn.Width = new GridLength(_pendingRightWidth, GridUnitType.Pixel);
            _hasPendingPanelSplit = false;
        }

        if (_hasPendingRowSplit)
        {
            CanvasLayoutRow0.Height = new GridLength(_pendingTopHeight, GridUnitType.Pixel);
            CanvasLayoutRow2.Height = new GridLength(_pendingBottomHeight, GridUnitType.Pixel);
            _hasPendingRowSplit = false;
        }
    }

    private void EnsureCanvasExtentForViewportAndNodes()
    {
        var viewportWidth = Math.Max(CanvasViewport.ActualWidth, 1);
        var viewportHeight = Math.Max(CanvasViewport.ActualHeight, 1);
        var scale = Math.Max(CanvasTransform.ScaleX, 0.001);
        var viewLeft = (-CanvasTransform.TranslateX) / scale;
        var viewTop = (-CanvasTransform.TranslateY) / scale;
        var viewRight = (viewportWidth - CanvasTransform.TranslateX) / scale;
        var viewBottom = (viewportHeight - CanvasTransform.TranslateY) / scale;

        var extendLeft = viewLeft < CanvasExtendThreshold;
        var extendTop = viewTop < CanvasExtendThreshold;
        var extendRight = viewRight > _canvasWidth - CanvasExtendThreshold;
        var extendBottom = viewBottom > _canvasHeight - CanvasExtendThreshold;

        if (!extendLeft && !extendTop && !extendRight && !extendBottom)
        {
            return;
        }

        var shiftX = 0.0;
        var shiftY = 0.0;
        if (extendLeft && _canvasWidth < MaxCanvasWidth)
        {
            shiftX = Math.Min(CanvasExtendChunk, MaxCanvasWidth - _canvasWidth);
        }
        if (extendTop && _canvasHeight < MaxCanvasHeight)
        {
            shiftY = Math.Min(CanvasExtendChunk, MaxCanvasHeight - _canvasHeight);
        }
        var growRight = 0.0;
        if (extendRight && _canvasWidth < MaxCanvasWidth)
        {
            growRight = Math.Min(CanvasExtendChunk, MaxCanvasWidth - _canvasWidth - shiftX);
        }
        var growBottom = 0.0;
        if (extendBottom && _canvasHeight < MaxCanvasHeight)
        {
            growBottom = Math.Min(CanvasExtendChunk, MaxCanvasHeight - _canvasHeight - shiftY);
        }

        _canvasWidth += shiftX + growRight;
        _canvasHeight += shiftY + growBottom;

        if (shiftX > 0 || shiftY > 0)
        {
            foreach (var child in WorkspaceCanvas.Children)
            {
                if (child is not FrameworkElement element)
                {
                    continue;
                }

                Canvas.SetLeft(element, Canvas.GetLeft(element) + shiftX);
                Canvas.SetTop(element, Canvas.GetTop(element) + shiftY);
            }

            CanvasTransform.TranslateX -= shiftX * scale;
            CanvasTransform.TranslateY -= shiftY * scale;
        }

        ClampCanvasTransform();
        UpdateAllConnections();
        BuildCanvasGrid();
    }

    private void OnResetCanvasViewClick(object sender, RoutedEventArgs e)
    {
        if (IsSpaceClickSuppressed())
        {
            return;
        }

        FitCanvasToNodes();
        SetInlineStatus("已定位到节点流。", InlineStatusTone.Success);
    }

    private void ResetCanvasView()
    {
        CanvasTransform.ScaleX = 1;
        CanvasTransform.ScaleY = 1;
        CanvasTransform.TranslateX = 0;
        CanvasTransform.TranslateY = 0;
        UpdateCanvasZoomIndicator();
    }

    private void ClampCanvasTransform()
    {
        var scale = Math.Max(CanvasTransform.ScaleX, 0.001);
        var viewportWidth = CanvasViewport.ActualWidth;
        var viewportHeight = CanvasViewport.ActualHeight;
        if (viewportWidth <= 0 || viewportHeight <= 0)
        {
            return;
        }

        var scaledWidth = _canvasWidth * scale;
        var scaledHeight = _canvasHeight * scale;
        var minX = Math.Min(0, viewportWidth - scaledWidth);
        var minY = Math.Min(0, viewportHeight - scaledHeight);
        CanvasTransform.TranslateX = Math.Clamp(CanvasTransform.TranslateX, minX, 0);
        CanvasTransform.TranslateY = Math.Clamp(CanvasTransform.TranslateY, minY, 0);
    }

    private bool DeleteSelectedConnection()
    {
        if (_selectedConnection is null)
        {
            return false;
        }

        WorkspaceCanvas.Children.Remove(_selectedConnection.Line);
        _connections.Remove(_selectedConnection);
        _selectedConnection = null;
        UpdateConnectionVisuals();
        UpdateNodePropertyPanel();
        SetInlineStatus("已删除连线。", InlineStatusTone.Success);
        RequestCanvasAutosave();
        return true;
    }

    private bool DeleteSelectedUserNode()
    {
        if (_multiSelectedNodes.Count > 0)
        {
            var removed = 0;
            foreach (var node in _multiSelectedNodes.ToList())
            {
                if (node.Tag is not CanvasNodeTag multiTag || !multiTag.IsUserNode)
                {
                    continue;
                }

                RemoveConnectionsForNode(node);
                WorkspaceCanvas.Children.Remove(node);
                removed++;
            }

            _multiSelectedNodes.Clear();
            _selectedNode = null;
            ApplyNodeSelectionVisuals();
            UpdateNodePropertyPanel();
            if (removed > 0)
            {
                SetInlineStatus($"已删除 {removed} 个节点。", InlineStatusTone.Success);
                RequestCanvasAutosave();
                return true;
            }
        }

        if (_selectedNode is null || _selectedNode.Tag is not CanvasNodeTag tag || !tag.IsUserNode)
        {
            return false;
        }

        RemoveConnectionsForNode(_selectedNode);
        WorkspaceCanvas.Children.Remove(_selectedNode);
        _selectedNode = null;
        UpdateNodePropertyPanel();
        SetInlineStatus("已删除节点。", InlineStatusTone.Success);
        RequestCanvasAutosave();
        return true;
    }

    private void OnWindowActivated(object sender, WindowActivatedEventArgs args)
    {
        ApplyResponsiveLayout();
        if (_didPlayIntroAnimation)
        {
            return;
        }

        _didPlayIntroAnimation = true;
        PlayFadeIn(HeroHeaderBorder, 0, 220);
        PlayFadeIn(NavShellBorder, 90, 220);
        PlayFadeIn(ContentHostGrid, 180, 260);
        var activeElement = _activeSection switch
        {
            NavSection.Workspace => WorkspaceSectionGrid,
            NavSection.Canvas => CanvasSectionGrid,
            _ => ResultsSectionGrid
        };
        PlaySectionEntrance(activeElement);
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
        EnforceMinimumWindowSize(args.Size.Width, args.Size.Height);
        ApplyResponsiveLayout();
    }

    private void EnforceMinimumWindowSize(double width, double height)
    {
        if (_isEnforcingWindowMinSize)
        {
            return;
        }

        var targetWidth = (int)Math.Ceiling(Math.Max(width, MinWindowWidth));
        var targetHeight = (int)Math.Ceiling(Math.Max(height, MinWindowHeight));
        var currentWidth = (int)Math.Ceiling(width);
        var currentHeight = (int)Math.Ceiling(height);
        if (targetWidth == currentWidth && targetHeight == currentHeight)
        {
            return;
        }

        try
        {
            _isEnforcingWindowMinSize = true;
            AppWindow.Resize(new Windows.Graphics.SizeInt32(targetWidth, targetHeight));
        }
        catch
        {
            // Ignore if host window does not support resizing APIs.
        }
        finally
        {
            _isEnforcingWindowMinSize = false;
        }
    }

    [DllImport("user32.dll", EntryPoint = "SetWindowLongPtrW", SetLastError = true)]
    private static extern IntPtr SetWindowLongPtr64(IntPtr hWnd, int nIndex, IntPtr dwNewLong);

    [DllImport("user32.dll", EntryPoint = "SetWindowLongW", SetLastError = true)]
    private static extern int SetWindowLong32(IntPtr hWnd, int nIndex, int dwNewLong);

    private static IntPtr SetWindowLongPtr(IntPtr hWnd, int nIndex, IntPtr dwNewLong)
    {
        return IntPtr.Size == 8
            ? SetWindowLongPtr64(hWnd, nIndex, dwNewLong)
            : new IntPtr(SetWindowLong32(hWnd, nIndex, dwNewLong.ToInt32()));
    }

    [DllImport("user32.dll", SetLastError = true)]
    private static extern IntPtr CallWindowProc(IntPtr lpPrevWndFunc, IntPtr hWnd, uint msg, IntPtr wParam, IntPtr lParam);

    [DllImport("user32.dll")]
    private static extern uint GetDpiForWindow(IntPtr hWnd);

    private void ApplyResponsiveLayout()
    {
        var root = Content as FrameworkElement;
        var width = root?.ActualWidth ?? 1180;
        var height = root?.ActualHeight ?? 760;
        var rasterScale = root?.XamlRoot?.RasterizationScale ?? 1.0;
        ApplyCanvasResponsiveLayout(width, height, rasterScale);

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

    private void ApplyCanvasResponsiveLayout(double width, double height, double rasterScale)
    {
        // Prefer side-by-side for desktop and half-screen widths; stack only on truly narrow/portrait windows.
        var logicalAspect = width / Math.Max(height, 1.0);
        var stacked = width < 760 || (logicalAspect < 0.95 && width < 980);
        _isCanvasStacked = stacked;
        CanvasSplitHandle.Visibility = stacked ? Visibility.Collapsed : Visibility.Visible;
        CanvasStackSplitHandle.Visibility = stacked ? Visibility.Visible : Visibility.Collapsed;
        CanvasViewport.MinHeight = stacked ? 240 : 320;

        if (stacked)
        {
            CanvasLeftColumn.MinWidth = 0;
            CanvasRightColumn.MinWidth = 0;
            CanvasLeftColumn.Width = new GridLength(1, GridUnitType.Star);
            CanvasRightColumn.Width = new GridLength(0);
            CanvasLayoutRow0.Height = new GridLength(1, GridUnitType.Star);
            CanvasLayoutRow1.Height = new GridLength(12);
            CanvasLayoutRow2.Height = new GridLength(280, GridUnitType.Pixel);

            Grid.SetColumn(CanvasEditorPane, 0);
            Grid.SetRow(CanvasEditorPane, 0);
            Grid.SetColumnSpan(CanvasEditorPane, 3);
            Grid.SetRowSpan(CanvasEditorPane, 1);

            Grid.SetColumn(CanvasPropertyPane, 0);
            Grid.SetRow(CanvasPropertyPane, 2);
            Grid.SetColumnSpan(CanvasPropertyPane, 3);
            Grid.SetRowSpan(CanvasPropertyPane, 1);
            return;
        }

        CanvasLeftColumn.MinWidth = 360;
        CanvasRightColumn.MinWidth = 240;
        // Always normalize to a safe side-by-side ratio when entering horizontal mode.
        CanvasLeftColumn.Width = new GridLength(2.6, GridUnitType.Star);
        CanvasRightColumn.Width = new GridLength(1.4, GridUnitType.Star);
        CanvasLayoutRow0.Height = new GridLength(1, GridUnitType.Star);
        CanvasLayoutRow1.Height = new GridLength(0);
        CanvasLayoutRow2.Height = new GridLength(0);

        Grid.SetColumn(CanvasEditorPane, 0);
        Grid.SetRow(CanvasEditorPane, 0);
        Grid.SetColumnSpan(CanvasEditorPane, 1);
        Grid.SetRowSpan(CanvasEditorPane, 1);

        Grid.SetColumn(CanvasPropertyPane, 2);
        Grid.SetRow(CanvasPropertyPane, 0);
        Grid.SetColumnSpan(CanvasPropertyPane, 1);
        Grid.SetRowSpan(CanvasPropertyPane, 1);
    }
}

