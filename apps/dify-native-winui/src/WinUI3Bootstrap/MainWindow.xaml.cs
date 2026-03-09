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
using Microsoft.UI.Xaml.Automation;
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
    private readonly CanvasRuntime.CanvasConnectionIndex<Border, ConnectionEdge> _connectionIndex = new();
    private readonly HashSet<Border> _highlightedNodes = new();
    private ConnectionEdge? _lastHighlightedConnection;
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
    private CopiedNodeTemplate? _copiedNodeTemplate;
    private readonly DispatcherQueueTimer? _canvasAutosaveTimer;
    private readonly DispatcherQueueTimer? _statusDecayTimer;
    private readonly DispatcherQueueTimer? _canvasInteractionSettleTimer;
    private readonly SemaphoreSlim _canvasSnapshotOperationLock = new(1, 1);
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
    private bool _isCanvasWorkspaceInitialized;
    private bool _didPrewarmCanvasSection;
    private bool _didScheduleCanvasWarmup;
    private string? _lastSavedCanvasSnapshotJson;
    private static readonly JsonSerializerOptions CanvasSnapshotJsonOptions = new()
    {
        WriteIndented = true
    };
    private static readonly string CanvasStateFilePath = System.IO.Path.Combine(
        Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
        "AIWF",
        "canvas-workflow.json");
    private static readonly NodeSelectionVisual ActiveNodeSelectionVisual = CanvasSelectionPresenter.ResolveNode(true);
    private static readonly NodeSelectionVisual InactiveNodeSelectionVisual = CanvasSelectionPresenter.ResolveNode(false);
    private static readonly ConnectionSelectionVisual ActiveConnectionSelectionVisual = CanvasSelectionPresenter.ResolveConnection(true);
    private static readonly ConnectionSelectionVisual InactiveConnectionSelectionVisual = CanvasSelectionPresenter.ResolveConnection(false);
    private static readonly SolidColorBrush ActiveNodeSelectionBrush = new(ActiveNodeSelectionVisual.BorderColor);
    private static readonly SolidColorBrush InactiveNodeSelectionBrush = new(InactiveNodeSelectionVisual.BorderColor);
    private static readonly SolidColorBrush ActiveConnectionSelectionBrush = new(ActiveConnectionSelectionVisual.StrokeColor);
    private static readonly SolidColorBrush InactiveConnectionSelectionBrush = new(InactiveConnectionSelectionVisual.StrokeColor);
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
        public bool IsArtifactNode { get; init; }
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

    private sealed class CopiedNodeTemplate
    {
        public string Title { get; set; } = string.Empty;
        public string Subtitle { get; set; } = string.Empty;
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
                _ = SaveCanvasSnapshotAsync(showStatus: false);
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

}
