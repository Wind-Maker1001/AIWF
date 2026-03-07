using System;
using System.Runtime.InteropServices;
using Microsoft.UI.Xaml;
using Microsoft.UI.Xaml.Controls;
using Microsoft.UI.Xaml.Media.Animation;

namespace AIWF.Native;

public sealed partial class MainWindow
{
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
        NativePerfRecorder.Mark("window_closed");
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

    private void OnWindowActivated(object sender, WindowActivatedEventArgs args)
    {
        NativePerfRecorder.Mark("window_activated");
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
