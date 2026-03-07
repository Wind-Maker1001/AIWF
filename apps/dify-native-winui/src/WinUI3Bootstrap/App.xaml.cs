using Microsoft.UI.Xaml;
using Microsoft.UI.Dispatching;

namespace AIWF.Native;

public partial class App : Application
{
    private Window? _window;
    private DispatcherQueueTimer? _perfAutoExitTimer;

    public App()
    {
        NativePerfRecorder.Mark("app_ctor");
        InitializeComponent();
    }

    protected override void OnLaunched(LaunchActivatedEventArgs args)
    {
        NativePerfRecorder.Mark("app_launch_enter");
        _window = new MainWindow();
        NativePerfRecorder.Mark("main_window_constructed");
        NativePerfRecorder.Mark("main_window_activated_request");
        _window.Activate();
        SchedulePerfAutoExit();
    }

    private void SchedulePerfAutoExit()
    {
        if (!NativePerfRecorder.IsEnabled)
        {
            return;
        }

        var raw = Environment.GetEnvironmentVariable("AIWF_NATIVE_PERF_AUTO_EXIT_MS");
        if (!int.TryParse(raw, out var autoExitMs) || autoExitMs <= 0)
        {
            return;
        }

        var dispatcherQueue = DispatcherQueue.GetForCurrentThread();
        if (dispatcherQueue is null)
        {
            return;
        }

        _perfAutoExitTimer = dispatcherQueue.CreateTimer();
        _perfAutoExitTimer.Interval = TimeSpan.FromMilliseconds(autoExitMs);
        _perfAutoExitTimer.IsRepeating = false;
        _perfAutoExitTimer.Tick += (_, _) =>
        {
            _perfAutoExitTimer?.Stop();
            NativePerfRecorder.Mark("auto_exit_triggered");
            _window?.Close();
        };
        _perfAutoExitTimer.Start();
        NativePerfRecorder.Mark("auto_exit_scheduled");
    }
}
