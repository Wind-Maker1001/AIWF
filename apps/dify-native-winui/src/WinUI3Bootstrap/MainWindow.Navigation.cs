using AIWF.Native.Runtime;
using Microsoft.UI.Dispatching;
using Microsoft.UI.Xaml;
using Microsoft.UI.Xaml.Controls;
using Microsoft.UI.Xaml.Media;

namespace AIWF.Native;

public sealed partial class MainWindow
{
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

    private void OnGovernanceNavClick(object sender, RoutedEventArgs e)
    {
        SetActiveSection(NavSection.Governance);
    }

    private void SetActiveSection(NavSection section)
    {
        if (section == NavSection.Canvas)
        {
            EnsureCanvasWorkspaceInitialized(shouldPrewarm: true);
        }

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
        GovernanceSectionGrid.Visibility = section == NavSection.Governance
            ? Visibility.Visible
            : Visibility.Collapsed;

        ApplyNavButtonState(WorkspaceNavButton, section == NavSection.Workspace);
        ApplyNavButtonState(CanvasNavButton, section == NavSection.Canvas);
        ApplyNavButtonState(ResultNavButton, section == NavSection.Results);
        ApplyNavButtonState(GovernanceNavButton, section == NavSection.Governance);
        var activeElement = section switch
        {
            NavSection.Workspace => WorkspaceSectionGrid,
            NavSection.Canvas => CanvasSectionGrid,
            NavSection.Results => ResultsSectionGrid,
            _ => GovernanceSectionGrid
        };
        PlaySectionEntrance(activeElement);

        if (section == NavSection.Canvas && !IsUiaSmokeMode)
        {
            ScheduleCanvasSnapshotRestoreIfNeeded();
        }

        if (section == NavSection.Canvas && IsUiaSmokeMode)
        {
            TryForceSaveCanvasSnapshotForSmoke();
        }

        if (section == NavSection.Governance)
        {
            _ = RefreshGovernanceAsync();
        }
    }

    private void PrewarmCanvasSection()
    {
        if (_didPrewarmCanvasSection || !_isCanvasWorkspaceInitialized)
        {
            return;
        }

        _didPrewarmCanvasSection = true;
        NativePerfRecorder.Mark("canvas_prewarm_enter");
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

        NativePerfRecorder.Mark("canvas_prewarm_exit");
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

    private bool EnsureCanvasWorkspaceInitialized(bool shouldPrewarm = false)
    {
        if (!_isCanvasWorkspaceInitialized)
        {
            try
            {
                NativePerfRecorder.Mark("canvas_workspace_init_enter");
                InitializeCanvasWorkspace();
                _isCanvasWorkspaceInitialized = true;
                NativePerfRecorder.Mark("canvas_workspace_init_exit");
            }
            catch (Exception ex)
            {
                SetInlineStatus($"画布初始化失败：{ex.Message}", InlineStatusTone.Error);
                return false;
            }
        }

        if (shouldPrewarm)
        {
            PrewarmCanvasSection();
        }

        return true;
    }

    private void ScheduleDeferredCanvasWarmup()
    {
        if (_didScheduleCanvasWarmup)
        {
            return;
        }

        _didScheduleCanvasWarmup = true;
        var dispatcherQueue = DispatcherQueue.GetForCurrentThread();
        if (dispatcherQueue is null)
        {
            return;
        }

        dispatcherQueue.TryEnqueue(() =>
        {
            EnsureCanvasWorkspaceInitialized(shouldPrewarm: true);
        });
    }
}
