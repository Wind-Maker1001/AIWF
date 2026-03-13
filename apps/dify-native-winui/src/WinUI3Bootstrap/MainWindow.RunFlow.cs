using System.Text.Json.Nodes;
using AIWF.Native.Runtime;
using Microsoft.UI.Xaml;
using Microsoft.UI.Xaml.Controls;

namespace AIWF.Native;

public sealed partial class MainWindow
{
    private async void OnHealthClick(object sender, RoutedEventArgs e)
    {
        SyncViewModelFromInputs();
        await SetBusyAsync(true, "Checking bridge service health...", InlineStatusTone.Busy);
        try
        {
            var baseUrl = GetBridgeBaseUrlOrThrow();
            var response = await _runnerAdapter.CheckHealthAsync(baseUrl, ApiKeyTextBox.Text.Trim());
            RawResponseTextBox.Text = PrettyJson(response.Body);
            RunReferenceTextBlock.Text = response.IsSuccessStatusCode
                ? "Bridge is reachable."
                : "Bridge is unavailable. Check service startup.";
            SetInlineStatus(
                response.IsSuccessStatusCode
                    ? "Bridge health check passed."
                    : $"Bridge health check failed: {(int)response.StatusCode}",
                response.IsSuccessStatusCode ? InlineStatusTone.Success : InlineStatusTone.Error);
        }
        catch (Exception ex)
        {
            SetInlineStatus($"Health check error: {ex.Message}", InlineStatusTone.Error);
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
            throw new InvalidOperationException("Bridge URL is required.");
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

        if (exec.RetryInfo.StartsWith("Preflight created job:", StringComparison.Ordinal))
        {
            RunReferenceTextBlock.Text = "A new job was prepared automatically.";
        }

        if (exec.RetriedAfterServerError)
        {
            SetInlineStatus("Detected a server 500 and retried once with a new job.", InlineStatusTone.Busy);
            RunReferenceTextBlock.Text = "Retried once automatically.";
        }

        RawResponseTextBox.Text = PrettyJson(exec.Body);

        if (!exec.IsSuccessStatusCode)
        {
            RunReferenceTextBlock.Text = "Run failed. Please try again.";
            SetInlineStatus($"Run failed: {(int)exec.StatusCode}", InlineStatusTone.Error);
            return false;
        }

        if (!TryBindRunResult(exec.Body, exec.RetryInfo))
        {
            RunReferenceTextBlock.Text = "Run completed, but the response format is unknown.";
            SetInlineStatus("Run completed, but the response could not be parsed.", InlineStatusTone.Error);
            SetActiveSection(NavSection.Results);
            return false;
        }

        if (!_lastBoundRunBusinessSuccess)
        {
            RunReferenceTextBlock.Text = "Run completed, but the business result is failure.";
            SetInlineStatus("The workflow returned a failure result. Check the detailed response.", InlineStatusTone.Error);
            SetActiveSection(NavSection.Results);
            return false;
        }

        RunReferenceTextBlock.Text = "Run succeeded. Results updated.";
        SetInlineStatus("Workflow request completed.", InlineStatusTone.Success);
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

        await SetBusyAsync(true, "Submitting workflow run request...", InlineStatusTone.Busy);
        try
        {
            RunReferenceTextBlock.Text = "Preparing run...";
            var input = CollectRunRequestInput();
            var exec = await ExecuteRunRequestAsync(input);
            TryApplyRunExecutionResult(exec);
        }
        catch (Exception ex)
        {
            RunReferenceTextBlock.Text = "Run request failed. Check service status.";
            SetInlineStatus($"Run request error: {ex.Message}", InlineStatusTone.Error);
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
}
