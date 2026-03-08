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
        await SetBusyAsync(true, "正在检查桥接服务健康状态...", InlineStatusTone.Busy);
        try
        {
            var baseUrl = GetBridgeBaseUrlOrThrow();
            var response = await _runnerAdapter.CheckHealthAsync(baseUrl, ApiKeyTextBox.Text.Trim());
            RawResponseTextBox.Text = PrettyJson(response.Body);
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

        if (!exec.IsSuccessStatusCode)
        {
            RunReferenceTextBlock.Text = "运行失败，请稍后重试。";
            SetInlineStatus($"运行失败：{(int)exec.StatusCode}", InlineStatusTone.Error);
            return false;
        }

        if (!TryBindRunResult(exec.Body, exec.RetryInfo))
        {
            RunReferenceTextBlock.Text = "运行完成，但结果格式无法识别。";
            SetInlineStatus("运行完成，但结果解析失败，请检查原始 JSON。", InlineStatusTone.Error);
            SetActiveSection(NavSection.Results);
            return false;
        }

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
}
