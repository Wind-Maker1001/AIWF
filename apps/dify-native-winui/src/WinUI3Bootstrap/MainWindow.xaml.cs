using Microsoft.UI.Xaml;
using Microsoft.UI.Xaml.Controls;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace AIWF.Native;

public sealed partial class MainWindow : Window
{
    private readonly HttpClient _http = new();

    public MainWindow()
    {
        InitializeComponent();
        try
        {
            AppWindow.Resize(new Windows.Graphics.SizeInt32(1180, 760));
        }
        catch
        {
            // Keep startup resilient if window sizing APIs are unavailable.
        }
    }

    private async void OnHealthClick(object sender, RoutedEventArgs e)
    {
        await SetBusyAsync(true, "Checking bridge health...");
        try
        {
            using var request = CreateRequest(HttpMethod.Get, "/health");
            using var response = await _http.SendAsync(request);
            var text = await response.Content.ReadAsStringAsync();
            RawResponseTextBox.Text = PrettyJson(text);
            StatusTextBlock.Text = response.IsSuccessStatusCode
                ? "Bridge health check passed."
                : $"Bridge health check failed: {(int)response.StatusCode}";
        }
        catch (Exception ex)
        {
            StatusTextBlock.Text = $"Health check error: {ex.Message}";
        }
        finally
        {
            await SetBusyAsync(false, StatusTextBlock.Text);
        }
    }

    private async void OnRunCleaningClick(object sender, RoutedEventArgs e)
    {
        await SetBusyAsync(true, "Submitting run-cleaning request...");
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
                StatusTextBlock.Text = $"Run failed: {(int)response.StatusCode}";
                return;
            }

            BindRunResult(text);
            StatusTextBlock.Text = "Run-cleaning request completed.";
        }
        catch (Exception ex)
        {
            StatusTextBlock.Text = $"Run request error: {ex.Message}";
        }
        finally
        {
            await SetBusyAsync(false, StatusTextBlock.Text);
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
        var baseUrl = BridgeUrlTextBox.Text.Trim().TrimEnd('/');
        if (string.IsNullOrWhiteSpace(baseUrl))
        {
            throw new InvalidOperationException("Bridge URL cannot be empty.");
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
        RunResultTextBlock.Text = "-";
        RunModeTextBlock.Text = "-";
        DurationTextBlock.Text = "-";

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
        RunResultTextBlock.Text = $"ok={ok}, job_id={jobId}";

        var data = root["data"];
        var mode = data?["mode"]?.GetValue<string?>() ?? root["run_mode"]?.GetValue<string?>() ?? "-";
        var duration = data?["duration_ms"]?.GetValue<int?>() ?? root["duration_ms"]?.GetValue<int?>();
        RunModeTextBlock.Text = mode;
        DurationTextBlock.Text = duration?.ToString() ?? "-";

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

    private Task SetBusyAsync(bool busy, string message)
    {
        HealthButton.IsEnabled = !busy;
        RunCleaningButton.IsEnabled = !busy;
        StatusTextBlock.Text = message;
        return Task.CompletedTask;
    }
}
