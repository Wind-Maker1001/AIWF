using System.Net;
using System.Net.Http.Headers;
using System.Globalization;
using System.Text;
using System.Text.Json.Nodes;

namespace AIWF.Native.Runtime;

public sealed record RunFlowExecutionResult(
    HttpStatusCode StatusCode,
    bool IsSuccessStatusCode,
    string Body,
    string EffectiveJobId,
    string RetryInfo,
    bool RetriedAfterServerError);

public sealed class RunFlowCoordinator
{
    private readonly HttpClient _http;
    private readonly WorkflowRunnerAdapter _runner;

    public RunFlowCoordinator(HttpClient http, WorkflowRunnerAdapter runner)
    {
        _http = http;
        _runner = runner;
    }

    public async Task<RunFlowExecutionResult> ExecuteAsync(
        string bridgeBaseUrl,
        string apiKey,
        string owner,
        string jobId,
        string flow,
        JsonObject payload,
        CancellationToken cancellationToken = default)
    {
        var retryInfo = "Not retried";
        var effectiveJobId = (jobId ?? string.Empty).Trim();
        var ensured = await EnsureJobIdAsync(bridgeBaseUrl, apiKey, owner, effectiveJobId, cancellationToken);
        if (!string.IsNullOrWhiteSpace(ensured) && !string.Equals(ensured, effectiveJobId, StringComparison.Ordinal))
        {
            effectiveJobId = ensured;
            retryInfo = $"Preflight created job: {ensured}";
        }

        if (string.IsNullOrWhiteSpace(effectiveJobId))
        {
            throw new InvalidOperationException("Unable to auto-create a job. Verify the bridge and job services are available.");
        }

        var runResult = await _runner.RunFlowAsync(
            bridgeBaseUrl,
            apiKey,
            effectiveJobId,
            flow,
            payload,
            cancellationToken);

        return new RunFlowExecutionResult(
            runResult.StatusCode,
            runResult.IsSuccessStatusCode,
            runResult.Body,
            effectiveJobId,
            retryInfo,
            false);
    }

    private async Task<string?> TryCreateJobAsync(string bridgeBaseUrl, string apiKey, string owner, CancellationToken cancellationToken)
    {
        var baseApiUrl = ResolveBaseApiUrlFromBridge(bridgeBaseUrl);
        if (string.IsNullOrWhiteSpace(baseApiUrl))
        {
            return null;
        }

        var useOwner = string.IsNullOrWhiteSpace(owner) ? "native" : owner.Trim();
        var uri = $"{baseApiUrl}/api/v1/jobs/create?owner={Uri.EscapeDataString(useOwner)}";
        using var request = new HttpRequestMessage(HttpMethod.Post, uri);
        request.Content = new StringContent("{}", Encoding.UTF8, "application/json");
        request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
        var token = (apiKey ?? string.Empty).Trim();
        if (!string.IsNullOrWhiteSpace(token))
        {
            request.Headers.Add("X-API-Key", token);
        }

        using var response = await _http.SendAsync(request, cancellationToken);
        if (!response.IsSuccessStatusCode)
        {
            return null;
        }

        var text = await response.Content.ReadAsStringAsync(cancellationToken);
        try
        {
            var root = JsonNode.Parse(text) as JsonObject;
            return ReadString(root?["job_id"]) ?? ReadString((root?["data"] as JsonObject)?["job_id"]);
        }
        catch
        {
            return null;
        }
    }

    private async Task<string> EnsureJobIdAsync(
        string bridgeBaseUrl,
        string apiKey,
        string owner,
        string requestedJobId,
        CancellationToken cancellationToken)
    {
        var trimmed = (requestedJobId ?? string.Empty).Trim();
        var token = apiKey ?? string.Empty;
        var baseApiUrl = ResolveBaseApiUrlFromBridge(bridgeBaseUrl);
        if (string.IsNullOrWhiteSpace(baseApiUrl))
        {
            return trimmed;
        }

        if (!string.IsNullOrWhiteSpace(trimmed))
        {
            try
            {
                using var getReq = new HttpRequestMessage(HttpMethod.Get, $"{baseApiUrl}/api/v1/jobs/{Uri.EscapeDataString(trimmed)}");
                var verifyToken = token.Trim();
                if (!string.IsNullOrWhiteSpace(verifyToken))
                {
                    getReq.Headers.Add("X-API-Key", verifyToken);
                }

                using var getResp = await _http.SendAsync(getReq, cancellationToken);
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

        var created = await TryCreateJobAsync(bridgeBaseUrl, token, owner, cancellationToken);
        return string.IsNullOrWhiteSpace(created) ? trimmed : created;
    }

    private static string? ReadString(JsonNode? node)
    {
        if (node is not JsonValue value)
        {
            return null;
        }

        if (value.TryGetValue<string>(out var text) && !string.IsNullOrWhiteSpace(text))
        {
            return text.Trim();
        }

        if (value.TryGetValue<int>(out var intValue))
        {
            return intValue.ToString(CultureInfo.InvariantCulture);
        }

        if (value.TryGetValue<long>(out var longValue))
        {
            return longValue.ToString(CultureInfo.InvariantCulture);
        }

        return null;
    }

    private static string? ResolveBaseApiUrlFromBridge(string bridgeBaseUrl)
    {
        var bridge = (bridgeBaseUrl ?? string.Empty).Trim().TrimEnd('/');
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
}
