using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json.Nodes;

namespace AIWF.Native.Runtime;

public sealed record RunFlowExecutionResult(
    HttpResponseMessage Response,
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
        var retryInfo = "未重试";
        var effectiveJobId = (jobId ?? string.Empty).Trim();
        var ensured = await EnsureJobIdAsync(bridgeBaseUrl, apiKey, owner, effectiveJobId, cancellationToken);
        if (!string.IsNullOrWhiteSpace(ensured) && !string.Equals(ensured, effectiveJobId, StringComparison.Ordinal))
        {
            effectiveJobId = ensured;
            retryInfo = $"预检创建作业：{ensured}";
        }

        var (response, body) = await _runner.RunFlowAsync(
            bridgeBaseUrl,
            apiKey,
            effectiveJobId,
            flow,
            payload,
            cancellationToken);
        var retriedAfter500 = false;

        if (!response.IsSuccessStatusCode && response.StatusCode == HttpStatusCode.InternalServerError)
        {
            var forcedJobId = await TryCreateJobAsync(bridgeBaseUrl, apiKey, owner, cancellationToken);
            if (!string.IsNullOrWhiteSpace(forcedJobId))
            {
                effectiveJobId = forcedJobId;
                (response, body) = await _runner.RunFlowAsync(
                    bridgeBaseUrl,
                    apiKey,
                    forcedJobId,
                    flow,
                    payload,
                    cancellationToken);
                retryInfo = $"已重试（新作业）：{forcedJobId}";
                retriedAfter500 = true;
            }
        }

        return new RunFlowExecutionResult(response, body, effectiveJobId, retryInfo, retriedAfter500);
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
            var root = JsonNode.Parse(text);
            return root?["job_id"]?.GetValue<string?>();
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
