using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace AIWF.Native.Runtime;

public sealed record WorkflowHttpResult(
    HttpStatusCode StatusCode,
    bool IsSuccessStatusCode,
    string Body);

public sealed class WorkflowRunnerAdapter
{
    private readonly HttpClient _http;

    public WorkflowRunnerAdapter(HttpClient http)
    {
        _http = http;
    }

    public async Task<WorkflowHttpResult> CheckHealthAsync(
        string baseUrl,
        string? apiKey,
        CancellationToken cancellationToken = default)
    {
        using var request = BuildRequest(HttpMethod.Get, baseUrl, "/health", apiKey);
        using var response = await _http.SendAsync(request, cancellationToken);
        var body = await response.Content.ReadAsStringAsync(cancellationToken);
        return new WorkflowHttpResult(response.StatusCode, response.IsSuccessStatusCode, body);
    }

    public async Task<WorkflowHttpResult> RunFlowAsync(
        string baseUrl,
        string? apiKey,
        string jobId,
        string flow,
        JsonObject payload,
        CancellationToken cancellationToken = default)
    {
        var encodedJobId = Uri.EscapeDataString(jobId.Trim());
        var encodedFlow = Uri.EscapeDataString(flow.Trim());
        using var request = BuildRequest(HttpMethod.Post, baseUrl, $"/jobs/{encodedJobId}/run/{encodedFlow}", apiKey);
        request.Content = new StringContent(payload.ToJsonString(), Encoding.UTF8, "application/json");
        using var response = await _http.SendAsync(request, cancellationToken);
        var body = await response.Content.ReadAsStringAsync(cancellationToken);
        return new WorkflowHttpResult(response.StatusCode, response.IsSuccessStatusCode, body);
    }

    public async Task<JsonObject> PrecheckCleaningAsync(
        string baseUrl,
        string? apiKey,
        JsonObject payload,
        CancellationToken cancellationToken = default)
    {
        return await PostJsonAsync(baseUrl, apiKey, "/cleaning/precheck", payload, cancellationToken);
    }

    public async Task<JsonObject> PostJsonAsync(
        string baseUrl,
        string? apiKey,
        string endpointPath,
        JsonNode payload,
        CancellationToken cancellationToken = default)
    {
        using var request = BuildRequest(HttpMethod.Post, baseUrl, endpointPath, apiKey);
        request.Content = new StringContent(
            payload.ToJsonString(),
            Encoding.UTF8,
            "application/json");
        using var response = await _http.SendAsync(request, cancellationToken);
        var body = await response.Content.ReadAsStringAsync(cancellationToken);
        JsonNode? parsed = null;
        try
        {
            parsed = JsonNode.Parse(body);
        }
        catch
        {
        }

        if (!response.IsSuccessStatusCode)
        {
            var message = parsed?["error"]?.GetValue<string>()
                ?? $"HTTP {(int)response.StatusCode}";
            throw new InvalidOperationException(message);
        }

        if (parsed is not JsonObject root)
        {
            throw new InvalidOperationException("JSON object response expected.");
        }

        return root;
    }

    private static HttpRequestMessage BuildRequest(HttpMethod method, string baseUrl, string endpointPath, string? apiKey)
    {
        var normalizedBase = (baseUrl ?? string.Empty).Trim().TrimEnd('/');
        var request = new HttpRequestMessage(method, $"{normalizedBase}{endpointPath}");
        request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
        var token = (apiKey ?? string.Empty).Trim();
        if (!string.IsNullOrWhiteSpace(token))
        {
            request.Headers.Add("X-API-Key", token);
        }

        return request;
    }
}
