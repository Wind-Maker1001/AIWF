using System.Net.Http.Headers;
using System.Text;
using System.Text.Json.Nodes;

namespace AIWF.Native.Runtime;

public sealed class WorkflowRunnerAdapter
{
    private readonly HttpClient _http;

    public WorkflowRunnerAdapter(HttpClient http)
    {
        _http = http;
    }

    public async Task<(HttpResponseMessage Response, string Body)> CheckHealthAsync(
        string baseUrl,
        string? apiKey,
        CancellationToken cancellationToken = default)
    {
        using var request = BuildRequest(HttpMethod.Get, baseUrl, "/health", apiKey);
        var response = await _http.SendAsync(request, cancellationToken);
        var body = await response.Content.ReadAsStringAsync(cancellationToken);
        return (response, body);
    }

    public async Task<(HttpResponseMessage Response, string Body)> RunFlowAsync(
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
        var response = await _http.SendAsync(request, cancellationToken);
        var body = await response.Content.ReadAsStringAsync(cancellationToken);
        return (response, body);
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
