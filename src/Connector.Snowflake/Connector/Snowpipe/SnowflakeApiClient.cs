using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;

namespace CluedIn.Connector.Snowflake.Connector.Snowpipe;

// Talks to Snowflake entirely over the account's public REST surface (the SQL API for
// DDL/MERGE, the Snowpipe Streaming REST API for row ingestion) using RSA key-pair JWT
// auth, rather than pulling in the ADO.NET driver - keeps the connector's only dependency
// on Snowflake being HttpClient + BCL crypto, and keeps both surfaces testable behind
// ISnowflakeApiClient with a mocked HttpMessageHandler.
//
// The Snowpipe Streaming REST API is a separate deployment from the SQL API: requests
// don't go to the account's control host, they go to a discovered "ingest host"
// (GET /v2/streaming/hostname on the control host), and they're authorized with a scoped
// OAuth token (POST /oauth/token on the control host, exchanging the account JWT for a
// token scoped to that ingest host) rather than the JWT directly. All of this was verified
// against a live account - see docs/snowflake-connector-plan.md.
internal sealed class SnowflakeApiClient : ISnowflakeApiClient, IDisposable
{
    private static readonly TimeSpan _tokenLifetime = TimeSpan.FromMinutes(59);
    private static readonly TimeSpan _statementPollInterval = TimeSpan.FromMilliseconds(500);
    private const int MaxStatementPollAttempts = 40;
    private static readonly JsonSerializerOptions _jsonOptions = new(JsonSerializerDefaults.Web);
    private static readonly MediaTypeHeaderValue _ndjsonMediaType = new("application/x-ndjson");

    private readonly HttpClient _httpClient;
    private readonly SnowflakeConnectionSettings _settings;
    private readonly RSA _privateKey;
    private readonly bool _ownsHttpClient;
    private readonly object _tokenLock = new();
    private string _cachedToken;
    private DateTimeOffset _cachedTokenExpiry;
    private readonly object _ingestLock = new();
    private string _cachedIngestHostname;
    private string _cachedScopedToken;
    private DateTimeOffset _cachedScopedTokenExpiry;

    public SnowflakeApiClient(SnowflakeConnectionSettings settings, HttpClient httpClient = null)
    {
        _settings = settings ?? throw new ArgumentNullException(nameof(settings));
        _privateKey = SnowflakeJwtTokenBuilder.LoadPrivateKey(settings.PrivateKeyPem, settings.PrivateKeyPassphrase);

        if (httpClient == null)
        {
            _httpClient = new HttpClient
            {
                // Unlike the JWT iss/sub claims (which must use the bare account locator,
                // see SnowflakeJwtTokenBuilder.NormalizeAccount), the HTTP host needs the
                // full account identifier the user was given - which for many accounts
                // includes a region/cloud suffix (e.g. "qs30799.ap-southeast-1"). Stripping
                // it here caused every request to hit Snowflake's generic 404 page instead
                // of the account's actual deployment.
                BaseAddress = new Uri($"https://{settings.Account.Trim().ToLowerInvariant()}.snowflakecomputing.com"),
            };
            _ownsHttpClient = true;
        }
        else
        {
            _httpClient = httpClient;
            _ownsHttpClient = false;
        }
    }

    public async Task<SnowflakeStatementResult> ExecuteStatementAsync(string sql, SnowflakeStatementScope scope = SnowflakeStatementScope.All, CancellationToken cancellationToken = default)
    {
        var requestBody = new StatementRequest
        {
            Statement = sql,
            Timeout = 60,
            Database = scope is SnowflakeStatementScope.All or SnowflakeStatementScope.DatabaseOnly or SnowflakeStatementScope.DatabaseAndSchema
                ? _settings.Database
                : null,
            Schema = scope is SnowflakeStatementScope.All or SnowflakeStatementScope.DatabaseAndSchema
                ? _settings.Schema
                : null,
            Warehouse = scope is SnowflakeStatementScope.All or SnowflakeStatementScope.WarehouseOnly
                ? _settings.Warehouse
                : null,
            Role = string.IsNullOrWhiteSpace(_settings.Role) ? null : _settings.Role,
        };

        using var response = await SendControlHostAsync(HttpMethod.Post, "/api/v2/statements", requestBody, cancellationToken);
        var body = await response.Content.ReadAsStringAsync(cancellationToken);

        if (!response.IsSuccessStatusCode && response.StatusCode != System.Net.HttpStatusCode.Accepted)
        {
            throw new SnowflakeApiException(ExtractErrorMessage(body), response.StatusCode, body);
        }

        var statementResponse = JsonSerializer.Deserialize<StatementResponse>(body, _jsonOptions);

        if (response.StatusCode == System.Net.HttpStatusCode.Accepted && statementResponse?.StatementHandle != null)
        {
            statementResponse = await PollStatementAsync(statementResponse.StatementHandle, cancellationToken);
        }

        var columnNames = statementResponse?.ResultSetMetaData?.RowType?.Select(field => field.Name).ToList()
            ?? new List<string>();
        var rows = statementResponse?.Data?.Select(row => (IReadOnlyList<string>)row.Select(cell => cell?.ToString()).ToList()).ToList()
            ?? new List<IReadOnlyList<string>>();

        return new SnowflakeStatementResult(true, columnNames, rows);
    }

    private async Task<StatementResponse> PollStatementAsync(string statementHandle, CancellationToken cancellationToken)
    {
        for (var attempt = 0; attempt < MaxStatementPollAttempts; attempt++)
        {
            await Task.Delay(_statementPollInterval, cancellationToken);

            using var response = await SendControlHostAsync(HttpMethod.Get, $"/api/v2/statements/{statementHandle}", null, cancellationToken);
            var body = await response.Content.ReadAsStringAsync(cancellationToken);

            if (response.StatusCode == System.Net.HttpStatusCode.Accepted)
            {
                continue;
            }

            if (!response.IsSuccessStatusCode)
            {
                throw new SnowflakeApiException(ExtractErrorMessage(body), response.StatusCode, body);
            }

            return JsonSerializer.Deserialize<StatementResponse>(body, _jsonOptions);
        }

        throw new TimeoutException($"Snowflake statement '{statementHandle}' did not complete after {MaxStatementPollAttempts} polling attempts.");
    }

    public async Task<SnowflakeChannelHandle> OpenChannelAsync(string pipeName, string channelName, CancellationToken cancellationToken = default)
    {
        var uri = await BuildIngestUriAsync(GetChannelPath(pipeName, channelName), cancellationToken);
        using var response = await SendIngestHostAsync(HttpMethod.Put, uri, new { offset_token = "0" }, cancellationToken);
        var body = await response.Content.ReadAsStringAsync(cancellationToken);

        if (!response.IsSuccessStatusCode)
        {
            throw new SnowflakeApiException(ExtractErrorMessage(body), response.StatusCode, body);
        }

        var channelResponse = JsonSerializer.Deserialize<ChannelResponse>(body, _jsonOptions);
        return new SnowflakeChannelHandle(channelName, channelResponse?.NextContinuationToken);
    }

    public async Task<SnowflakeChannelHandle> AppendRowsAsync(
        string pipeName,
        string channelName,
        string continuationToken,
        IReadOnlyList<IReadOnlyDictionary<string, object>> rows,
        CancellationToken cancellationToken = default)
    {
        var path = $"{GetChannelDataPath(pipeName, channelName)}/rows?continuationToken={Uri.EscapeDataString(continuationToken)}";
        var uri = await BuildIngestUriAsync(path, cancellationToken);

        // The Snowpipe Streaming REST API takes newline-delimited JSON here, not a JSON
        // object wrapping a "rows" array - each row is one JSON object on its own line.
        var ndjson = string.Join("\n", rows.Select(row => JsonSerializer.Serialize(row, _jsonOptions)));

        using var response = await SendIngestHostAsync(HttpMethod.Post, uri, ndjson, cancellationToken);
        var body = await response.Content.ReadAsStringAsync(cancellationToken);

        if (!response.IsSuccessStatusCode)
        {
            throw new SnowflakeApiException(ExtractErrorMessage(body), response.StatusCode, body);
        }

        var appendResponse = JsonSerializer.Deserialize<AppendRowsResponse>(body, _jsonOptions);
        if (appendResponse?.StatusCode is not null && appendResponse.StatusCode != 0)
        {
            throw new SnowflakeApiException(appendResponse.Message ?? body, response.StatusCode, body);
        }

        if (appendResponse?.Errors is { Count: > 0 })
        {
            var firstError = appendResponse.Errors[0];
            throw new SnowflakeApiException($"row {firstError.RowIndex}: {firstError.Message}", response.StatusCode, body);
        }

        return new SnowflakeChannelHandle(channelName, appendResponse?.NextContinuationToken);
    }

    public async Task CloseChannelAsync(string pipeName, string channelName, CancellationToken cancellationToken = default)
    {
        var uri = await BuildIngestUriAsync(GetChannelPath(pipeName, channelName), cancellationToken);
        using var response = await SendIngestHostAsync(HttpMethod.Delete, uri, null, cancellationToken);
        if (!response.IsSuccessStatusCode && response.StatusCode != System.Net.HttpStatusCode.NotFound)
        {
            var body = await response.Content.ReadAsStringAsync(cancellationToken);
            throw new SnowflakeApiException(ExtractErrorMessage(body), response.StatusCode, body);
        }
    }

    // Named-channel open/close path: /v2/streaming/databases/{db}/schemas/{schema}/pipes/{pipe}/channels/{channel}
    private string GetChannelPath(string pipeName, string channelName)
    {
        return $"/v2/streaming/databases/{Uri.EscapeDataString(_settings.Database)}/schemas/{Uri.EscapeDataString(_settings.Schema)}"
            + $"/pipes/{Uri.EscapeDataString(pipeName)}/channels/{Uri.EscapeDataString(channelName)}";
    }

    // Row-append path has an extra "/data" segment the open/close path doesn't:
    // /v2/streaming/data/databases/{db}/schemas/{schema}/pipes/{pipe}/channels/{channel}
    private string GetChannelDataPath(string pipeName, string channelName)
    {
        return $"/v2/streaming/data/databases/{Uri.EscapeDataString(_settings.Database)}/schemas/{Uri.EscapeDataString(_settings.Schema)}"
            + $"/pipes/{Uri.EscapeDataString(pipeName)}/channels/{Uri.EscapeDataString(channelName)}";
    }

    private async Task<Uri> BuildIngestUriAsync(string pathAndQuery, CancellationToken cancellationToken)
    {
        var ingestHostname = await GetIngestHostnameAsync(cancellationToken);
        return new Uri($"https://{ingestHostname}{pathAndQuery}");
    }

    private async Task<string> GetIngestHostnameAsync(CancellationToken cancellationToken)
    {
        string cached;
        lock (_ingestLock)
        {
            cached = _cachedIngestHostname;
        }

        if (cached != null)
        {
            return cached;
        }

        using var response = await SendControlHostAsync(HttpMethod.Get, "/v2/streaming/hostname", null, cancellationToken);
        var body = (await response.Content.ReadAsStringAsync(cancellationToken)).Trim().Trim('"');

        if (!response.IsSuccessStatusCode)
        {
            throw new SnowflakeApiException(ExtractErrorMessage(body), response.StatusCode, body);
        }

        lock (_ingestLock)
        {
            _cachedIngestHostname = body;
        }

        return body;
    }

    private async Task<string> GetScopedTokenAsync(CancellationToken cancellationToken)
    {
        string cached;
        DateTimeOffset expiry;
        lock (_ingestLock)
        {
            cached = _cachedScopedToken;
            expiry = _cachedScopedTokenExpiry;
        }

        var now = DateTimeOffset.UtcNow;
        if (cached != null && now < expiry)
        {
            return cached;
        }

        var ingestHostname = await GetIngestHostnameAsync(cancellationToken);

        using var request = new HttpRequestMessage(HttpMethod.Post, "/oauth/token")
        {
            Content = new FormUrlEncodedContent(new[]
            {
                new KeyValuePair<string, string>("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"),
                new KeyValuePair<string, string>("scope", ingestHostname),
            }),
        };
        ApplyJwtAuthHeaders(request);

        using var response = await _httpClient.SendAsync(request, cancellationToken);
        var body = (await response.Content.ReadAsStringAsync(cancellationToken)).Trim().Trim('"');

        if (!response.IsSuccessStatusCode)
        {
            throw new SnowflakeApiException(ExtractErrorMessage(body), response.StatusCode, body);
        }

        lock (_ingestLock)
        {
            _cachedScopedToken = body;
            // Scoped tokens mirror the underlying JWT's lifetime; refresh a couple of
            // minutes early to avoid racing expiry mid-request.
            _cachedScopedTokenExpiry = now.Add(_tokenLifetime) - TimeSpan.FromMinutes(2);
        }

        return body;
    }

    private async Task<HttpResponseMessage> SendControlHostAsync(HttpMethod method, string path, object body, CancellationToken cancellationToken)
    {
        using var request = new HttpRequestMessage(method, path);
        if (body != null)
        {
            request.Content = JsonContent.Create(body, options: _jsonOptions);
        }

        ApplyJwtAuthHeaders(request);

        return await _httpClient.SendAsync(request, cancellationToken);
    }

    private async Task<HttpResponseMessage> SendIngestHostAsync(HttpMethod method, Uri uri, object jsonBody, CancellationToken cancellationToken)
    {
        using var request = new HttpRequestMessage(method, uri);
        if (jsonBody != null)
        {
            request.Content = JsonContent.Create(jsonBody, options: _jsonOptions);
        }

        await ApplyScopedTokenAuthHeadersAsync(request, cancellationToken);

        return await _httpClient.SendAsync(request, cancellationToken);
    }

    private async Task<HttpResponseMessage> SendIngestHostAsync(HttpMethod method, Uri uri, string ndjsonBody, CancellationToken cancellationToken)
    {
        using var request = new HttpRequestMessage(method, uri);
        if (ndjsonBody != null)
        {
            request.Content = new StringContent(ndjsonBody, Encoding.UTF8);
            request.Content.Headers.ContentType = _ndjsonMediaType;
        }

        await ApplyScopedTokenAuthHeadersAsync(request, cancellationToken);

        return await _httpClient.SendAsync(request, cancellationToken);
    }

    private void ApplyJwtAuthHeaders(HttpRequestMessage request)
    {
        var token = GetOrCreateToken();
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
        request.Headers.Add("X-Snowflake-Authorization-Token-Type", "KEYPAIR_JWT");
        request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
        request.Headers.UserAgent.Add(new ProductInfoHeaderValue("CluedIn-Connector-Snowflake", "1.0"));
    }

    private async Task ApplyScopedTokenAuthHeadersAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        var token = await GetScopedTokenAsync(cancellationToken);
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
        request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
        request.Headers.UserAgent.Add(new ProductInfoHeaderValue("CluedIn-Connector-Snowflake", "1.0"));
    }

    private string GetOrCreateToken()
    {
        lock (_tokenLock)
        {
            var now = DateTimeOffset.UtcNow;
            if (_cachedToken != null && now < _cachedTokenExpiry)
            {
                return _cachedToken;
            }

            _cachedToken = SnowflakeJwtTokenBuilder.BuildToken(_settings.Account, _settings.User, _privateKey, now, _tokenLifetime);
            _cachedTokenExpiry = now.Add(_tokenLifetime) - TimeSpan.FromMinutes(2);
            return _cachedToken;
        }
    }

    private static string ExtractErrorMessage(string body)
    {
        if (string.IsNullOrWhiteSpace(body))
        {
            return "no response body";
        }

        try
        {
            var error = JsonSerializer.Deserialize<ErrorResponse>(body, _jsonOptions);
            return error?.Message ?? body;
        }
        catch (JsonException)
        {
            return body;
        }
    }

    public void Dispose()
    {
        _privateKey?.Dispose();
        if (_ownsHttpClient)
        {
            _httpClient?.Dispose();
        }
    }

    private class StatementRequest
    {
        [JsonPropertyName("statement")]
        public string Statement { get; set; }

        [JsonPropertyName("timeout")]
        public int Timeout { get; set; }

        [JsonPropertyName("database")]
        public string Database { get; set; }

        [JsonPropertyName("schema")]
        public string Schema { get; set; }

        [JsonPropertyName("warehouse")]
        public string Warehouse { get; set; }

        [JsonPropertyName("role")]
        public string Role { get; set; }
    }

    private class StatementResponse
    {
        [JsonPropertyName("statementHandle")]
        public string StatementHandle { get; set; }

        [JsonPropertyName("resultSetMetaData")]
        public ResultSetMetaData ResultSetMetaData { get; set; }

        [JsonPropertyName("data")]
        public List<List<object>> Data { get; set; }
    }

    private class ResultSetMetaData
    {
        [JsonPropertyName("rowType")]
        public List<RowTypeField> RowType { get; set; }
    }

    private class RowTypeField
    {
        [JsonPropertyName("name")]
        public string Name { get; set; }
    }

    private class ChannelResponse
    {
        [JsonPropertyName("next_continuation_token")]
        public string NextContinuationToken { get; set; }
    }

    private class AppendRowsResponse
    {
        [JsonPropertyName("next_continuation_token")]
        public string NextContinuationToken { get; set; }

        [JsonPropertyName("status_code")]
        public int? StatusCode { get; set; }

        [JsonPropertyName("message")]
        public string Message { get; set; }

        [JsonPropertyName("errors")]
        public List<AppendRowError> Errors { get; set; }
    }

    private class AppendRowError
    {
        [JsonPropertyName("rowIndex")]
        public int RowIndex { get; set; }

        [JsonPropertyName("message")]
        public string Message { get; set; }
    }

    private class ErrorResponse
    {
        [JsonPropertyName("message")]
        public string Message { get; set; }
    }
}
