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
internal sealed class SnowflakeApiClient : ISnowflakeApiClient, IDisposable
{
    private static readonly TimeSpan _tokenLifetime = TimeSpan.FromMinutes(59);
    private static readonly TimeSpan _statementPollInterval = TimeSpan.FromMilliseconds(500);
    private const int MaxStatementPollAttempts = 40;
    private static readonly JsonSerializerOptions _jsonOptions = new(JsonSerializerDefaults.Web);

    private readonly HttpClient _httpClient;
    private readonly SnowflakeConnectionSettings _settings;
    private readonly RSA _privateKey;
    private readonly bool _ownsHttpClient;
    private readonly object _tokenLock = new();
    private string _cachedToken;
    private DateTimeOffset _cachedTokenExpiry;

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

    public async Task<SnowflakeStatementResult> ExecuteStatementAsync(string sql, CancellationToken cancellationToken = default)
    {
        var requestBody = new StatementRequest
        {
            Statement = sql,
            Timeout = 60,
            Database = _settings.Database,
            Schema = _settings.Schema,
            Warehouse = _settings.Warehouse,
            Role = string.IsNullOrWhiteSpace(_settings.Role) ? null : _settings.Role,
        };

        using var response = await SendAsync(HttpMethod.Post, "/api/v2/statements", requestBody, cancellationToken);
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

            using var response = await SendAsync(HttpMethod.Get, $"/api/v2/statements/{statementHandle}", null, cancellationToken);
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
        using var response = await SendAsync(HttpMethod.Put, GetChannelPath(pipeName, channelName), new { }, cancellationToken);
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
        var path = $"{GetChannelPath(pipeName, channelName)}/rows?continuationToken={Uri.EscapeDataString(continuationToken)}";
        var requestBody = new AppendRowsRequest { Rows = rows };

        using var response = await SendAsync(HttpMethod.Post, path, requestBody, cancellationToken);
        var body = await response.Content.ReadAsStringAsync(cancellationToken);

        if (!response.IsSuccessStatusCode)
        {
            throw new SnowflakeApiException(ExtractErrorMessage(body), response.StatusCode, body);
        }

        var appendResponse = JsonSerializer.Deserialize<AppendRowsResponse>(body, _jsonOptions);
        if (appendResponse?.Errors is { Count: > 0 })
        {
            var firstError = appendResponse.Errors[0];
            throw new SnowflakeApiException($"row {firstError.RowIndex}: {firstError.Message}", response.StatusCode, body);
        }

        return new SnowflakeChannelHandle(channelName, appendResponse?.NextContinuationToken);
    }

    public async Task CloseChannelAsync(string pipeName, string channelName, CancellationToken cancellationToken = default)
    {
        using var response = await SendAsync(HttpMethod.Delete, GetChannelPath(pipeName, channelName), null, cancellationToken);
        if (!response.IsSuccessStatusCode && response.StatusCode != System.Net.HttpStatusCode.NotFound)
        {
            var body = await response.Content.ReadAsStringAsync(cancellationToken);
            throw new SnowflakeApiException(ExtractErrorMessage(body), response.StatusCode, body);
        }
    }

    private static string GetChannelPath(string pipeName, string channelName)
    {
        return $"/v2/streaming/channels/{Uri.EscapeDataString(pipeName)}/{Uri.EscapeDataString(channelName)}";
    }

    private async Task<HttpResponseMessage> SendAsync(HttpMethod method, string path, object body, CancellationToken cancellationToken)
    {
        using var request = new HttpRequestMessage(method, path);
        if (body != null)
        {
            request.Content = JsonContent.Create(body, options: _jsonOptions);
        }

        var token = GetOrCreateToken();
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
        request.Headers.Add("X-Snowflake-Authorization-Token-Type", "KEYPAIR_JWT");
        request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
        request.Headers.UserAgent.Add(new ProductInfoHeaderValue("CluedIn-Connector-Snowflake", "1.0"));

        return await _httpClient.SendAsync(request, cancellationToken);
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

    private class AppendRowsRequest
    {
        [JsonPropertyName("rows")]
        public IReadOnlyList<IReadOnlyDictionary<string, object>> Rows { get; set; }
    }

    private class AppendRowsResponse
    {
        [JsonPropertyName("next_continuation_token")]
        public string NextContinuationToken { get; set; }

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
