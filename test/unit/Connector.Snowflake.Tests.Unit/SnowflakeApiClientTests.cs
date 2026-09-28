using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using CluedIn.Connector.Snowflake.Connector.Snowpipe;

using Xunit;

namespace CluedIn.Connector.Snowflake.Tests.Unit;

public class SnowflakeApiClientTests
{
    private static SnowflakeConnectionSettings CreateSettings()
    {
        using var rsa = RSA.Create(2048);
        return new SnowflakeConnectionSettings(
            "qs30799",
            "cluedin_svc",
            rsa.ExportPkcs8PrivateKeyPem(),
            null,
            "SNOWFLAKE_LEARNING_DB",
            "TESTSCHEMA",
            "COMPUTE_WH",
            "ACCOUNTADMIN");
    }

    [Fact]
    public async Task ExecuteStatementAsync_SendsBearerJwtAndStatementBody()
    {
        var handler = new RecordingHttpMessageHandler(
            (_, _) => new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent("""{"resultSetMetaData":{"rowType":[]},"data":[]}""", Encoding.UTF8, "application/json"),
            });
        using var httpClient = new HttpClient(handler) { BaseAddress = new Uri("https://qs30799.snowflakecomputing.com") };
        using var apiClient = new SnowflakeApiClient(CreateSettings(), httpClient);

        var result = await apiClient.ExecuteStatementAsync("SELECT 1");

        Assert.True(result.Success);
        var request = Assert.Single(handler.Requests);
        Assert.Equal(HttpMethod.Post, request.Method);
        Assert.Equal("/api/v2/statements", request.Uri.AbsolutePath);
        Assert.Equal("Bearer", request.AuthorizationScheme);
        Assert.NotEmpty(request.AuthorizationParameter);
        Assert.Equal("KEYPAIR_JWT", request.Headers["X-Snowflake-Authorization-Token-Type"]);
        Assert.Contains("\"statement\":\"SELECT 1\"", request.Body);
        Assert.Contains("\"database\":\"SNOWFLAKE_LEARNING_DB\"", request.Body);
        Assert.Contains("\"warehouse\":\"COMPUTE_WH\"", request.Body);
    }

    // The Snowpipe Streaming REST API is a separate deployment from the SQL API: every
    // streaming call is preceded by (1) GET /v2/streaming/hostname on the control host to
    // discover the ingest host, and (2) POST /oauth/token on the control host to exchange
    // the account JWT for a token scoped to that ingest host - both verified against a
    // live account (see docs/snowflake-connector-plan.md). This handler answers those two
    // first, then routes the remaining request(s) to the per-test responder.
    private static RecordingHttpMessageHandler CreateStreamingHandler(
        Func<HttpRequestMessage, string, HttpResponseMessage> respondToDataCall)
    {
        return new RecordingHttpMessageHandler((request, body) =>
        {
            if (request.RequestUri!.AbsolutePath == "/v2/streaming/hostname")
            {
                return new HttpResponseMessage(HttpStatusCode.OK)
                {
                    Content = new StringContent("QS30799.ingest.example.snowflakecomputing.com", Encoding.UTF8, "text/plain"),
                };
            }

            if (request.RequestUri.AbsolutePath == "/oauth/token")
            {
                return new HttpResponseMessage(HttpStatusCode.OK)
                {
                    Content = new StringContent("scoped-token-1", Encoding.UTF8, "text/plain"),
                };
            }

            return respondToDataCall(request, body);
        });
    }

    [Fact]
    public async Task OpenChannelAsync_SendsPutToChannelPath()
    {
        var handler = CreateStreamingHandler(
            (_, _) => new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent("""{"next_continuation_token":"token-1"}""", Encoding.UTF8, "application/json"),
            });
        using var httpClient = new HttpClient(handler) { BaseAddress = new Uri("https://qs30799.snowflakecomputing.com") };
        using var apiClient = new SnowflakeApiClient(CreateSettings(), httpClient);

        var channel = await apiClient.OpenChannelAsync("MYTESTTABLE__CLUEDIN_PIPE", "channel-1");

        Assert.Equal("token-1", channel.ContinuationToken);
        var request = handler.Requests.Last();
        Assert.Equal(HttpMethod.Put, request.Method);
        Assert.Equal("qs30799.ingest.example.snowflakecomputing.com", request.Uri.Host.ToLowerInvariant());
        Assert.Equal("/v2/streaming/databases/SNOWFLAKE_LEARNING_DB/schemas/TESTSCHEMA/pipes/MYTESTTABLE__CLUEDIN_PIPE/channels/channel-1", request.Uri.AbsolutePath);
        Assert.Equal("Bearer", request.AuthorizationScheme);
        Assert.Equal("scoped-token-1", request.AuthorizationParameter);
    }

    [Fact]
    public async Task AppendRowsAsync_SendsPostWithContinuationTokenAndNdjsonRows()
    {
        var handler = CreateStreamingHandler(
            (_, _) => new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent("""{"status_code":0,"next_continuation_token":"token-2"}""", Encoding.UTF8, "application/json"),
            });
        using var httpClient = new HttpClient(handler) { BaseAddress = new Uri("https://qs30799.snowflakecomputing.com") };
        using var apiClient = new SnowflakeApiClient(CreateSettings(), httpClient);
        var rows = new List<IReadOnlyDictionary<string, object>>
        {
            new Dictionary<string, object> { ["ENTITY_ID"] = "e1", ["CHANGE_TYPE"] = "Added" },
        };

        var channel = await apiClient.AppendRowsAsync("MYTESTTABLE__CLUEDIN_PIPE", "channel-1", "token-1", rows);

        Assert.Equal("token-2", channel.ContinuationToken);
        var request = handler.Requests.Last();
        Assert.Equal(HttpMethod.Post, request.Method);
        Assert.StartsWith(
            "/v2/streaming/data/databases/SNOWFLAKE_LEARNING_DB/schemas/TESTSCHEMA/pipes/MYTESTTABLE__CLUEDIN_PIPE/channels/channel-1/rows",
            request.Uri.AbsolutePath);
        Assert.Contains("continuationToken=token-1", request.Uri.Query);
        // NDJSON, not a JSON object wrapping a "rows" array.
        Assert.Equal("""{"ENTITY_ID":"e1","CHANGE_TYPE":"Added"}""", request.Body);
    }

    [Fact]
    public async Task CloseChannelAsync_SendsDelete()
    {
        var handler = CreateStreamingHandler(
            (_, _) => new HttpResponseMessage(HttpStatusCode.OK));
        using var httpClient = new HttpClient(handler) { BaseAddress = new Uri("https://qs30799.snowflakecomputing.com") };
        using var apiClient = new SnowflakeApiClient(CreateSettings(), httpClient);

        await apiClient.CloseChannelAsync("MYTESTTABLE__CLUEDIN_PIPE", "channel-1");

        var request = handler.Requests.Last();
        Assert.Equal(HttpMethod.Delete, request.Method);
        Assert.Equal("/v2/streaming/databases/SNOWFLAKE_LEARNING_DB/schemas/TESTSCHEMA/pipes/MYTESTTABLE__CLUEDIN_PIPE/channels/channel-1", request.Uri.AbsolutePath);
    }

    [Fact]
    public async Task ExecuteStatementAsync_WhenErrorResponse_ThrowsSnowflakeApiExceptionWithMessage()
    {
        var handler = new RecordingHttpMessageHandler(
            (_, _) => new HttpResponseMessage(HttpStatusCode.BadRequest)
            {
                Content = new StringContent("""{"message":"SQL compilation error"}""", Encoding.UTF8, "application/json"),
            });
        using var httpClient = new HttpClient(handler) { BaseAddress = new Uri("https://qs30799.snowflakecomputing.com") };
        using var apiClient = new SnowflakeApiClient(CreateSettings(), httpClient);

        var exception = await Assert.ThrowsAsync<SnowflakeApiException>(() => apiClient.ExecuteStatementAsync("SELECT * FROM missing"));

        Assert.Contains("SQL compilation error", exception.Message);
        Assert.Equal(HttpStatusCode.BadRequest, exception.StatusCode);
    }

    private sealed class RecordingHttpMessageHandler : HttpMessageHandler
    {
        private readonly Func<HttpRequestMessage, string, HttpResponseMessage> _respond;

        public RecordingHttpMessageHandler(Func<HttpRequestMessage, string, HttpResponseMessage> respond)
        {
            _respond = respond;
        }

        public List<RecordedRequest> Requests { get; } = new();

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var body = request.Content == null ? string.Empty : await request.Content.ReadAsStringAsync(cancellationToken);
            var headers = new Dictionary<string, string>();
            foreach (var header in request.Headers)
            {
                headers[header.Key] = string.Join(",", header.Value);
            }

            Requests.Add(new RecordedRequest(
                request.Method,
                request.RequestUri,
                body,
                request.Headers.Authorization?.Scheme,
                request.Headers.Authorization?.Parameter,
                headers));

            return _respond(request, body);
        }
    }

    private sealed record RecordedRequest(
        HttpMethod Method,
        Uri Uri,
        string Body,
        string AuthorizationScheme,
        string AuthorizationParameter,
        IReadOnlyDictionary<string, string> Headers)
    {
        public string this[string headerName] => Headers.TryGetValue(headerName, out var value) ? value : null;
    }
}
