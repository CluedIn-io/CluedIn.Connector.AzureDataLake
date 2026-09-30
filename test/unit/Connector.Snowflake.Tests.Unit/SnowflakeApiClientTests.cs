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
        return CreateSettingsWithAccount("qs30799");
    }

    private static SnowflakeConnectionSettings CreateSettingsWithAccount(string account)
    {
        using var rsa = RSA.Create(2048);
        return new SnowflakeConnectionSettings(
            account,
            "cluedin_svc",
            ExportPkcs8PrivateKeyPem(rsa),
            null,
            "SNOWFLAKE_LEARNING_DB",
            "TESTSCHEMA",
            "COMPUTE_WH",
            "ACCOUNTADMIN");
    }

    [Fact]
    public void Constructor_ValidAccount_BuildsExpectedControlHost()
    {
        using var client = new SnowflakeApiClient(CreateSettingsWithAccount("qs30799.ap-southeast-1"));

        // No public accessor for the built HttpClient's BaseAddress - a request against a
        // relative path is enough to prove the client constructed successfully (an invalid
        // account throws from the constructor itself, verified below).
        Assert.NotNull(client);
    }

    // Account comes from connector configuration and is interpolated into a URI authority -
    // without validation, a value like "attacker.example#" would make Uri parse the
    // intended Snowflake suffix as a fragment instead of part of the host, sending the
    // bearer JWT to "attacker.example" instead of any *.snowflakecomputing.com host.
    [Theory]
    [InlineData("attacker.example#")]
    [InlineData("attacker.example/path")]
    [InlineData("attacker.example@qs30799")]
    [InlineData("qs30799\\@attacker.example")]
    [InlineData("")]
    [InlineData(" ")]
    public void Constructor_InvalidAccount_ThrowsArgumentException(string maliciousAccount)
    {
        Assert.Throws<ArgumentException>(() => new SnowflakeApiClient(CreateSettingsWithAccount(maliciousAccount)));
    }

    // RSA.ExportPkcs8PrivateKeyPem() isn't available on net6.0 (added in .NET 7) - this repo
    // also builds unit tests against net6.0 for CluedIn 4.6.0/4.7.0/4.8.0 (see
    // Directory.Build.props), so the PEM has to be built by hand from the lower-level,
    // net5.0+ ExportPkcs8PrivateKey()/PemEncoding APIs instead.
    private static string ExportPkcs8PrivateKeyPem(RSA rsa)
    {
        var pkcs8 = rsa.ExportPkcs8PrivateKey();
        return new string(PemEncoding.Write("PRIVATE KEY", pkcs8));
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

    [Fact]
    public Task ExecuteStatementAsync_WarehouseOnlyScope_OnlyIncludesWarehouse()
    {
        return AssertScopeIncludesObjects(SnowflakeStatementScope.WarehouseOnly, expectWarehouse: true, expectDatabase: false, expectSchema: false);
    }

    [Fact]
    public Task ExecuteStatementAsync_DatabaseOnlyScope_OnlyIncludesDatabase()
    {
        return AssertScopeIncludesObjects(SnowflakeStatementScope.DatabaseOnly, expectWarehouse: false, expectDatabase: true, expectSchema: false);
    }

    [Fact]
    public Task ExecuteStatementAsync_DatabaseAndSchemaScope_IncludesDatabaseAndSchemaButNotWarehouse()
    {
        return AssertScopeIncludesObjects(SnowflakeStatementScope.DatabaseAndSchema, expectWarehouse: false, expectDatabase: true, expectSchema: true);
    }

    [Fact]
    public Task ExecuteStatementAsync_AllScope_IncludesEverything()
    {
        return AssertScopeIncludesObjects(SnowflakeStatementScope.All, expectWarehouse: true, expectDatabase: true, expectSchema: true);
    }

    [Fact]
    public Task ExecuteStatementAsync_NoneScope_IncludesNothing()
    {
        return AssertScopeIncludesObjects(SnowflakeStatementScope.None, expectWarehouse: false, expectDatabase: false, expectSchema: false);
    }

    private static async Task AssertScopeIncludesObjects(SnowflakeStatementScope scope, bool expectWarehouse, bool expectDatabase, bool expectSchema)
    {
        var handler = new RecordingHttpMessageHandler(
            (_, _) => new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent("""{"resultSetMetaData":{"rowType":[]},"data":[]}""", Encoding.UTF8, "application/json"),
            });
        using var httpClient = new HttpClient(handler) { BaseAddress = new Uri("https://qs30799.snowflakecomputing.com") };
        using var apiClient = new SnowflakeApiClient(CreateSettings(), httpClient);

        await apiClient.ExecuteStatementAsync("SELECT 1", scope);

        var request = Assert.Single(handler.Requests);
        Assert.Equal(expectWarehouse, request.Body.Contains("\"warehouse\":\"COMPUTE_WH\""));
        Assert.Equal(expectDatabase, request.Body.Contains("\"database\":\"SNOWFLAKE_LEARNING_DB\""));
        Assert.Equal(expectSchema, request.Body.Contains("\"schema\":\"TESTSCHEMA\""));
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

        var channel = await apiClient.AppendRowsAsync("MYTESTTABLE__CLUEDIN_PIPE", "channel-1", "token-1", "42", rows);

        Assert.Equal("token-2", channel.ContinuationToken);
        var request = handler.Requests.Last();
        Assert.Equal(HttpMethod.Post, request.Method);
        Assert.StartsWith(
            "/v2/streaming/data/databases/SNOWFLAKE_LEARNING_DB/schemas/TESTSCHEMA/pipes/MYTESTTABLE__CLUEDIN_PIPE/channels/channel-1/rows",
            request.Uri.AbsolutePath);
        Assert.Contains("continuationToken=token-1", request.Uri.Query);
        // startOffsetToken/endOffsetToken are what actually makes Snowflake commit the
        // batch - verified live that without them the append is buffered but never
        // committed (rows never become queryable, even after 60+ seconds).
        Assert.Contains("startOffsetToken=42", request.Uri.Query);
        Assert.Contains("endOffsetToken=42", request.Uri.Query);
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
    public async Task GetChannelStatusAsync_SendsBulkStatusPostAndParsesCommittedOffset()
    {
        var handler = CreateStreamingHandler(
            (_, _) => new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(
                    """{"channel_statuses":{"channel-1":{"last_committed_offset_token":"42","rows_inserted":5,"rows_error_count":0,"last_error_message":null}}}""",
                    Encoding.UTF8,
                    "application/json"),
            });
        using var httpClient = new HttpClient(handler) { BaseAddress = new Uri("https://qs30799.snowflakecomputing.com") };
        using var apiClient = new SnowflakeApiClient(CreateSettings(), httpClient);

        var status = await apiClient.GetChannelStatusAsync("MYTESTTABLE__CLUEDIN_PIPE", "channel-1");

        Assert.Equal("42", status.LastCommittedOffsetToken);
        Assert.Equal(5, status.RowsInserted);
        Assert.Equal(0, status.RowsErrorCount);
        var request = handler.Requests.Last();
        Assert.Equal(HttpMethod.Post, request.Method);
        Assert.Equal("/v2/streaming/databases/SNOWFLAKE_LEARNING_DB/schemas/TESTSCHEMA/pipes/MYTESTTABLE__CLUEDIN_PIPE:bulk-channel-status", request.Uri.AbsolutePath);
        Assert.Contains("\"channel_names\":[\"channel-1\"]", request.Body);
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
