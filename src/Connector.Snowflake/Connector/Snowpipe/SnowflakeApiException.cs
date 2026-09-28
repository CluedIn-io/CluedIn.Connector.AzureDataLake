using System;
using System.Net;

namespace CluedIn.Connector.Snowflake.Connector.Snowpipe;

internal class SnowflakeApiException : Exception
{
    public SnowflakeApiException(string message, HttpStatusCode statusCode, string responseBody)
        : base($"Snowflake API call failed with status {(int)statusCode} ({statusCode}): {message}")
    {
        StatusCode = statusCode;
        ResponseBody = responseBody;
    }

    public HttpStatusCode StatusCode { get; }

    public string ResponseBody { get; }
}
