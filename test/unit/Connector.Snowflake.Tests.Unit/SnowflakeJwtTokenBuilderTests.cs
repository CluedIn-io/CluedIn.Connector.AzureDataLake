using System;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

using CluedIn.Connector.Snowflake.Connector.Snowpipe;

using Xunit;

namespace CluedIn.Connector.Snowflake.Tests.Unit;

public class SnowflakeJwtTokenBuilderTests
{
    [Theory]
    [InlineData("qs30799", "QS30799")]
    [InlineData("qs30799.snowflakecomputing.com", "QS30799")]
    [InlineData("orgname-accountname", "ORGNAME-ACCOUNTNAME")]
    [InlineData("  qs30799  ", "QS30799")]
    public void NormalizeAccount_StripsHostSuffixAndTrims(string input, string expectedUppercased)
    {
        var normalized = SnowflakeJwtTokenBuilder.NormalizeAccount(input);
        Assert.Equal(expectedUppercased, normalized.ToUpperInvariant());
    }

    [Fact]
    public void Base64UrlEncode_ProducesUrlSafeStringWithoutPadding()
    {
        var input = new byte[] { 0xFB, 0xFF, 0xFE };
        var encoded = SnowflakeJwtTokenBuilder.Base64UrlEncode(input);

        Assert.DoesNotContain("+", encoded);
        Assert.DoesNotContain("/", encoded);
        Assert.DoesNotContain("=", encoded);
    }

    [Fact]
    public void ComputePublicKeyFingerprint_IsStableForSameKey()
    {
        using var rsa = RSA.Create(2048);
        var first = SnowflakeJwtTokenBuilder.ComputePublicKeyFingerprint(rsa);
        var second = SnowflakeJwtTokenBuilder.ComputePublicKeyFingerprint(rsa);

        Assert.Equal(first, second);
        Assert.NotEmpty(first);
    }

    [Fact]
    public void BuildToken_ProducesValidRs256JwtWithExpectedClaims()
    {
        using var rsa = RSA.Create(2048);
        var now = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var lifetime = TimeSpan.FromMinutes(59);

        var token = SnowflakeJwtTokenBuilder.BuildToken("qs30799", "cluedin_svc", rsa, now, lifetime);

        var parts = token.Split('.');
        Assert.Equal(3, parts.Length);

        var header = JsonDocument.Parse(Base64UrlDecode(parts[0]));
        Assert.Equal("RS256", header.RootElement.GetProperty("alg").GetString());
        Assert.Equal("JWT", header.RootElement.GetProperty("typ").GetString());

        var payload = JsonDocument.Parse(Base64UrlDecode(parts[1]));
        var fingerprint = SnowflakeJwtTokenBuilder.ComputePublicKeyFingerprint(rsa);
        Assert.Equal($"QS30799.CLUEDIN_SVC.SHA256:{fingerprint}", payload.RootElement.GetProperty("iss").GetString());
        Assert.Equal("QS30799.CLUEDIN_SVC", payload.RootElement.GetProperty("sub").GetString());
        Assert.Equal(now.ToUnixTimeSeconds(), payload.RootElement.GetProperty("iat").GetInt64());
        Assert.Equal(now.Add(lifetime).ToUnixTimeSeconds(), payload.RootElement.GetProperty("exp").GetInt64());

        var signingInput = Encoding.UTF8.GetBytes($"{parts[0]}.{parts[1]}");
        var signature = Base64UrlDecodeBytes(parts[2]);
        var isValidSignature = rsa.VerifyData(signingInput, signature, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        Assert.True(isValidSignature);
    }

    private static string Base64UrlDecode(string value) => Encoding.UTF8.GetString(Base64UrlDecodeBytes(value));

    private static byte[] Base64UrlDecodeBytes(string value)
    {
        var padded = value.Replace('-', '+').Replace('_', '/');
        switch (padded.Length % 4)
        {
            case 2: padded += "=="; break;
            case 3: padded += "="; break;
        }

        return Convert.FromBase64String(padded);
    }
}
