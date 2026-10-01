using System;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace CluedIn.Connector.Snowflake.Connector.Snowpipe;

internal static class SnowflakeJwtTokenBuilder
{
    public static RSA LoadPrivateKey(string pem, string passphrase)
    {
        var rsa = RSA.Create();
        if (string.IsNullOrEmpty(passphrase))
        {
            rsa.ImportFromPem(pem);
        }
        else
        {
            rsa.ImportFromEncryptedPem(pem, passphrase);
        }

        return rsa;
    }

    public static string BuildToken(string account, string user, RSA privateKey, DateTimeOffset now, TimeSpan lifetime)
    {
        var accountIdentifier = NormalizeAccount(account).ToUpperInvariant();
        var userIdentifier = user.Trim().ToUpperInvariant();
        var publicKeyFingerprint = ComputePublicKeyFingerprint(privateKey);
        var qualifiedUser = $"{accountIdentifier}.{userIdentifier}";

        var headerJson = JsonSerializer.Serialize(new { alg = "RS256", typ = "JWT" });
        var payloadJson = JsonSerializer.Serialize(new
        {
            iss = $"{qualifiedUser}.SHA256:{publicKeyFingerprint}",
            sub = qualifiedUser,
            iat = now.ToUnixTimeSeconds(),
            exp = now.Add(lifetime).ToUnixTimeSeconds(),
        });

        var encodedHeader = Base64UrlEncode(Encoding.UTF8.GetBytes(headerJson));
        var encodedPayload = Base64UrlEncode(Encoding.UTF8.GetBytes(payloadJson));
        var signingInput = $"{encodedHeader}.{encodedPayload}";
        var signature = privateKey.SignData(Encoding.UTF8.GetBytes(signingInput), HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);

        return $"{signingInput}.{Base64UrlEncode(signature)}";
    }

    // Snowflake's JWT issuer/subject use only the account locator segment (e.g. "qs30799"),
    // not a full host name - a caller may pass either form ("qs30799" or
    // "qs30799.snowflakecomputing.com" or "orgname-accountname"), so trim at the first '.'.
    internal static string NormalizeAccount(string account)
    {
        var trimmed = account.Trim();
        var dotIndex = trimmed.IndexOf('.');
        return dotIndex > 0 ? trimmed[..dotIndex] : trimmed;
    }

    internal static string ComputePublicKeyFingerprint(RSA privateKey)
    {
        var publicKeyDer = privateKey.ExportSubjectPublicKeyInfo();
        var hash = SHA256.HashData(publicKeyDer);
        return Convert.ToBase64String(hash);
    }

    internal static string Base64UrlEncode(byte[] input)
    {
        return Convert.ToBase64String(input)
            .TrimEnd('=')
            .Replace('+', '-')
            .Replace('/', '_');
    }
}
