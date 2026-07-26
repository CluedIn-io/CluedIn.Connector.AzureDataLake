using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using Azure;
using Azure.Storage.Files.DataLake;
using CluedIn.Connector.DataLake.Common;
using CluedIn.Connector.FileStorage.Common;
using CluedIn.Core;

namespace CluedIn.Connector.AzureDataLake;

internal class AzureDataLakeConnectorConfiguration
    : StorageConfigurationBase,
    IAzureSharedKeyCredentialConfiguration,
    IAzureServicePrincipalCredentialConfiguration,
    IAzureKeyVaultConfiguration
{
    public AzureDataLakeConnectorConfiguration(
        IDictionary<string, object> configurations,
        string containerName = null)
        : base(configurations, containerName)
    {
    }

    public string AccountName => GetConfigurationValue(AzureDataLakeConfigurationConstants.AccountName) as string;

    public string DirectoryName => GetConfigurationValue(AzureDataLakeConfigurationConstants.DirectoryName) as string;

    public virtual string FileSystemName => GetConfigurationValue(AzureDataLakeConfigurationConstants.FileSystemName) as string;

    public string AuthenticationMethod => GetConfigurationValue(AzureDataLakeConfigurationConstants.AuthenticationMethod) as string ?? AuthenticationMethods.SharedKey.ToString();

    // ISharedKeyCredentialConfiguration implementation
    public string AccountKey => GetConfigurationValue(AzureDataLakeConfigurationConstants.AccountKey) as string;

    // IAzureServicePrincipalCredentialConfiguration implementation
    public string TenantId => GetConfigurationValue(AzureDataLakeConfigurationConstants.TenantId) as string;

    public string ClientId => GetConfigurationValue(AzureDataLakeConfigurationConstants.ClientId) as string;

    public string ClientSecret => GetConfigurationValue(AzureDataLakeConfigurationConstants.ClientSecret) as string;

    // IAzureKeyVaultConfiguration implementation
    public bool UseKeyVault => GetConfigurationValue(AzureDataLakeConfigurationConstants.UseKeyVault) as bool? ?? false;

    public string KeyVaultUri => GetConfigurationValue(AzureDataLakeConfigurationConstants.KeyVaultUri) as string;

    public string KeyVaultSecretName => GetConfigurationValue(AzureDataLakeConfigurationConstants.KeyVaultSecretName) as string;

    public override string RootDirectoryPath => DirectoryName;

    public string StorageUri => $"https://{AccountName}.dfs.core.windows.net";

    protected override void AddToHashCode(HashCode hash)
    {
        hash.Add(AccountName);
        hash.Add(FileSystemName);
        hash.Add(DirectoryName);
        hash.Add(AuthenticationMethod);

        // IAzureSharedKeyCredentialConfiguration implementation
        hash.Add(AccountKey);

        // IAzureServicePrincipalCredentialConfiguration implementation
        hash.Add(TenantId);
        hash.Add(ClientId);
        hash.Add(ClientSecret);

        // ISelectableCredentialConfiguration implementation

        // IAzureKeyVaultConfiguration implementation
        hash.Add(UseKeyVault);
        hash.Add(KeyVaultUri);
        hash.Add(KeyVaultSecretName);

        base.AddToHashCode(hash);
    }

    public override bool Equals(object obj)
    {
        return Equals(obj as AzureDataLakeConnectorConfiguration);
    }

    public bool Equals(AzureDataLakeConnectorConfiguration other)
    {
        return other != null &&
            AccountName == other.AccountName &&
            FileSystemName == other.FileSystemName &&
            DirectoryName == other.DirectoryName &&
            AuthenticationMethod == other.AuthenticationMethod &&
            AccountKey == other.AccountKey &&
            TenantId == other.TenantId &&
            ClientId == other.ClientId &&
            ClientSecret == other.ClientSecret &&
            UseKeyVault == other.UseKeyVault &&
            KeyVaultUri == other.KeyVaultUri &&
            KeyVaultSecretName == other.KeyVaultSecretName &&
            base.Equals(other);
    }

    public override int GetHashCode()
    {
        return base.GetHashCode();
    }

    private static bool IsBase64String(string base64)
    {
        var buffer = new Span<byte>(new byte[base64.Length]);
        return Convert.TryFromBase64String(base64, buffer, out _);
    }

    internal bool IsValidSharedKey()
    {
        return !string.IsNullOrWhiteSpace(AccountKey) && IsBase64String(AccountKey);
    }

    internal bool IsValidSasTokenTime(IDateTimeOffsetProvider dateTimeOffsetProvider)
    {
        if (string.IsNullOrWhiteSpace(AccountKey))
        {
            return false;
        }

        try
        {
            var queryParameters = HttpUtility.ParseQueryString(AccountKey); // Validate the SAS token format
            var st = queryParameters["st"];
            var se = queryParameters["se"];

            // st is optional, but if it is present, it must be a valid DateTimeOffset and less than se. If st is not present, we assume the SAS token is valid.
            if (se == null)
            {
                return false;
            }

            var now = dateTimeOffsetProvider.GetCurrentUtcTime();
            var parsedEnd = DateTimeOffset.MinValue;
            var isValidEnd = DateTimeOffset.TryParse(se, out parsedEnd) && parsedEnd.ToUniversalTime() > now; // Validate the end time format

            // Validate the start time format
            // If st is null, we consider it valid.
            // If st is not null, we check if it is a valid DateTimeOffset, later than the current time and less than se
            var isValidStart = st == null ||
                (DateTimeOffset.TryParse(st, out var parsedStart) &&
                    parsedStart < parsedEnd &&
                    parsedStart.ToUniversalTime() <= now);

            return isValidStart && isValidEnd;
        }
        catch
        {
            return false;
        }
    }

    internal bool IsValidSasTokenPermissions()
    {
        if (string.IsNullOrWhiteSpace(AccountKey))
        {
            return false;
        }

        try
        {
            var queryParameters = HttpUtility.ParseQueryString(AccountKey); // Validate the SAS token format
            var sp = queryParameters["sp"];
            var ss = queryParameters["ss"];
            var srt = queryParameters["srt"];

            if (sp ==  null || ss == null || srt == null)
            {
                return false;
            }

            if ("rwdlc".Any(permission => !sp.Contains(permission)))
            {
                return false;
            }

            if (!ss.Contains("b"))
            {
                return false;
            }

            if ("co".Any(resourceType => !srt.Contains(resourceType)))
            {
                return false;
            }


            return true;
        }
        catch
        {
            return false;
        }
    }

    internal static bool IsSharedAccessKey(string sharedKeyOrSasToken)
    {
        // If the string is null, empty, or whitespace, or if it is a valid Base64 string, we consider it a shared access key.
        // This is to preserve backward compatibility with existing behaviour that only supports shared access keys and not SAS tokens.
        if (string.IsNullOrWhiteSpace(sharedKeyOrSasToken) || IsBase64String(sharedKeyOrSasToken))
        {
            return true;
        }

        return !IsSasToken(sharedKeyOrSasToken);
    }

    private static bool IsSasToken(string sharedKeyOrSasToken)
    {
        try
        {
            var queryParameters = HttpUtility.ParseQueryString(sharedKeyOrSasToken); // Validate the SAS token format
            return queryParameters.Count > 1 &&
                   queryParameters["sig"] != null &&
                   queryParameters["sv"] != null;
        }
        catch
        {
            return false;
        }
    }
}
