using System;
using System.Collections.Generic;
using System.Web;
using Azure;
using Azure.Storage.Files.DataLake;
using CluedIn.Connector.DataLake.Common;
using CluedIn.Connector.FileStorage.Common;

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

    internal bool IsValidAccountKey()
    {
        return !string.IsNullOrWhiteSpace(AccountKey) && IsBase64String(AccountKey);
    }

    internal static bool IsSharedAccessKey(string sharedKeyOrSasToken)
    {
        if (IsBase64String(sharedKeyOrSasToken))
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
