using System;
using System.Threading.Tasks;
using System.Web;

using Azure;
using Azure.Core;
using Azure.Identity;
using Azure.Security.KeyVault.Secrets;
using Azure.Storage;
using Azure.Storage.Files.DataLake;

using CluedIn.Connector.DataLake.Common.Connector;
using CluedIn.Core.Configuration;

using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.AzureDataLake.Connector;

internal class AzureDataLakeStorageClient : DataLakeStorageClient
{
    private readonly ILogger<AzureDataLakeStorageClient> _logger;
    private readonly AzureDataLakeConnectorConfiguration _storageConfiguration;
    private readonly IAzureDataLakeConfigurationConstants _constants;
    private readonly bool _shouldEnableWorkloadIdentity;

    public AzureDataLakeStorageClient(ILogger<AzureDataLakeStorageClient> logger, AzureDataLakeConnectorConfiguration storageConfiguration, IAzureDataLakeConfigurationConstants constants)
        : base (logger, storageConfiguration)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _storageConfiguration = storageConfiguration ?? throw new ArgumentNullException(nameof(storageConfiguration));
        _constants = constants ?? throw new ArgumentNullException(nameof(constants));
        _shouldEnableWorkloadIdentity = ConfigurationManagerEx.AppSettings.GetValue(
            constants.WorkloadIdentityAuthenticationMethodEnabledKeyName,
            constants.WorkloadIdentityAuthenticationMethodEnabledDefaultValue);
    }

    protected override async Task<DataLakeServiceClient> GetDataLakeServiceClientAsync()
    {
        // Determine the authentication method based on the configuration
        var hasParsedAuthenticationMethod = Enum.TryParse<AuthenticationMethods>(_storageConfiguration.AuthenticationMethod, out var authenticationMethod);

        if (!hasParsedAuthenticationMethod || authenticationMethod == AuthenticationMethods.AccessKeyOrSasToken)
        {
            return await GetDataLakeServiceClientAsync((IAzureSharedKeyCredentialConfiguration)_storageConfiguration);
        }

        var tokenCredential = authenticationMethod switch
        {
            AuthenticationMethods.ServicePrincipal => GetTokenCredential(_storageConfiguration),
            AuthenticationMethods.WorkloadIdentity => _shouldEnableWorkloadIdentity
                ? new WorkloadIdentityCredential()
                : throw new NotSupportedException("Workload Identity authentication method is not enabled."),
            _ => throw new NotSupportedException($"Unable to create datalake service client from selected method '{authenticationMethod}'"),
        };

        if (_storageConfiguration.UseKeyVault)
        {
            var accountKeyOrSasToken = await GetAccountKeyOrSasTokenUsingKeyVaultAsync((IAzureKeyVaultConfiguration)_storageConfiguration, tokenCredential);
            return await GetDataLakeServiceClientAsync(_storageConfiguration.AccountName, accountKeyOrSasToken);
        }
        else
        {
            return await GetDataLakeServiceClientAsync(tokenCredential);
        }
    }

    private async Task<string> GetAccountKeyOrSasTokenUsingKeyVaultAsync(IAzureKeyVaultConfiguration keyVaultConfiguration, TokenCredential tokenCredential)
    {
        var keyVaultClient = new SecretClient(vaultUri: new Uri(keyVaultConfiguration.KeyVaultUri), credential: tokenCredential);

        var secret = await keyVaultClient.GetSecretAsync(keyVaultConfiguration.KeyVaultSecretName);
        return secret.Value.Value;
    }

    private Task<DataLakeServiceClient> GetDataLakeServiceClientAsync(IAzureSharedKeyCredentialConfiguration sharedKeyCredential)
    {
        return GetDataLakeServiceClientAsync(sharedKeyCredential.AccountName, sharedKeyCredential.AccountKey);
    }

    private async Task<DataLakeServiceClient> GetDataLakeServiceClientAsync(string accountName, string accountKeyOrSasToken)
    {
        if (AzureDataLakeConnectorConfiguration.IsSharedAccessKey(accountKeyOrSasToken))
        {
            return new DataLakeServiceClient(
                new Uri(await GetStorageUrlAsync()),
                new StorageSharedKeyCredential(
                    accountName,
                    accountKeyOrSasToken));
        }

        return new DataLakeServiceClient(
            new Uri(await GetStorageUrlAsync()),
            new AzureSasCredential(accountKeyOrSasToken));
    }
}
