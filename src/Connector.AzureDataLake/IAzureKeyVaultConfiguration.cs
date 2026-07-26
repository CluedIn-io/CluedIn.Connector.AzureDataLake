using CluedIn.Connector.DataLake.Common;

namespace CluedIn.Connector.AzureDataLake;

internal interface IAzureKeyVaultConfiguration : IDataLakeStorageConfiguration
{
    string KeyVaultUri { get; }

    string KeyVaultSecretName { get; }
}
