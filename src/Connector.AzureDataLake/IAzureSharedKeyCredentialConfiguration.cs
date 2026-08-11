using CluedIn.Connector.DataLake.Common;

namespace CluedIn.Connector.AzureDataLake;

internal interface IAzureSharedKeyCredentialConfiguration : IDataLakeStorageConfiguration
{
    string AccountKey { get; }
}
