namespace CluedIn.Connector.DataLake.Common;

internal interface IAzureSharedKeyCredentialConfiguration : IDataLakeStorageConfiguration
{
    string AccountName { get; }
    string AccountKey { get; }
}
