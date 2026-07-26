namespace CluedIn.Connector.DataLake.Common;

internal interface IAzureServicePrincipalCredentialConfiguration : IDataLakeStorageConfiguration
{
    string TenantId { get; }
    string ClientId { get; }
    string ClientSecret { get; }
}
