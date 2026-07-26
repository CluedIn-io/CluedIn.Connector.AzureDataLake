using CluedIn.Connector.FileStorage.Common;
// ReSharper disable ArgumentsStyleStringLiteral

namespace CluedIn.Connector.AzureDataLake;

public interface IAzureDataLakeConfigurationConstants : IStorageConfigurationConstants
{
    string WorkloadIdentityAuthenticationMethodEnabledKeyName { get; }

    bool WorkloadIdentityAuthenticationMethodEnabledDefaultValue { get; }
}
