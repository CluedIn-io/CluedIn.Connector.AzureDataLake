using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using CluedIn.Connector.FileStorage.Common;
using CluedIn.Core;
using CluedIn.Core.Providers;

using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.AzureDataLake;

public class AzureDataLakeConnectorProvider : ConnectorProviderBase<AzureDataLakeConnectorProvider>
{
    public AzureDataLakeConnectorProvider([NotNull] ApplicationContext appContext,
        IAzureDataLakeConfigurationConstants configuration, ILogger<AzureDataLakeConnectorProvider> logger)
        : base(appContext, configuration, logger)
    {
    }

    protected override async Task TransformConfigurationAsync(ProviderUpdateContext context, IDictionary<string, object> configuration, Guid providerDefinitionId)
    {
        await base.TransformConfigurationAsync(context, configuration, providerDefinitionId);
        // Add default authentication method if not provided
        if (!configuration.ContainsKey(AzureDataLakeConfigurationConstants.AuthenticationMethod))
        {
            configuration.Add(AzureDataLakeConfigurationConstants.AuthenticationMethod, AuthenticationMethods.AccessKeyOrSasToken.ToString());
        }
    }

    protected override IEnumerable<string> ProviderNameParts => new[]
    {
        AzureDataLakeConfigurationConstants.AccountName,
        AzureDataLakeConfigurationConstants.FileSystemName,
        AzureDataLakeConfigurationConstants.DirectoryName,
    };
}
