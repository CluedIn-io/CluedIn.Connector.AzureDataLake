using System.Collections.Generic;

using CluedIn.Connector.FileStorage.Common;
using CluedIn.Core;

using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.AzureDataLake;

public class AzureDataLakeConnectorProvider : ConnectorProviderBase<AzureDataLakeConnectorProvider>
{
    public AzureDataLakeConnectorProvider([NotNull] ApplicationContext appContext,
        IAzureDataLakeConfigurationConstants configuration, ILogger<AzureDataLakeConnectorProvider> logger)
        : base(appContext, configuration, logger)
    {
    }

    protected override IEnumerable<string> ProviderNameParts => new[]
    {
        AzureDataLakeConfigurationConstants.AccountName,
        AzureDataLakeConfigurationConstants.FileSystemName,
        AzureDataLakeConfigurationConstants.DirectoryName,
    };
}
