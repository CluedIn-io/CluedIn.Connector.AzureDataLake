using CluedIn.Connector.DataLake.Common;
using CluedIn.Core;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;

namespace CluedIn.Connector.AzureDataLake;

public class AzureDataLakeConnectorProvider : ConnectorProviderBase<AzureDataLakeConnectorProvider>
{
    public AzureDataLakeConnectorProvider([NotNull] ApplicationContext appContext,
        IAzureDataLakeConstants configuration, ILogger<AzureDataLakeConnectorProvider> logger)
        : base(appContext, configuration, logger)
    {
    }

    protected override IEnumerable<string> ProviderNameParts => new[]
    {
        AzureDataLakeConstants.AccountName,
        AzureDataLakeConstants.FileSystemName,
        AzureDataLakeConstants.DirectoryName,
    };
}
