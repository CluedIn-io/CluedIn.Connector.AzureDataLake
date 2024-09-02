using CluedIn.Connector.DataLake.Common;
using CluedIn.Core;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;

namespace CluedIn.Connector.AzureDatabricks;

public class AzureDatabricksConnectorProvider : ConnectorProviderBase<AzureDatabricksConnectorProvider>
{
    public AzureDatabricksConnectorProvider([NotNull] ApplicationContext appContext,
        IAzureDatabricksConstants configuration, ILogger<AzureDatabricksConnectorProvider> logger)
        : base(appContext, configuration, logger)
    {
    }

    protected override IEnumerable<string> ProviderNameParts => new[]
    {
        AzureDatabricksConstants.AccountName,
        AzureDatabricksConstants.FileSystemName,
        AzureDatabricksConstants.DirectoryName,
    };
}
