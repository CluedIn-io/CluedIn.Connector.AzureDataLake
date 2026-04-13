using CluedIn.Connector.FileStorage.Common;
using CluedIn.Core;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;

namespace CluedIn.Connector.AzureDatabricks;

public class AzureDatabricksConnectorProvider : ConnectorProviderBase<AzureDatabricksConnectorProvider>
{
    public AzureDatabricksConnectorProvider([NotNull] ApplicationContext appContext,
        IAzureDatabricksConfigurationConstants configuration, ILogger<AzureDatabricksConnectorProvider> logger)
        : base(appContext, configuration, logger)
    {
    }

    protected override IEnumerable<string> ProviderNameParts => new[]
    {
       AzureDatabricksConfigurationConstants.WorkspaceName,
       AzureDatabricksConfigurationConstants.ItemFolder,
       AzureDatabricksConfigurationConstants.ItemType,
       AzureDatabricksConfigurationConstants.ItemName,
       AzureDatabricksConfigurationConstants.ClientId,
       AzureDatabricksConfigurationConstants.TenantId,
    };
}
