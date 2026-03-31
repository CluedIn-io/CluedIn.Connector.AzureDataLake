using CluedIn.Connector.DataLake.Common;
using CluedIn.Connector.FileStorage.Common;
using CluedIn.Core;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;

namespace CluedIn.Connector.OneLake;

public class OneLakeConnectorProvider : ConnectorProviderBase<OneLakeConnectorProvider>
{
    public OneLakeConnectorProvider([NotNull] ApplicationContext appContext,
        IOneLakeConfigurationConstants configuration, ILogger<OneLakeConnectorProvider> logger)
        : base(appContext, configuration, logger)
    {
    }

    protected override IEnumerable<string> ProviderNameParts => new[]
    {
       OneLakeConfigurationConstants.WorkspaceName,
       OneLakeConfigurationConstants.ItemFolder,
       OneLakeConfigurationConstants.ItemType,
       OneLakeConfigurationConstants.ItemName,
       OneLakeConfigurationConstants.ClientId,
       OneLakeConfigurationConstants.TenantId,
    };
}
