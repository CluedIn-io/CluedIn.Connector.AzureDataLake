using CluedIn.Connector.FileStorage.Common;
using CluedIn.Core;

using Microsoft.Extensions.Logging;

using System.Collections.Generic;

namespace CluedIn.Connector.SynapseDataEngineering;

public class SynapseDataEngineeringConnectorProvider : ConnectorProviderBase<SynapseDataEngineeringConnectorProvider>
{
    public SynapseDataEngineeringConnectorProvider([NotNull] ApplicationContext appContext,
        ISynapseDataEngineeringConfigurationConstants configuration, ILogger<SynapseDataEngineeringConnectorProvider> logger)
        : base(appContext, configuration, logger)
    {
    }

    protected override IEnumerable<string> ProviderNameParts => new[]
    {
       SynapseDataEngineeringConfigurationConstants.WorkspaceName,
       SynapseDataEngineeringConfigurationConstants.ItemFolder,
       SynapseDataEngineeringConfigurationConstants.ItemType,
       SynapseDataEngineeringConfigurationConstants.ItemName,
       SynapseDataEngineeringConfigurationConstants.ClientId,
       SynapseDataEngineeringConfigurationConstants.TenantId,
    };
}
