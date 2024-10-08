using CluedIn.Connector.DataLake.Common;
using CluedIn.Core;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;

namespace CluedIn.Connector.SynapseDataEngineering;

public class SynapseDataEngineeringConnectorProvider : ConnectorProviderBase<SynapseDataEngineeringConnectorProvider>
{
    public SynapseDataEngineeringConnectorProvider([NotNull] ApplicationContext appContext,
        ISynapseDataEngineeringConstants configuration, ILogger<SynapseDataEngineeringConnectorProvider> logger)
        : base(appContext, configuration, logger)
    {
    }

    protected override IEnumerable<string> ProviderNameParts => new[]
    {
       SynapseDataEngineeringConstants.WorkspaceName,
       SynapseDataEngineeringConstants.ItemFolder,
       SynapseDataEngineeringConstants.ItemType,
       SynapseDataEngineeringConstants.ItemName,
       SynapseDataEngineeringConstants.ClientId,
       SynapseDataEngineeringConstants.TenantId,
    };
}
