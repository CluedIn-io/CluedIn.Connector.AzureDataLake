using CluedIn.Connector.DataLake.Common;
using CluedIn.Core;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;

namespace CluedIn.Connector.FabricMirroring;

public class FabricMirroringConnectorProvider : ConnectorProviderBase<FabricMirroringConnectorProvider>
{
    public FabricMirroringConnectorProvider([NotNull] ApplicationContext appContext,
        IFabricMirroringConstants configuration, ILogger<FabricMirroringConnectorProvider> logger)
        : base(appContext, configuration, logger)
    {
    }

    protected override IEnumerable<string> ProviderNameParts => new[]
    {
       FabricMirroringConstants.WorkspaceName,
       FabricMirroringConstants.ItemFolder,
       FabricMirroringConstants.ItemType,
       FabricMirroringConstants.ItemName,
       FabricMirroringConstants.ClientId,
       FabricMirroringConstants.TenantId,
    };
}
