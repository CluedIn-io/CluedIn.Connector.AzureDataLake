using CluedIn.Connector.DataLake.Common;
using CluedIn.Core;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;

namespace CluedIn.Connector.FabricOpenMirroring;

public class OpenMirroringConnectorProvider : ConnectorProviderBase<OpenMirroringConnectorProvider>
{
    public OpenMirroringConnectorProvider([NotNull] ApplicationContext appContext,
        IOpenMirroringConstants configuration, ILogger<OpenMirroringConnectorProvider> logger)
        : base(appContext, configuration, logger)
    {
    }

    protected override IEnumerable<string> ProviderNameParts => new[]
    {
       OpenMirroringConstants.WorkspaceName,
       OpenMirroringConstants.MirroredDatabaseName,
       OpenMirroringConstants.ClientId,
       OpenMirroringConstants.TenantId,
    };
}
