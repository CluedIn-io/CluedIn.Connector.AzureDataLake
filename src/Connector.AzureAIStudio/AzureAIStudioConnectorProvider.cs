using CluedIn.Connector.DataLake.Common;
using CluedIn.Core;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;

namespace CluedIn.Connector.AzureAIStudio;

public class AzureAIStudioConnectorProvider : ConnectorProviderBase<AzureAIStudioConnectorProvider>
{
    public AzureAIStudioConnectorProvider([NotNull] ApplicationContext appContext,
        IAzureAIStudioConstants configuration, ILogger<AzureAIStudioConnectorProvider> logger)
        : base(appContext, configuration, logger)
    {
    }

    protected override IEnumerable<string> ProviderNameParts => new[]
    {
       AzureAIStudioConstants.WorkspaceName,
       AzureAIStudioConstants.ItemFolder,
       AzureAIStudioConstants.ItemType,
       AzureAIStudioConstants.ItemName,
       AzureAIStudioConstants.ClientId,
       AzureAIStudioConstants.TenantId,
    };
}
