using CluedIn.Connector.FileStorage.Common;
using CluedIn.Core;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;

namespace CluedIn.Connector.AzureAIStudio;

public class AzureAIStudioConnectorProvider : ConnectorProviderBase<AzureAIStudioConnectorProvider>
{
    public AzureAIStudioConnectorProvider([NotNull] ApplicationContext appContext,
        IAzureAIStudioConfigurationConstants configuration, ILogger<AzureAIStudioConnectorProvider> logger)
        : base(appContext, configuration, logger)
    {
    }

    protected override IEnumerable<string> ProviderNameParts => new[]
    {
       AzureAIStudioConfigurationConstants.WorkspaceName,
       AzureAIStudioConfigurationConstants.ItemFolder,
       AzureAIStudioConfigurationConstants.ItemType,
       AzureAIStudioConfigurationConstants.ItemName,
       AzureAIStudioConfigurationConstants.ClientId,
       AzureAIStudioConfigurationConstants.TenantId,
    };
}
