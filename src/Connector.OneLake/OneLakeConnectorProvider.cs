using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using CluedIn.Connector.FileStorage.Common;
using CluedIn.Core;
using CluedIn.Core.Crawling;
using CluedIn.Core.Providers;

using Microsoft.Extensions.Logging;

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
    }; public override async Task<CrawlJobData> GetCrawlJobData(
        ProviderUpdateContext context, IDictionary<string, object> configuration, Guid organizationId, Guid userId, Guid providerDefinitionId)
    {
        var data = await base.GetCrawlJobData(context, configuration, organizationId, userId, providerDefinitionId);

        if (data is CrawlJobDataWrapper wrapper)
        {
            var hasWorkspaceName = wrapper.Configurations.TryGetValue(nameof(OneLakeConfigurationConstants.WorkspaceName), out var workspaceName);

            if (hasWorkspaceName && workspaceName is string workspaceNameString)
            {
                TrimWorkspaceName(wrapper, workspaceNameString);
            }

            var hasItemName = wrapper.Configurations.TryGetValue(nameof(OneLakeConfigurationConstants.ItemName), out var itemName);

            if (hasItemName && itemName is string itemNameString)
            {
                TrimItemName(wrapper, itemNameString);
            }

            var hasItemFolder = wrapper.Configurations.TryGetValue(nameof(OneLakeConfigurationConstants.ItemFolder), out var itemFolder);

            if (hasItemFolder && itemFolder is string itemFolderString)
            {
                TrimItemFolder(wrapper, itemFolderString);
            }
        }

        return data;

        static void TrimWorkspaceName(CrawlJobDataWrapper wrapper, string workspaceNameString)
        {
            wrapper.Configurations[nameof(OneLakeConfigurationConstants.WorkspaceName)] = workspaceNameString?.Trim();
        }

        static void TrimItemName(CrawlJobDataWrapper wrapper, string itemNameString)
        {
            wrapper.Configurations[nameof(OneLakeConfigurationConstants.ItemName)] = itemNameString?.Trim();
        }

        static void TrimItemFolder(CrawlJobDataWrapper wrapper, string itemFolderString)
        {
            wrapper.Configurations[nameof(OneLakeConfigurationConstants.ItemFolder)] = itemFolderString?.Trim();
        }
    }
}
