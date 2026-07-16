using CluedIn.Connector.FileStorage.Common;
using CluedIn.Core;
using CluedIn.Core.Crawling;
using CluedIn.Core.Providers;

using Microsoft.Extensions.Logging;

using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace CluedIn.Connector.FabricOpenMirroring;

public class OpenMirroringConnectorProvider : ConnectorProviderBase<OpenMirroringConnectorProvider>
{
    public OpenMirroringConnectorProvider([NotNull] ApplicationContext appContext,
        IOpenMirroringConfigurationConstants configuration, ILogger<OpenMirroringConnectorProvider> logger)
        : base(appContext, configuration, logger)
    {
    }

    protected override IEnumerable<string> ProviderNameParts => new[]
    {
       OpenMirroringConfigurationConstants.WorkspaceName,
       OpenMirroringConfigurationConstants.MirroredDatabaseName,
       OpenMirroringConfigurationConstants.ClientId,
       OpenMirroringConfigurationConstants.TenantId,
    };

    public override async Task<CrawlJobData> GetCrawlJobData(
        ProviderUpdateContext context, IDictionary<string, object> configuration, Guid organizationId, Guid userId, Guid providerDefinitionId)
    {
        var data = await base.GetCrawlJobData(context, configuration, organizationId, userId, providerDefinitionId);

        if (data is CrawlJobDataWrapper wrapper)
        {
            if (!wrapper.Configurations.TryGetValue(nameof(OpenMirroringConfigurationConstants.MirroredDatabaseName), out var dbName)
                || (dbName is string dbNameString && string.IsNullOrWhiteSpace(dbNameString)))
            {
                wrapper.Configurations[nameof(OpenMirroringConfigurationConstants.MirroredDatabaseName)] = $"CluedIn_ExportTarget_{providerDefinitionId:N}";
            }
        }

        return data;
    }
}
