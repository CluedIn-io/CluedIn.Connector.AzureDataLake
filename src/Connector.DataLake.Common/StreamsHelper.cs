using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using CluedIn.Core;
using CluedIn.Core.Accounts;
using CluedIn.Core.Data.Relational;
using CluedIn.Core.Streams;
using CluedIn.Core.Streams.Models;

namespace CluedIn.Connector.DataLake.Common;

internal class StreamsHelper
{
    public static async Task ForEachStreamAsync(
        ApplicationContext applicationContext,
        IDataLakeConstants dataLakeConstants,
        Func<ExecutionContext, ProviderDefinition, StreamModel, Task> streamTask)
    {
        var streamRepository = applicationContext.Container.Resolve<IStreamRepository>();
        var streams = streamRepository.GetAllStreams().ToList();

        var organizationIds = streams.Select(s => s.OrganizationId).Distinct().ToArray();
        foreach (var orgId in organizationIds)
        {
            var org = new Organization(applicationContext, orgId);
            var executionContext = applicationContext.CreateExecutionContext(orgId);

            foreach (var provider in org.Providers.AllProviderDefinitions.Where(x =>
                         x.ProviderId == dataLakeConstants.ProviderId))
            {
                foreach (var stream in streams.Where(s => s.ConnectorProviderDefinitionId == provider.Id))
                {
                    await streamTask(executionContext, provider, stream);
                }
            }
        }
    }
}
