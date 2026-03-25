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
        var orgDataStore = applicationContext.System.Organization.DataStores.GetDataStore<OrganizationProfile>();
        var organizationProfiles = await orgDataStore.SelectAsync(applicationContext.System.CreateExecutionContext(), _ => true);
        foreach (var organizationProfile in organizationProfiles)
        {
            var executionContext = applicationContext.CreateExecutionContext(organizationProfile.Id);

            foreach (var provider in executionContext.Organization.Providers.AllProviderDefinitions.Where(x =>
                             x.ProviderId == dataLakeConstants.ProviderId))
            {
                var streams = await streamRepository.GetAllStreams(executionContext);
                foreach (var stream in streams.Where(s => s.ConnectorProviderDefinitionId == provider.Id))
                {
                    await streamTask(executionContext, provider, stream);
                }
            }
        }
    }
}
