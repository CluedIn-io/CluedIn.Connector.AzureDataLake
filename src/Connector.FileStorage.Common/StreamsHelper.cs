using System;
using System.Linq;
using System.Threading.Tasks;

using CluedIn.Core;
using CluedIn.Core.Data.Relational;
using CluedIn.Core.Streams;
using CluedIn.Core.Streams.Models;

namespace CluedIn.Connector.FileStorage.Common;

internal class StreamsHelper
{
    public static async Task ForEachStreamAsync(
        ApplicationContext applicationContext,
        IStorageConfigurationConstants configurationConstants,
        Func<ExecutionContext, ProviderDefinition, StreamModel, Task> streamTask)
    {
        var streamRepository = applicationContext.Container.Resolve<IStreamRepository>();
        var orgDataStore = applicationContext.System.Organization.DataStores.GetDataStore<OrganizationProfile>();
        var organizationProfiles = await orgDataStore.SelectAsync(applicationContext.System.CreateExecutionContext(), _ => true);
        foreach (var organizationProfile in organizationProfiles)
        {
            var executionContext = applicationContext.CreateExecutionContext(organizationProfile.Id);

            foreach (var provider in executionContext.Organization.Providers.AllProviderDefinitions.Where(x =>
                             x.ProviderId == configurationConstants.ProviderId))
            {
                // IStreamRepository.GetAllStreams() takes no parameters and returns
                // IEnumerable<StreamModel> synchronously in CluedIn.Core 4.6.0 - the
                // ExecutionContext-taking async overload doesn't exist until 4.7.
#if CLUEDIN_V47
                var streams = await streamRepository.GetAllStreams(executionContext);
#else
                var streams = streamRepository.GetAllStreams();
#endif
                foreach (var stream in streams.Where(s => s.ConnectorProviderDefinitionId == provider.Id))
                {
                    await streamTask(executionContext, provider, stream);
                }
            }
        }
    }
}
