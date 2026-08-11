#if !CLUEDIN_V47_OR_GREATER
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using CluedIn.Core;
using CluedIn.Core.Streams;
using CluedIn.Core.Streams.Models;

namespace CluedIn.Connector.FileStorage.Common;

internal static partial class StreamRepositoryExtensions
{
    internal static partial async Task<StreamModel> GetStreamEx(this IStreamRepository streamRepository, ExecutionContext executionContext, Guid streamId)
    {
        return await streamRepository.GetStream(streamId);
    }

    internal static partial Task<IEnumerable<StreamModel>> GetAllStreamsEx(this IStreamRepository streamRepository, ExecutionContext executionContext)
    {
        return Task.FromResult(streamRepository.GetAllStreams());
    }

    internal static partial async Task<int> GetOrganizationStreamsCountEx(this IStreamRepository streamRepository, ExecutionContext executionContext, Guid providerDefinitionId)
    {
        return await streamRepository.GetOrganizationStreamsCount(executionContext.Organization.Id, filterConnectorProviderDefinitionId: providerDefinitionId);
    }

    internal static partial async Task<IEnumerable<StreamModel>> GetOrganizationStreamsEx(this IStreamRepository streamRepository, ExecutionContext executionContext, int page, int take, Guid providerDefinitionId)
    {
        return await streamRepository.GetOrganizationStreams(executionContext.Organization.Id, page: page, take: take, filterConnectorProviderDefinitionId: providerDefinitionId);
    }

    internal static partial async Task<IList<StreamMappingModel>> GetStreamMappingsEx(this IStreamRepository streamRepository, ExecutionContext executionContext, Guid id)
    {
        return await streamRepository.GetStreamMappings(id);
    }

    internal static partial Task SetupConnectorEx(this IStreamRepository streamRepository, ExecutionContext executionContext, Guid streamId, SetupConnectorModel model)
    {
        return streamRepository.SetupConnector(streamId, model, executionContext);
    }
}

#endif
