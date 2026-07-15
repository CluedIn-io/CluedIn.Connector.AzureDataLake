using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using CluedIn.Core;
using CluedIn.Core.Streams;
using CluedIn.Core.Streams.Models;

namespace CluedIn.Connector.FileStorage.Common;

internal static partial class StreamRepositoryExtensions
{
    internal static partial Task<StreamModel> GetStreamEx(this IStreamRepository streamRepository, ExecutionContext executionContext, Guid streamId);

    internal static partial Task<IEnumerable<StreamModel>> GetAllStreamsEx(this IStreamRepository streamRepository, ExecutionContext executionContext);

    internal static partial Task<int> GetOrganizationStreamsCountEx(this IStreamRepository streamRepository, ExecutionContext executionContext, Guid providerDefinitionId);

    internal static partial Task<IEnumerable<StreamModel>> GetOrganizationStreamsEx(this IStreamRepository streamRepository, ExecutionContext executionContext, int page, int take, Guid providerDefinitionId);

    internal static partial Task<IList<StreamMappingModel>> GetStreamMappingsEx(this IStreamRepository streamRepository, ExecutionContext executionContext, Guid id);

    internal static partial Task SetupConnectorEx(this IStreamRepository streamRepository, ExecutionContext executionContext, Guid streamId, SetupConnectorModel model);
}
