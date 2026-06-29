using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using CluedIn.Core;
using CluedIn.Core.Streams.Models;

namespace CluedIn.Connector.FileStorage.Common;

public interface IStorageFactory
{
    Task<IStorageConfiguration> CreateStorageConfiguration(
        ExecutionContext executionContext,
        Guid providerDefinitionId);

    Task<IStorageConfiguration> CreateStorageConfiguration(
        ExecutionContext executionContext,
        IReadOnlyStreamModel streamModel);

    Task<IStorageConfiguration> CreateStorageConfiguration(
        ExecutionContext executionContext,
        IDictionary<string, object> authenticationDetails,
        string containerName = null);

    Task<IStorageClient> CreateStorageClient(
        ExecutionContext executionContext,
        IStorageConfiguration configuration);
}
