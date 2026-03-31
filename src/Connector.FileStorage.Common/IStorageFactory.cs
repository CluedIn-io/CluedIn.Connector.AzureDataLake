using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using CluedIn.Core;

namespace CluedIn.Connector.FileStorage.Common;

public interface IStorageFactory
{
    Task<IStorageConfiguration> CreateStorageConfiguration(
        ExecutionContext executionContext,
        Guid providerDefinitionId,
        string containerName);

    Task<IStorageConfiguration> CreateStorageConfiguration(
        ExecutionContext executionContext,
        IDictionary<string, object> authenticationDetails,
        string containerName = null);

    Task<IStorageClient> CreateStorageClient(
        ExecutionContext executionContext,
        IStorageConfiguration jobData);
}
