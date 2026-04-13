using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using CluedIn.Core;
using CluedIn.Core.Streams.Models;

namespace CluedIn.Connector.FileStorage.Common;

public interface IDataLakeJobDataFactory
{
    Task<IDataLakeJobData> GetConfiguration(
        ExecutionContext executionContext,
        Guid providerDefinitionId);

    Task<IDataLakeJobData> GetConfiguration(
        ExecutionContext executionContext,
        IReadOnlyStreamModel streamModel);

    Task<IDataLakeJobData> GetConfiguration(
        ExecutionContext executionContext,
        IDictionary<string, object> authenticationDetails,
        string containerName = null);
}
