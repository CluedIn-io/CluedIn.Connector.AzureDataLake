using System.Collections.Generic;
using System.Threading.Tasks;
using System;
using CluedIn.Core;

namespace CluedIn.Connector.DataLake;

public interface IDataLakeJobDataFactory
{
    Task<IDataLakeJobData> GetConfiguration(
        ExecutionContext executionContext,
        Guid providerDefinitionId,
        string containerName);

    Task<IDataLakeJobData> GetConfiguration(
        ExecutionContext executionContext,
        IDictionary<string, object> authenticationDetails,
        string containerName = null);
}
