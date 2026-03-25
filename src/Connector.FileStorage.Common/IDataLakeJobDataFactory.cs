using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using CluedIn.Core;

namespace CluedIn.Connector.DataLake.Common;

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
