using System.Collections.Generic;
using System.Threading.Tasks;

using CluedIn.Connector.DataLake.Common;
using CluedIn.Core;

namespace CluedIn.Connector.AzureAIStudio;

public class AzureAIStudioJobDataFactory : DataLakeJobDataFactoryBase, IDataLakeJobDataFactory
{
    protected override Task<IDataLakeJobData> CreateJobData(
        ExecutionContext executionContext,
        IDictionary<string, object> authenticationDetails,
        string containerName)
    {
        return Task.FromResult<IDataLakeJobData>(new AzureAIStudioConnectorJobData(authenticationDetails, containerName));
    }
}
