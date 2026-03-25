using System.Collections.Generic;
using System.Threading.Tasks;

using CluedIn.Connector.DataLake.Common;
using CluedIn.Connector.DataLake.Common.Connector;
using CluedIn.Connector.OneLake.Connector;
using CluedIn.Core;

using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.OneLake;

public class OneLakeJobDataFactory : DataLakeJobDataFactoryBase, IDataLakeJobDataFactory
{
    public override async Task<IDataLakeClient> CreateDataLakeClient(ExecutionContext executionContext, IDataLakeJobData jobData)
    {
        return await CreateDataLakeClient(executionContext, jobData as OneLakeConnectorJobData);
    }

    internal virtual Task<OneLakeClient> CreateDataLakeClient(ExecutionContext executionContext, OneLakeConnectorJobData jobData)
    {
        var logger = executionContext.ApplicationContext.Container.Resolve<ILogger<OneLakeClient>>();
        var client = new OneLakeClient(logger, jobData);
        return Task.FromResult(client);
    }

    protected override Task<IDataLakeJobData> CreateJobData(
        ExecutionContext executionContext,
        IDictionary<string, object> authenticationDetails,
        string containerName)
    {
        return Task.FromResult<IDataLakeJobData>(new OneLakeConnectorJobData(authenticationDetails, containerName));
    }
}
