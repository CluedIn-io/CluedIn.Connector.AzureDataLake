using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using CluedIn.Connector.AzureDataLake.Connector;
using CluedIn.Connector.DataLake.Common;
using CluedIn.Connector.DataLake.Common.Connector;
using CluedIn.Core;

using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.AzureDataLake;

public class AzureDataLakeJobDataFactory : DataLakeJobDataFactoryBase, IDataLakeJobDataFactory
{
    public override Task<IDataLakeClient> CreateDataLakeClient(ExecutionContext executionContext, IDataLakeJobData jobData)
    {
        if (jobData is not AzureDataLakeConnectorJobData castedJobData)
        {
            throw new ApplicationException($"Provided job data is not of expected type '{typeof(AzureDataLakeConnectorJobData)}'. It is '{jobData.GetType()}'.");
        }

        var logger = executionContext.ApplicationContext.Container.Resolve<ILogger<AzureDataLakeClient>>();
        return Task.FromResult<IDataLakeClient>(new AzureDataLakeClient(logger, castedJobData));
    }

    protected override Task<IDataLakeJobData> CreateJobData(
        ExecutionContext executionContext,
        IDictionary<string, object> authenticationDetails,
        string containerName)
    {
        return Task.FromResult<IDataLakeJobData>(new AzureDataLakeConnectorJobData(authenticationDetails, containerName));
    }
}
