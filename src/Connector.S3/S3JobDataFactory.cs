using CluedIn.Connector.DataLake.Common;
using System.Threading.Tasks;
using System;
using System.Collections.Generic;
using CluedIn.Core;

namespace CluedIn.Connector.S3
{
    public class S3JobDataFactory : IDataLakeJobDataFactory
    {
        public Task<IDataLakeJobData> GetConfiguration(CluedIn.Core.ExecutionContext executionContext, Guid providerDefinitionId, string containerName)
        {
            // TODO: Implement S3-specific configuration logic
            throw new NotImplementedException();
        }

        public Task<IDataLakeJobData> GetConfiguration(CluedIn.Core.ExecutionContext executionContext, IDictionary<string, object> authenticationDetails, string containerName = null)
        {
            // TODO: Implement S3-specific configuration logic
            throw new NotImplementedException();
        }
    }
}
