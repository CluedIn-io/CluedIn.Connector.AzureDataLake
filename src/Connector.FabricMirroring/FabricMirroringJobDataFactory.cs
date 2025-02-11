using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using CluedIn.Connector.DataLake.Common;
using CluedIn.Core;
using CluedIn.Core.Data.Relational;

namespace CluedIn.Connector.FabricMirroring;

public class FabricMirroringJobDataFactory : DataLakeJobDataFactoryBase, IDataLakeJobDataFactory
{
    protected override Task<IDataLakeJobData> CreateJobData(
        ExecutionContext executionContext,
        IDictionary<string, object> authenticationDetails,
        string containerName)
    {
        return Task.FromResult<IDataLakeJobData>(new FabricMirroringConnectorJobData(authenticationDetails, containerName));
    }

    //public override async Task<IDataLakeJobData> GetConfiguration(ExecutionContext executionContext, Guid providerDefinitionId, string containerName)
    //{
    //    var baseResult = await base.GetConfiguration(executionContext, providerDefinitionId, containerName);
    //}

    public override async Task<IDataLakeJobData> GetConfiguration(ExecutionContext executionContext, IDictionary<string, object> authenticationDetails, string containerName = null)
    {
        if (!authenticationDetails.TryGetValue(FabricMirroringConstants.MirroredDatabaseName, out var value)
            || string.IsNullOrWhiteSpace(value?.ToString()))
        {
            if (!authenticationDetails.TryGetValue("__ProviderDefinitionId__", out var providerDefinitionId))
            {
                throw new ApplicationException("Failed to get provider definition id from authentication details.");
            }

            authenticationDetails[FabricMirroringConstants.MirroredDatabaseName] = $"CluedIn_ExportTarget_{providerDefinitionId:N}";
            authenticationDetails[FabricMirroringConstants.ShouldCreateMirroredDatabase] = true;
        }

        return await base.GetConfiguration(executionContext, authenticationDetails, containerName);
    }

    //private static void UpdateMirroredDatabaseName(IDictionary<string, object> authenticationDetails, Guid providerDefinitionId, bool shouldCreate)
    //{
    //    if (!authenticationDetails.TryGetValue(FabricMirroringConstants.MirroredDatabaseName, out var value)
    //        || string.IsNullOrWhiteSpace(value?.ToString()))
    //    {
    //        authenticationDetails[FabricMirroringConstants.MirroredDatabaseName] = $"CluedIn_ExportTarget_{providerDefinitionId:N}";
    //        authenticationDetails[FabricMirroringConstants.ShouldCreateMirroredDatabase] = true;
    //    }
    //}
}
