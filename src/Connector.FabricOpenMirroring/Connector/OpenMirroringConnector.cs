using System;
using System.Threading.Tasks;

using CluedIn.Connector.DataLake.Common;
using CluedIn.Connector.DataLake.Common.Connector;
using CluedIn.Core;
using CluedIn.Core.Streams.Models;

using Microsoft.Extensions.Logging;


namespace CluedIn.Connector.FabricOpenMirroring.Connector;

public class OpenMirroringConnector : DataLakeConnector
{
    public OpenMirroringConnector(
        ILogger<OpenMirroringConnector> logger,
        OpenMirroringClient client,
        IOpenMirroringConstants constants,
        OpenMirroringJobDataFactory dataLakeJobDataFactory,
        IDateTimeOffsetProvider dateTimeOffsetProvider)
        : base(logger, client, constants, dataLakeJobDataFactory, dateTimeOffsetProvider)
    {
    }

    protected override async Task<bool> VerifyDataLakeConnection(IDataLakeJobData jobData)
    {
        //TODO: verify path exists only if mirroring created
        // find out a way to see if it's called by test connection or healthcheck (might have to do ugly reflection to look at stack)
        // probably could do it by verifying if jobdata from db = jobdata created from passed config
        // but we need to be able to get provider definition id, which is not passed
        if (jobData is OpenMirroringConnectorJobData castedJobData
            && (!castedJobData.Configurations.TryGetValue(DataLakeConstants.ProviderDefinitionIdKey, out var providerDefinition)))
        {
            return true;
        }

        try
        {
            return await Client.DirectoryExists(jobData);
        }
        catch (Exception ex)
        {
            return false;
        }
    }

    public override async Task ArchiveContainer(ExecutionContext executionContext, IReadOnlyStreamModel streamModel)
    {
        var providerDefinitionId = streamModel.ConnectorProviderDefinitionId!.Value;
        var containerName = streamModel.ContainerName;

        var jobData = await DataLakeJobDataFactory.GetConfiguration(executionContext, providerDefinitionId, containerName) as OpenMirroringConnectorJobData;
        await Client.DeleteDirectory(jobData, streamModel.Id.ToString("N"));
        await base.ArchiveContainer(executionContext, streamModel);
    }
}
