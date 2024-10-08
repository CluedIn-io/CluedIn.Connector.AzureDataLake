using System;
using CluedIn.Connector.DataLake.Common;
using CluedIn.Connector.DataLake.Common.Connector;
using CluedIn.Core;
using CluedIn.Core.Streams;
using CluedIn.Core.Streams.Models;

namespace CluedIn.Connector.AzureAIStudio.Connector;

internal class AzureAIStudioExportEntitiesJob : DataLakeExportEntitiesJobBase
{
    public AzureAIStudioExportEntitiesJob(
        ApplicationContext appContext,
        IStreamRepository streamRepository,
        AzureAIStudioClient dataLakeClient,
        IAzureAIStudioConstants dataLakeConstants,
        AzureAIStudioJobDataFactory dataLakeJobDataFactory)
        : base(appContext, streamRepository, dataLakeClient, dataLakeConstants, dataLakeJobDataFactory)
    {
    }

    protected override string GetOutputFileName(StreamModel streamModel, DateTime asOfTime, string outputFormat, IDataLakeJobData configuration)
    {
        var fileExtension = GetFileExtension(outputFormat);
        return $"{streamModel.ContainerName}_{asOfTime:yyyyMMddHHmmss}.{fileExtension}";
    }
}
