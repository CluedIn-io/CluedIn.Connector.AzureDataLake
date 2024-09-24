using System;

using CluedIn.Connector.DataLake.Common.Connector;
using CluedIn.Core;
using CluedIn.Core.Streams;

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

    protected override string GetOutputFileName(string containerName, DateTime asOfTime, string outputFormat)
    {
        var fileExtension = GetFileExtension(outputFormat);
        return $"{containerName}_{asOfTime:yyyyMMddHHmmss}.{fileExtension}";
    }
}
