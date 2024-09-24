using System;

using CluedIn.Connector.DataLake.Common.Connector;
using CluedIn.Core;
using CluedIn.Core.Streams;

namespace CluedIn.Connector.OneLake.Connector;

internal class OneLakeExportEntitiesJob : DataLakeExportEntitiesJobBase
{
    public OneLakeExportEntitiesJob(
        ApplicationContext appContext,
        IStreamRepository streamRepository,
        OneLakeClient dataLakeClient,
        IOneLakeConstants dataLakeConstants,
        OneLakeJobDataFactory dataLakeJobDataFactory)
        : base(appContext, streamRepository, dataLakeClient, dataLakeConstants, dataLakeJobDataFactory)
    {
    }

    protected override string GetOutputFileName(string containerName, DateTime asOfTime, string outputFormat)
    {
        var fileExtension = GetFileExtension(outputFormat);
        return $"{containerName}_{asOfTime:yyyyMMddHHmmss}.{fileExtension}";
    }
}
