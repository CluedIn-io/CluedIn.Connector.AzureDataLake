using System;
using CluedIn.Connector.DataLake.Common;
using CluedIn.Connector.DataLake.Common.Connector;
using CluedIn.Core;
using CluedIn.Core.Streams;
using CluedIn.Core.Streams.Models;

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

    protected override string GetOutputFileName(StreamModel streamModel, DateTime asOfTime, string outputFormat, IDataLakeJobData configuration)
    {
        var fileExtension = GetFileExtension(outputFormat);
        return $"{streamModel.ContainerName}_{asOfTime:yyyyMMddHHmmss}.{fileExtension}";
    }
}
