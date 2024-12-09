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
        OneLakeJobDataFactory dataLakeJobDataFactory,
        IDateTimeOffsetProvider dateTimeOffsetProvider)
        : base(appContext, streamRepository, dataLakeClient, dataLakeConstants, dataLakeJobDataFactory, dateTimeOffsetProvider)
    {
    }

    protected override string GetDefaultOutputFileName(Guid streamId, string containerName, DateTimeOffset asOfTime, string outputFormat)
    {
        var fileExtension = GetFileExtension(outputFormat);
        return $"{streamId:N}_{asOfTime:yyyyMMddHHmmss}.{fileExtension}";
    }
}
