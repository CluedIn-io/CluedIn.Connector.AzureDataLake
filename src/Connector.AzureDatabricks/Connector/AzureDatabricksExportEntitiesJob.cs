using System;

using CluedIn.Connector.DataLake.Common.Connector;
using CluedIn.Core;
using CluedIn.Core.Streams;

namespace CluedIn.Connector.AzureDatabricks.Connector;

internal class AzureDatabricksExportEntitiesJob : DataLakeExportEntitiesJobBase
{
    public AzureDatabricksExportEntitiesJob(
        ApplicationContext appContext,
        IStreamRepository streamRepository,
        AzureDatabricksClient dataLakeClient,
        IAzureDatabricksConstants dataLakeConstants,
        AzureDatabricksJobDataFactory dataLakeJobDataFactory,
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
