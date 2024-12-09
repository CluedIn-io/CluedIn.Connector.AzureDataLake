using System;

using CluedIn.Connector.DataLake.Common.Connector;
using CluedIn.Core;
using CluedIn.Core.Streams;

namespace CluedIn.Connector.SynapseDataEngineering.Connector;

internal class SynapseDataEngineeringExportEntitiesJob : DataLakeExportEntitiesJobBase
{
    public SynapseDataEngineeringExportEntitiesJob(
        ApplicationContext appContext,
        IStreamRepository streamRepository,
        SynapseDataEngineeringClient dataLakeClient,
        ISynapseDataEngineeringConstants dataLakeConstants,
        SynapseDataEngineeringJobDataFactory dataLakeJobDataFactory,
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
