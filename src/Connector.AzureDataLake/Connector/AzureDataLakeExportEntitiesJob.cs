using CluedIn.Connector.DataLake.Common.Connector;
using CluedIn.Core;
using CluedIn.Core.Streams;

namespace CluedIn.Connector.AzureDataLake.Connector;

internal class AzureDataLakeExportEntitiesJob : DataLakeExportEntitiesJobBase
{
    public AzureDataLakeExportEntitiesJob(
        ApplicationContext appContext,
        IStreamRepository streamRepository,
        AzureDataLakeClient dataLakeClient,
        IAzureDataLakeConstants dataLakeConstants,
        AzureDataLakeJobDataFactory dataLakeJobDataFactory,
        IDateTimeOffsetProvider dateTimeOffsetProvider)
        : base(appContext, streamRepository, dataLakeClient, dataLakeConstants, dataLakeJobDataFactory, dateTimeOffsetProvider)
    {
    }
}
