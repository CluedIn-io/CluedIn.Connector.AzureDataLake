using System;
using System.Threading.Tasks;

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
        DataLakeClient = dataLakeClient;
    }

    private OneLakeClient DataLakeClient { get; }

    private protected override async Task PostExportAsync(ExportJobData exportJobData)
    {
        await base.PostExportAsync(exportJobData);
        await DataLakeClient.LoadToTableAsync(exportJobData.OutputFileName, exportJobData.DataLakeJobData);
    }
}
