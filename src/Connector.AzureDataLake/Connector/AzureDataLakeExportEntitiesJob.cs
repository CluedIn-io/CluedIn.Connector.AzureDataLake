using System.Threading.Tasks;
using CluedIn.Connector.DataLake.Common.Connector;
using CluedIn.Core;
using CluedIn.Core.Jobs;
using CluedIn.Core.Streams;

namespace CluedIn.Connector.AzureDataLake.Connector;

internal class AzureDataLakeExportEntitiesJob : DataLakeExportEntitiesJobBase
{
    public AzureDataLakeExportEntitiesJob(
        ApplicationContext appContext,
        IStreamRepository streamRepository,
        AzureDataLakeClient dataLakeClient,
        IAzureDataLakeConstants dataLakeConstants,
        AzureDataLakeJobDataFactory dataLakeJobDataFactory)
        : base(appContext, streamRepository, dataLakeClient, dataLakeConstants, dataLakeJobDataFactory)
    {
    }

    public override async Task DoRunAsync(ExecutionContext context, JobArgs args)
    {
        //save main Table
        await base.DoRunAsync(context, args);


    }
}
