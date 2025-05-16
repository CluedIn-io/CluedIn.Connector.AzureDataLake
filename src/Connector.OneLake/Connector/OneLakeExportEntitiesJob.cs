using System;
using System.Threading.Tasks;

using CluedIn.Connector.DataLake.Common.Connector;
using CluedIn.Core;
using CluedIn.Core.Streams;

using Microsoft.Extensions.Logging;

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

    private protected override async Task PostExportAsync(ExecutionContext context, ExportJobData exportJobData)
    {
        var jobData = exportJobData.DataLakeJobData as OneLakeConnectorJobData;
        if (!jobData.ShouldLoadToTable)
        {
            context.Log.LogDebug("Skipping loading to table as the job data does not require it.");
            return;
        }

        if (string.IsNullOrWhiteSpace(jobData.TableName))
        {
            context.Log.LogWarning("Skipping loading to table as the table name is not specified.");
            return;
        }

        var replacedTableName = await ReplaceNameUsingPatternAsync(
            context,
            jobData.TableName,
            exportJobData.StreamId,
            exportJobData.StreamModel.ContainerName,
            exportJobData.AsOfTime,
            exportJobData.OutputFormat);
        await DataLakeClient.LoadToTableAsync(exportJobData.OutputFileName, replacedTableName, exportJobData.DataLakeJobData);
    }
}
