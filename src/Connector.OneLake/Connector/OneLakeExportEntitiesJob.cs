using System;
using System.Threading.Tasks;

using CluedIn.Connector.DataLake.Common;
using CluedIn.Connector.DataLake.Common.Connector;
using CluedIn.Core;
using CluedIn.Core.Streams;

using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.OneLake.Connector;

internal class OneLakeExportEntitiesJob : DataLakeExportEntitiesJobBase
{
    private readonly OneLakeJobDataFactory _dataLakeJobDataFactory;

    public OneLakeExportEntitiesJob(
        ApplicationContext appContext,
        IStreamRepository streamRepository,
        IOneLakeConstants dataLakeConstants,
        OneLakeJobDataFactory dataLakeJobDataFactory,
        IDateTimeOffsetProvider dateTimeOffsetProvider)
        : base(appContext, streamRepository, dataLakeConstants, dataLakeJobDataFactory, dateTimeOffsetProvider)
    {
        _dataLakeJobDataFactory = dataLakeJobDataFactory ?? throw new ArgumentNullException(nameof(dataLakeJobDataFactory));
    }

    private protected override async Task PostExportAsync(ExecutionContext context, ExportJobData exportJobData)
    {
        var client = await _dataLakeJobDataFactory.CreateDataLakeClient(context, exportJobData.DataLakeJobData as OneLakeConnectorJobData);
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

        var replacedTableName = await PatternHelper.ReplaceNameUsingPatternAsync(
            context,
            jobData.TableName,
            exportJobData.StreamId,
            exportJobData.StreamModel.ContainerName,
            exportJobData.AsOfTime,
            exportJobData.OutputFormat);
        await client.LoadToTableAsync(exportJobData.OutputFileName, replacedTableName);
    }
}
