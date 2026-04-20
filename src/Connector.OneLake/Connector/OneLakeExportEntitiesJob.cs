using System;
using System.Threading.Tasks;

using CluedIn.Connector.FileStorage.Common.Connector;
using CluedIn.Core;
using CluedIn.Core.Streams;

using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.OneLake.Connector;

internal class OneLakeExportEntitiesJob : StorageExportEntitiesJobBase
{
    private readonly OneLakeStorageFactory _storageFactory;

    public OneLakeExportEntitiesJob(
        ApplicationContext appContext,
        IStreamRepository streamRepository,
        IOneLakeConfigurationConstants configurationConstants,
        OneLakeStorageFactory storageFactory,
        IDateTimeOffsetProvider dateTimeOffsetProvider)
        : base(appContext, streamRepository, configurationConstants, storageFactory, dateTimeOffsetProvider)
    {
        _storageFactory = storageFactory ?? throw new ArgumentNullException(nameof(storageFactory));
    }

    private protected override async Task PostExportAsync(ExecutionContext context, ExportJobData exportJobData)
    {
        var client = await _storageFactory.CreateStorageClient(context, exportJobData.StorageConfiguration) as OneLakeStorageClient;
        var configuration = exportJobData.StorageConfiguration as OneLakeConnectorConfiguration;
        if (!configuration.ShouldLoadToTable)
        {
            context.Log.LogDebug("Skipping loading to table as the job data does not require it.");
            return;
        }

        if (string.IsNullOrWhiteSpace(configuration.TableName))
        {
            context.Log.LogWarning("Skipping loading to table as the table name is not specified.");
            return;
        }

        var replacedTableName = await PatternHelper.ReplaceNameUsingPatternAsync(
            context,
            configuration.TableName,
            exportJobData.StreamId,
            exportJobData.StreamModel.ContainerName,
            exportJobData.AsOfTime,
            exportJobData.OutputFormat);
        await client.LoadToTableAsync(exportJobData.OutputFileName, replacedTableName);
    }
}
