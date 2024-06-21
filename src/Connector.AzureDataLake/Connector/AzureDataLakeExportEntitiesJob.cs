using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using System.Transactions;

using CluedIn.Connector.AzureDataLake.Connector.SqlDataWriter;
using CluedIn.Core;
using CluedIn.Core.Data.Relational;
using CluedIn.Core.Jobs;
using CluedIn.Core.Streams;

using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.AzureDataLake.Connector;

internal class AzureDataLakeExportEntitiesJob : AzureDataLakeJobBase
{
    private static readonly TimeSpan exportTimeout = TimeSpan.MaxValue;
    private const int ExportEntitiesLockInMilliseconds = 100;
    public AzureDataLakeExportEntitiesJob(ApplicationContext appContext) : base(appContext)
    {
    }

    public override async Task DoRunAsync(ExecutionContext context, JobArgs args)
    {
        using var exportJobLoggingScope = context.Log.BeginScope(new Dictionary<string, object>
        {
            ["StreamId"] = args.Message,
            ["Schedule"] = args.Schedule,
            ["ExportJob"] = nameof(AzureDataLakeExportEntitiesJob),
        });
        context.Log.LogDebug("Begin export entities job '{ExportJob}' for '{StreamId}' using {Schedule}.", nameof(AzureDataLakeExportEntitiesJob), args.Message, args.Schedule);
                
        var streamRepository = context.ApplicationContext.Container.Resolve<IStreamRepository>();
        var client = context.ApplicationContext.Container.Resolve<IAzureDataLakeClient>();
        var organizationProviderDataStore = context.Organization.DataStores.GetDataStore<ProviderDefinition>();

        var streamId = new Guid(args.Message);
        var streamModel = await streamRepository.GetStream(streamId);

        var providerDefinitionId = streamModel.ConnectorProviderDefinitionId!.Value;
        var provider = await organizationProviderDataStore.GetByIdAsync(context, providerDefinitionId);

        if (provider == null)
        {
            context.Log.LogDebug("Unable to get provider {ProviderDefinitionId}. Skipping export.", providerDefinitionId);
            return;
        }

        if (provider.ProviderId != AzureDataLakeConstants.DataLakeProviderId)
        {
            context.Log.LogDebug(
                "ProviderId {ProviderDefinitionId} is not the expected {DataLakeProviderId}. Skipping export.",
                provider.ProviderId,
                AzureDataLakeConstants.DataLakeProviderId);
            return;
        }

        if (!provider.IsEnabled)
        {
            context.Log.LogDebug("Provider {ProviderDefinitionId} is not enabled. Skipping export.", providerDefinitionId);
            return;
        }

        var containerName = streamModel.ContainerName;
        var executionContext = context.ApplicationContext.CreateExecutionContext(streamModel.OrganizationId);

        var configuration = await AzureDataLakeConnectorJobData.Create(executionContext, providerDefinitionId, containerName);

        if (!configuration.IsStreamCacheEnabled)
        {
            context.Log.LogDebug("Stream cache is not enabled for stream {StreamId}. Skipping export.", streamModel.Id);
            return;
        }

        if (streamModel.Status != StreamStatus.Started)
        {
            context.Log.LogDebug("Stream not started for stream {StreamId}. Skipping export.", streamModel.Id);
            return;
        }


        var tableName = CacheTableHelper.GetCacheTableName(streamId);
        using var transactionScope = new TransactionScope(
            TransactionScopeOption.Required,
            exportTimeout,
            TransactionScopeAsyncFlowOption.Enabled);
        await using var connection = new SqlConnection(configuration.StreamCacheConnectionString);
        await connection.OpenAsync();
        if (!await DistributedLockHelper.TryAcquireTableCreationLock(connection, $"{nameof(AzureDataLakeExportEntitiesJob)}_{streamModel.Id}", ExportEntitiesLockInMilliseconds))
        {
            context.Log.LogInformation("Unable to acquire lock to export data for Stream '{StreamId}'. Skipping export.", streamModel.Id);
            return;
        }

        var asOfTime = GetLastOccurence(context, args, configuration);
        var outputFormat = configuration.OutputFormat.ToLowerInvariant();
        var outputFileName = GetOutputFileName(streamId, asOfTime, outputFormat);

        if (await client.FileInPathExists(configuration, outputFileName))
        {
            context.Log.LogDebug("Output file '{OutputFileName}' exists using data at {DataTime}. Switching to using current time", outputFileName, asOfTime);
            asOfTime = DateTime.UtcNow;
            outputFileName = GetOutputFileName(streamId, asOfTime, outputFormat);
        }

        var getDataSql = $"SELECT * FROM [{tableName}] FOR SYSTEM_TIME AS OF '{asOfTime:o}'";
        var command = new SqlCommand(getDataSql, connection)
        {
            CommandType = CommandType.Text
        };
        await using var reader = await command.ExecuteReaderAsync();

        var fieldNames = Enumerable.Range(0, reader.VisibleFieldCount)
            .Select(reader.GetName)
            .ToList();

        using var loggingScope = context.Log.BeginScope(new Dictionary<string, object>
        {
            ["FileName"] = outputFileName,
            ["Format"] = outputFormat,
            ["StartTime"] = DateTimeOffset.UtcNow,
            ["DataTime"] = asOfTime,
        });

        var sqlDataWriter = GetSqlDataWriter(outputFormat);

        context.Log.LogDebug("Begin writing to file '{OutputFileName}' using data at {DataTime}.", outputFileName, asOfTime);
        var directoryClient = await client.EnsureDataLakeDirectoryExist(configuration);
        var dataLakeFileClient = directoryClient.GetFileClient(outputFileName);
        await using var outputStream = await dataLakeFileClient.OpenWriteAsync(true);

        await sqlDataWriter?.WriteAsync(context, outputStream, fieldNames, reader);
        context.Log.LogDebug("End export entities job '{ExportJob}' for '{StreamId}' using {Schedule}.", nameof(AzureDataLakeExportEntitiesJob), args.Message, args.Schedule);
    }

    private static string GetOutputFileName(Guid streamId, DateTime asOfTime, string outputFormat)
    {
        var fileExtension = GetFileExtension(outputFormat);
        var outputFileName = $"{streamId}_{asOfTime:yyyyMMddHHmmss}.{fileExtension}";
        return outputFileName;
    }

    private static string GetFileExtension(string outputFormat)
    {
        return outputFormat;
    }

    private static DateTime GetLastOccurence(ExecutionContext context, JobArgs args, AzureDataLakeConnectorJobData configuration)
    {
        if (configuration.UseCurrentTimeForExport
            || args.Schedule == AzureDataLakeConstants.CronSchedules[AzureDataLakeConstants.JobScheduleNames.Never])
        {
            context.Log.LogDebug("Using current time for export.");
            return DateTime.UtcNow;
        }

        var cronSchedule = NCrontab.CrontabSchedule.Parse(args.Schedule);
        var next = cronSchedule.GetNextOccurrence(DateTime.UtcNow.AddMinutes(1));
        var nextNext = cronSchedule.GetNextOccurrence(next.AddMinutes(1));
        var diff = nextNext - next;
        var asOfTime = next - diff;
        return asOfTime;
    }

    private static ISqlDataWriter GetSqlDataWriter(string outputFormat)
    {
        if (outputFormat.Equals(AzureDataLakeConstants.OutputFormats.Csv, StringComparison.OrdinalIgnoreCase))
        {
            return new CsvSqlDataWriter();
        }
        else if (outputFormat.Equals(AzureDataLakeConstants.OutputFormats.Json, StringComparison.OrdinalIgnoreCase))
        {
            return new JsonSqlDataWriter();
        }
        else if (outputFormat.Equals(AzureDataLakeConstants.OutputFormats.Parquet, StringComparison.OrdinalIgnoreCase))
        {
            return new ParquetSqlDataWriter();
        }

        throw new NotSupportedException($"Format '{outputFormat}' is not supported.");
    }
}
