using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using System.Transactions;
using CluedIn.Connector.DataLake.Common.Connector.SqlDataWriter;
using CluedIn.Core;
using CluedIn.Core.Data.Relational;
using CluedIn.Core.Jobs;
using CluedIn.Core.Streams;

using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.DataLake.Common.Connector;

internal abstract class DataLakeExportEntitiesJobBase : DataLakeJobBase
{
    private readonly IStreamRepository _streamRepository;
    private readonly IDataLakeClient _dataLakeClient;
    private readonly IDataLakeConstants _dataLakeConstants;
    private readonly IDataLakeJobDataFactory _dataLakeJobDataFactory;
    private static readonly TimeSpan exportTimeout = TimeSpan.MaxValue;
    private const int ExportEntitiesLockInMilliseconds = 100;

    protected DataLakeExportEntitiesJobBase(
        ApplicationContext appContext,
        IStreamRepository streamRepository,
        IDataLakeClient dataLakeClient,
        IDataLakeConstants dataLakeConstants,
        IDataLakeJobDataFactory dataLakeJobDataFactory) : base(appContext)
    {
        _streamRepository = streamRepository ?? throw new ArgumentNullException(nameof(streamRepository));
        _dataLakeClient = dataLakeClient ?? throw new ArgumentNullException(nameof(dataLakeClient));
        _dataLakeConstants = dataLakeConstants ?? throw new ArgumentNullException(nameof(dataLakeConstants));
        _dataLakeJobDataFactory = dataLakeJobDataFactory ?? throw new ArgumentNullException(nameof(dataLakeJobDataFactory));
    }

    public override async Task DoRunAsync(ExecutionContext context, DataLakeJobArgs args)
    {
        var typeName = this.GetType().Name;
        using var exportJobLoggingScope = context.Log.BeginScope(new Dictionary<string, object>
        {
            ["StreamId"] = args.Message,
            ["Schedule"] = args.Schedule,
            ["ExportJob"] = typeName,
        });
        context.Log.LogInformation("Begin export entities job '{ExportJob}' for '{StreamId}' using {Schedule}.", typeName, args.Message, args.Schedule);

        var organizationProviderDataStore = context.Organization.DataStores.GetDataStore<ProviderDefinition>();

        var streamId = new Guid(args.Message);
        var streamModel = await _streamRepository.GetStream(streamId);

        var providerDefinitionId = streamModel.ConnectorProviderDefinitionId!.Value;
        var provider = await organizationProviderDataStore.GetByIdAsync(context, providerDefinitionId);

        if (provider == null)
        {
            context.Log.LogWarning("Unable to get provider {ProviderDefinitionId}. Skipping export.", providerDefinitionId);
            return;
        }

        if (provider.ProviderId != _dataLakeConstants.ProviderId)
        {
            context.Log.LogDebug(
                "ProviderId {ProviderDefinitionId} is not the expected {DataLakeProviderId}. Skipping export.",
                provider.ProviderId,
                _dataLakeConstants.ProviderId);
            return;
        }

        if (!provider.IsEnabled)
        {
            context.Log.LogDebug("Provider {ProviderDefinitionId} is not enabled. Skipping export.", providerDefinitionId);
            return;
        }

        var containerName = streamModel.ContainerName;
        var executionContext = context.ApplicationContext.CreateExecutionContext(streamModel.OrganizationId);

        var configuration = await _dataLakeJobDataFactory.GetConfiguration(executionContext, providerDefinitionId, containerName);

        if (!configuration.IsStreamCacheEnabled)
        {
            context.Log.LogDebug("Stream cache is not enabled for stream {StreamId}. Skipping export.", streamModel.Id);
            return;
        }

        if (streamModel.Status != StreamStatus.Started)
        {
            context.Log.LogInformation("Stream not started for stream {StreamId}. Skipping export.", streamModel.Id);
            return;
        }

        var tableName = CacheTableHelper.GetCacheTableName(streamId);
        using var transactionScope = new TransactionScope(
            TransactionScopeOption.Required,
            exportTimeout,
            TransactionScopeAsyncFlowOption.Enabled);
        await using var connection = new SqlConnection(configuration.StreamCacheConnectionString);
        await connection.OpenAsync();
        if (!await DistributedLockHelper.TryAcquireTableCreationLock(connection, $"{typeName}_{streamModel.Id}", ExportEntitiesLockInMilliseconds))
        {
            context.Log.LogInformation("Unable to acquire lock to export data for Stream '{StreamId}'. Skipping export.", streamModel.Id);
            return;
        }

        var asOfTime = GetLastOccurence(context, args, configuration);
        var outputFormat = configuration.OutputFormat.ToLowerInvariant();
        var outputFileName = GetOutputFileName(streamId, asOfTime, outputFormat);
        var isFileExists = await _dataLakeClient.FileInPathExists(configuration, outputFileName);

        if (isFileExists)
        {
            if (args.IsTriggeredFromJobServer)
            {
                context.Log.LogInformation(
                    "Output file '{OutputFileName}' exists using data at {DataTime} and job is triggered from job server. Switching to using current time.",
                    outputFileName,
                    asOfTime);
                asOfTime = DateTime.UtcNow;
                outputFileName = GetOutputFileName(streamId, asOfTime, outputFormat);
            }
            else
            {
                context.Log.LogInformation(
                    "Output file '{OutputFileName}' exists using data at {DataTime} and job is triggered from {SchedulerType}. Skipping export.",
                    outputFileName,
                    asOfTime,
                    nameof(DataLakeConnectorComponentBase));
                return;
            }
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

        context.Log.LogInformation("Begin writing to file '{OutputFileName}' using data at {DataTime}.", outputFileName, asOfTime);
        var directoryClient = await _dataLakeClient.EnsureDataLakeDirectoryExist(configuration);
        var dataLakeFileClient = directoryClient.GetFileClient(outputFileName);
        await using var outputStream = await dataLakeFileClient.OpenWriteAsync(true);
        using var bufferedStream = new DataLakeBufferedWriteStream(outputStream);

        await sqlDataWriter?.WriteAsync(context, configuration, bufferedStream, fieldNames, reader);
        context.Log.LogInformation("End export entities job '{ExportJob}' for '{StreamId}' using {Schedule}.", typeName, args.Message, args.Schedule);
    }

    protected virtual string GetOutputFileName(Guid streamId, DateTime asOfTime, string outputFormat)
    {
        var fileExtension = GetFileExtension(outputFormat);
        var outputFileName = $"{streamId}_{asOfTime:yyyyMMddHHmmss}.{fileExtension}";
        return outputFileName;
    }

    protected virtual string GetFileExtension(string outputFormat)
    {
        return outputFormat;
    }

    private DateTime GetLastOccurence(ExecutionContext context, JobArgs args, IDataLakeJobData jobData)
    {
        if (jobData.UseCurrentTimeForExport
            || args.Schedule == DataLakeConstants.CronSchedules[DataLakeConstants.JobScheduleNames.Never])
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
        var format = outputFormat.Trim();
        if (format.Equals(DataLakeConstants.OutputFormats.Csv, StringComparison.OrdinalIgnoreCase))
        {
            return new CsvSqlDataWriter();
        }
        else if (format.Equals(DataLakeConstants.OutputFormats.Json, StringComparison.OrdinalIgnoreCase))
        {
            return new JsonSqlDataWriter();
        }
        else if (format.Equals(DataLakeConstants.OutputFormats.Parquet, StringComparison.OrdinalIgnoreCase))
        {
            return new ParquetSqlDataWriter();
        }

        throw new NotSupportedException($"Format '{outputFormat}' is not supported.");
    }
}
