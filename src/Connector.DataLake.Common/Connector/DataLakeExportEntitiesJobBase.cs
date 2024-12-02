﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Net;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Transactions;

using Azure.Storage.Files.DataLake.Models;

using CluedIn.Connector.DataLake.Common.Connector.SqlDataWriter;
using CluedIn.Core;
using CluedIn.Core.Data.Relational;
using CluedIn.Core.Streams;
using CluedIn.Core.Streams.Models;

using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.DataLake.Common.Connector;

internal abstract class DataLakeExportEntitiesJobBase : DataLakeJobBase
{
    private readonly IStreamRepository _streamRepository;
    private readonly IDataLakeClient _dataLakeClient;
    private readonly IDataLakeConstants _dataLakeConstants;
    private readonly IDataLakeJobDataFactory _dataLakeJobDataFactory;
    private readonly IDateTimeOffsetProvider _dateTimeOffsetProvider;
    private static readonly TimeSpan exportTimeout = TimeSpan.MaxValue;
    private const int ExportEntitiesLockInMilliseconds = 100;
    private const string StreamIdKey = "StreamId";
    private const string DataTimeKey = "DataTime";
    private const string InstanceTimeKey = "InstanceTime";
    private const string TemporaryFileSuffix = ".tmp";

    protected DataLakeExportEntitiesJobBase(
        ApplicationContext appContext,
        IStreamRepository streamRepository,
        IDataLakeClient dataLakeClient,
        IDataLakeConstants dataLakeConstants,
        IDataLakeJobDataFactory dataLakeJobDataFactory,
        IDateTimeOffsetProvider dateTimeOffsetProvider) : base(appContext, dateTimeOffsetProvider)
    {
        _streamRepository = streamRepository ?? throw new ArgumentNullException(nameof(streamRepository));
        _dataLakeClient = dataLakeClient ?? throw new ArgumentNullException(nameof(dataLakeClient));
        _dataLakeConstants = dataLakeConstants ?? throw new ArgumentNullException(nameof(dataLakeConstants));
        _dataLakeJobDataFactory = dataLakeJobDataFactory ?? throw new ArgumentNullException(nameof(dataLakeJobDataFactory));
        _dateTimeOffsetProvider = dateTimeOffsetProvider ?? throw new ArgumentNullException(nameof(dateTimeOffsetProvider));
    }

    public override async Task DoRunAsync(ExecutionContext context, IDataLakeJobArgs args)
    {
        var typeName = this.GetType().Name;
        using var exportJobLoggingScope = context.Log.BeginScope(CreateLoggingScope(args));
        context.Log.LogInformation(
            "Begin export entities job '{ExportJob}' for '{StreamId}' using {Schedule} at {InstanceTime}.",
            typeName,
            args.Message,
            args.Schedule,
            args.InstanceTime);

        var exportJobData = await GetJobDataAsync(context, args, "export");
        if (exportJobData == null)
        {
            return;
        }

        var (streamId, streamModel, provider, configuration, asOfTime, outputFormat, outputFileName, filePathProperties) = exportJobData;

        var tableName = CacheTableHelper.GetCacheTableName(streamId);
        using var transactionScope = new TransactionScope(
            TransactionScopeOption.Required,
            exportTimeout,
            TransactionScopeAsyncFlowOption.Enabled);
        await using var connection = new SqlConnection(configuration.StreamCacheConnectionString);
        await connection.OpenAsync();
        if (!await DistributedLockHelper.TryAcquireExclusiveLock(connection, $"{typeName}_{streamModel.Id}", ExportEntitiesLockInMilliseconds))
        {
            context.Log.LogInformation("Unable to acquire lock to export data for Stream '{StreamId}'. Skipping export.", streamModel.Id);
            return;
        }

        if (filePathProperties != null)
        {
            if (args.IsTriggeredFromJobServer)
            {
                context.Log.LogInformation(
                    "Output file '{OutputFileName}' exists using data at {DataTime} and job is triggered from job server. Switching to using current time.",
                    outputFileName,
                    asOfTime);
                asOfTime = _dateTimeOffsetProvider.GetCurrentUtcTime();
                outputFileName = GetOutputFileName(configuration, streamId, streamModel.ContainerName, asOfTime, outputFormat);
            }
            else if (HasExportedFileBefore(streamId, asOfTime, filePathProperties?.Metadata))
            {
                context.Log.LogInformation(
                    "Output file '{OutputFileName}' exists using data at {DataTime} and job is triggered from {SchedulerType}. Skipping export.",
                    outputFileName,
                    asOfTime,
                    nameof(DataLakeConnectorComponentBase));
                return;
            }
            else
            {
                context.Log.LogInformation(
                    "Output file '{OutputFileName}' exists and will be overwritten.", outputFileName);
            }
        }

        var startExportTime = _dateTimeOffsetProvider.GetCurrentUtcTime();
        var exportHistory = new ExportHistory(
            streamId,
            asOfTime,
            args.IsTriggeredFromJobServer ? "JobServer" : "InternalScheduler",
            args.Schedule,
            outputFileName,
            startExportTime,
            _dateTimeOffsetProvider.GetCurrentUtcTime(),
            0,
            "Starting",
            Dns.GetHostName());

        await InsertHistory(context, connection, exportHistory);

        var getDataSql = $"SELECT * FROM [{tableName}] FOR SYSTEM_TIME AS OF '{asOfTime:o}'";
        var command = new SqlCommand(getDataSql, connection)
        {
            CommandType = CommandType.Text
        };
        await using var reader = await command.ExecuteReaderAsync();

        var fieldNames = Enumerable.Range(0, reader.VisibleFieldCount)
            .Select(reader.GetName)
            .ToList();

        var temporaryOutputFileName = outputFileName + TemporaryFileSuffix;
        using var loggingScope = context.Log.BeginScope(new Dictionary<string, object>
        {
            ["FileName"] = outputFileName,
            ["TemporaryFileName"] = temporaryOutputFileName,
            ["Format"] = outputFormat,
            ["StartTime"] = _dateTimeOffsetProvider.GetCurrentUtcTime(),
            [InstanceTimeKey] = args.InstanceTime,
            [DataTimeKey] = asOfTime,
        });

        context.Log.LogInformation(
            "Begin writing to file '{OutputFileName}' using data at {DataTime} and {TemporaryOutputFileName}.",
            outputFileName,
            asOfTime,
            temporaryOutputFileName);
        var directoryClient = await _dataLakeClient.EnsureDataLakeDirectoryExist(configuration);
        var temporaryFileClient = directoryClient.GetFileClient(temporaryOutputFileName);
        var totalRows = await writeFileContentsAsync();
        await setFilePropertiesAsync();
        await deleteTargetFileIfExistsAsync();
        await renameToTargetFileAsync();
        context.Log.LogInformation(
            "End writing to file '{OutputFileName}' using data at {DataTime} and {TemporaryOutputFileName}.",
            outputFileName,
            asOfTime,
            temporaryOutputFileName);

        await reader.CloseAsync();

        var updatedHistory = exportHistory with
        {
            TotalRows = totalRows,
            EndTime = _dateTimeOffsetProvider.GetCurrentUtcTime(),
            Status = "Complete"
        };
        await UpdateHistory(context, connection, updatedHistory);
        transactionScope.Complete();
        context.Log.LogInformation(
            "End export entities job '{ExportJob}' for '{StreamId}' using {Schedule}.",
            typeName,
            args.Message,
            args.Schedule);


        async Task<long> writeFileContentsAsync()
        {
            var sqlDataWriter = GetSqlDataWriter(outputFormat);
            await using var outputStream = await temporaryFileClient.OpenWriteAsync(true);
            using var bufferedStream = new DataLakeBufferedWriteStream(outputStream);
            return await sqlDataWriter?.WriteAsync(context, configuration, bufferedStream, fieldNames, reader);
        }

        async Task setFilePropertiesAsync()
        {
            context.Log.LogDebug(
                "Begin setting file properties to file '{OutputFileName}' StreamId {StreamId} and DataTime {DataTime}.",
                outputFileName,
                streamId,
                asOfTime);
            await temporaryFileClient.SetMetadataAsync(
                new Dictionary<string, string>
                {
                    [StreamIdKey] = streamId.ToString(),
                    [DataTimeKey] = asOfTime.ToString("O"),
                });
            context.Log.LogDebug(
                "End setting file properties to file '{OutputFileName}' StreamId {StreamId} and DataTime {DataTime}.",
                outputFileName,
                streamId,
                asOfTime);
        }

        async Task renameToTargetFileAsync()
        {
            context.Log.LogDebug(
                "Begin rename temporary file {TemporaryOutputFileName} to '{OutputFileName}' for StreamId {StreamId} and DataTime {DataTime}.",
                temporaryOutputFileName,
                outputFileName,
                streamId,
                asOfTime);
            await temporaryFileClient.RenameAsync(temporaryFileClient.Path[..^TemporaryFileSuffix.Length]);
            context.Log.LogDebug(
                "End rename temporary file {TemporaryOutputFileName} to '{OutputFileName}' for StreamId {StreamId} and DataTime {DataTime}.",
                temporaryOutputFileName,
                outputFileName,
                streamId,
                asOfTime);
        }

        async Task deleteTargetFileIfExistsAsync()
        {
            var targetFileClient = directoryClient.GetFileClient(outputFileName);
            await targetFileClient.DeleteIfExistsAsync();
        }
    }

    private async Task<ExportJobData> GetJobDataAsync(ExecutionContext context, IDataLakeJobArgs args, string taskName)
    {
        using var exportJobLoggingScope = context.Log.BeginScope(CreateLoggingScope(args));

        var organizationProviderDataStore = context.Organization.DataStores.GetDataStore<ProviderDefinition>();

        var streamId = new Guid(args.Message);
        var streamModel = await _streamRepository.GetStream(streamId);

        if (streamModel == null)
        {
            context.Log.LogWarning($"Unable to get stream with Id {{StreamId}}. Skipping {taskName}.", streamId);
            return null;
        }

        var providerDefinitionId = streamModel.ConnectorProviderDefinitionId!.Value;
        var provider = await organizationProviderDataStore.GetByIdAsync(context, providerDefinitionId);

        if (provider == null)
        {
            context.Log.LogWarning($"Unable to get provider {{ProviderDefinitionId}}. Skipping {taskName}.", providerDefinitionId);
            return null;
        }

        if (provider.ProviderId != _dataLakeConstants.ProviderId)
        {
            context.Log.LogDebug(
                $"Unable to get provider {{ProviderDefinitionId}}. Skipping {{DataLakeProviderId}}. Skipping {taskName}.",
                provider.ProviderId,
                _dataLakeConstants.ProviderId);
            return null;
        }

        if (!provider.IsEnabled)
        {
            context.Log.LogDebug($"Provider {{ProviderDefinitionId}} is not enabled. Skipping {taskName}.", providerDefinitionId);
            return null;
        }

        var containerName = streamModel.ContainerName;
        var executionContext = context.ApplicationContext.CreateExecutionContext(streamModel.OrganizationId);

        var configuration = await _dataLakeJobDataFactory.GetConfiguration(executionContext, providerDefinitionId, containerName);

        if (!configuration.IsStreamCacheEnabled)
        {
            context.Log.LogDebug($"Stream cache is not enabled for stream {{StreamId}}. Skipping {taskName}.", streamModel.Id);
            return null;
        }

        if (streamModel.Status != StreamStatus.Started)
        {
            context.Log.LogInformation($"Stream not started for stream {{StreamId}}. Skipping {taskName}.", streamModel.Id);
            return null;
        }
        var asOfTime = GetAsOfTime(context, args, configuration);
        var outputFormat = configuration.OutputFormat.ToLowerInvariant();
        var outputFileName = GetOutputFileName(configuration, streamId, containerName, asOfTime, outputFormat);
        var filePathProperties = await _dataLakeClient.GetFilePathProperties(configuration, outputFileName);

        return new ExportJobData(
            streamId,
            streamModel,
            provider,
            configuration,
            asOfTime,
            OutputFormat: outputFormat,
            OutputFileName: outputFileName,
            filePathProperties);
    }

    private Dictionary<string, object> CreateLoggingScope(IDataLakeJobArgs args)
    {
        var typeName = GetType().Name;
        return new Dictionary<string, object>
        {
            [StreamIdKey] = args.Message,
            ["Schedule"] = args.Schedule,
            ["ExportJob"] = typeName,
            [InstanceTimeKey] = args.InstanceTime,
        };
    }

    public override async Task<bool> HasMissed(ExecutionContext context, IDataLakeJobArgs args)
    {
        var typeName = GetType().Name;
        using var exportJobLoggingScope = context.Log.BeginScope(CreateLoggingScope(args));
        context.Log.LogInformation(
            "Begin checking export entities job '{ExportJob}' for '{StreamId}' using {Schedule} at {InstanceTime}.",
            typeName,
            args.Message,
            args.Schedule,
            args.InstanceTime);

        var exportJobData = await GetJobDataAsync(context, args, "checking export");
        if (exportJobData == null)
        {
            context.Log.LogDebug("Unable to get export data information. Returning job not missed.");
            return false;
        }

        var (streamId, streamModel, provider, configuration, asOfTime, outputFormat, outputFileName, filePathProperties) = exportJobData;

        if (args.IsTriggeredFromJobServer)
        {
            context.Log.LogDebug(
                "Output file '{OutputFileName}' exists using data at {DataTime} and job is triggered from job server. Job not missed.",
                outputFileName,
                asOfTime);
            return false;
        }

        await using var connection = new SqlConnection(configuration.StreamCacheConnectionString);
        await connection.OpenAsync();
        var hasMissed = !await HasExported(context, connection, streamId, asOfTime, "InternalScheduler");
        context.Log.LogInformation(
            "End checking export entities job '{ExportJob}' for '{StreamId}' using {Schedule} at {InstanceTime}, HasMissed {HasMissed}.",
            typeName,
            args.Message,
            args.Schedule,
            args.InstanceTime,
            hasMissed);
        return hasMissed;
    }

    private static bool TryGetMetadata(IDictionary<string, string> metadata, out FileMetadata fileMetadata)
    {
        if(metadata != null
                && metadata.TryGetValue(StreamIdKey, out var fileStreamIdString)
                && metadata.TryGetValue(DataTimeKey, out var fileDataTimeString)
                && Guid.TryParse(fileStreamIdString, out var fileStreamId)
                && DateTimeOffset.TryParse(fileDataTimeString, out var fileDataTime))
        {
            fileMetadata = new FileMetadata(fileStreamId, fileDataTime);
            return true;
        }

        fileMetadata = null;
        return false;
    }

    private static bool HasExportedFileBefore(Guid streamId, DateTimeOffset asOfTime, IDictionary<string, string> metadata)
    {
        return TryGetMetadata(metadata, out var fileMetadata)
                && fileMetadata.StreamId == streamId
                && fileMetadata.DataTime == asOfTime;
    }

    protected virtual string GetOutputFileName(IDataLakeJobData configuration, Guid streamId, string containerName, DateTimeOffset asOfTime, string outputFormat)
    {
        if (HasCustomFileNamePattern(configuration))
        {
            return GetOutputFileNameUsingPattern(configuration.FileNamePattern, streamId, containerName, asOfTime, outputFormat);
        }

        return GetDefaultOutputFileName(streamId, containerName, asOfTime, outputFormat);
    }

    private static bool HasCustomFileNamePattern(IDataLakeJobData configuration)
    {
        return !string.IsNullOrWhiteSpace(configuration.FileNamePattern);
    }

    protected virtual string GetDefaultOutputFileName(Guid streamId, string containerName, DateTimeOffset asOfTime, string outputFormat)
    {
        var fileExtension = GetFileExtension(outputFormat);
        var outputFileName = $"{streamId}_{asOfTime:yyyyMMddHHmmss}.{fileExtension}";
        return outputFileName;
    }

    private static string GetOutputFileNameUsingPattern(string outputFileNamePattern, Guid streamId, string containerName, DateTimeOffset asOfTime, string outputFormat)
    {
        var timeRegexPattern = @"\{(DataTime)(\:[a-zA-Z0-9\-\._]+)?\}";
        var streamIdRegexPattern = @"\{(StreamId)(\:[a-zA-Z0-9\-\._]+)?\}";
        var containerNameRegexPattern = @"\{(ContainerName)(\:[a-zA-Z0-9\-\._]+)?\}";
        var outputFormatRegexPattern = @"\{(OutputFormat)(\:[a-zA-Z0-9\-\._]+)?\}";

        var timeReplaced = Replace(timeRegexPattern, outputFileNamePattern, (match, format) => asOfTime.ToString(format ?? "o"));
        var streamIdReplaced = Replace(streamIdRegexPattern, timeReplaced, (match, format) => streamId.ToString(format ?? "D"));
        var containerNameReplaced = Replace(containerNameRegexPattern, streamIdReplaced, (match, format) => containerName);
        var outputFormatReplaced = Replace(outputFormatRegexPattern, containerNameReplaced, (match, format) =>
        {
            return format?.ToLowerInvariant() switch
            {
                "toupper" => outputFormat.ToUpperInvariant(),
                "toupperinvariant" => outputFormat.ToUpperInvariant(),
                "tolower" => outputFormat.ToLowerInvariant(),
                "tolowerinvariant" => outputFormat.ToLowerInvariant(),
                null => outputFormat,
                _ => throw new NotSupportedException($"Format '{format}' is not supported"),
            };
        });

        return outputFormatReplaced;
    }

    private static string Replace(string pattern, string input, Func<Match, string, string> formatter)
    {
        var regex = new Regex(pattern);
        var matches = regex.Matches(input);
        var result = input;
        foreach (var match in matches.Reverse())
        {
            if (match.Groups.Count != 3 || !match.Groups[1].Success)
            {
                continue;
            }
            var format = match.Groups[2].Success
                ? match.Groups[2].Captures.Single().Value[1..]
                : null;
            var formatted = formatter(match, format);
            result = $"{result[0..match.Index]}{formatted}{result[(match.Index + match.Length)..]}";
        }

        return result;
    }

    protected virtual string GetFileExtension(string outputFormat)
    {
        return outputFormat;
    }

    private DateTimeOffset GetAsOfTime(ExecutionContext context, IDataLakeJobArgs args, IDataLakeJobData jobData)
    {
        if (jobData.UseCurrentTimeForExport || args.Schedule == CronSchedules.NeverCron)
        {
            context.Log.LogDebug("Using current time for export.");
            return _dateTimeOffsetProvider.GetCurrentUtcTime();
        }

        return args.InstanceTime;
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

    private async Task InsertHistory(ExecutionContext context, SqlConnection connection, ExportHistory exportHistory)
    {
        try
        {
            await insert(connection, exportHistory);
        }
        catch (SqlException writeDataException) when (writeDataException.IsTableNotFoundException())
        {
            var tableName = GetExportHistoryTableName(exportHistory.StreamId);
            context.Log.LogDebug("Table {TableName} does not exist. Creating it now.", tableName);
            await EnsureHistoryTableExists(connection, exportHistory.StreamId);
            await insert(connection, exportHistory);
        }

        static async Task insert(SqlConnection connection, ExportHistory exportHistory)
        {
            var tableName = GetExportHistoryTableName(exportHistory.StreamId);
            var insertSql = $"""
                        INSERT INTO [{tableName}] (
                            StreamId,
                            DataTime,
                            TriggerSource,
                            CronSchedule,
                            FilePath,
                            StartTime,
                            EndTime,
                            TotalRows,
                            Status,
                            ExporterHostName
                        )
                        VALUES(
                            @StreamId,
                            @DataTime,
                            @TriggerSource,
                            @CronSchedule,
                            @FilePath,
                            @StartTime,
                            @EndTime,
                            @TotalRows,
                            @Status,
                            @ExporterHostName
                        )
                        """;
            var command = new SqlCommand(insertSql, connection)
            {
                CommandType = CommandType.Text
            };
            command.Parameters.Add(new SqlParameter($"@StreamId", exportHistory.StreamId));
            command.Parameters.Add(new SqlParameter($"@DataTime", exportHistory.DataTime));
            command.Parameters.Add(new SqlParameter($"@TriggerSource", exportHistory.TriggerSource));
            command.Parameters.Add(new SqlParameter($"@CronSchedule", exportHistory.CronSchedule));
            command.Parameters.Add(new SqlParameter($"@FilePath", exportHistory.FilePath));
            command.Parameters.Add(new SqlParameter($"@StartTime", exportHistory.StartTime));
            command.Parameters.Add(new SqlParameter($"@EndTime", exportHistory.EndTime));
            command.Parameters.Add(new SqlParameter($"@TotalRows", exportHistory.TotalRows));
            command.Parameters.Add(new SqlParameter($"@Status", exportHistory.Status));
            command.Parameters.Add(new SqlParameter($"@ExporterHostName", exportHistory.ExporterHostName));

            var rowsAffected = await command.ExecuteNonQueryAsync();
            if (rowsAffected != 1)
            {
                throw new ApplicationException($"Rows affected for insertion of is not 1, it is {rowsAffected}.");
            }
        }
    }

    private async Task UpdateHistory(ExecutionContext context, SqlConnection connection, ExportHistory exportHistory)
    {
        var tableName = GetExportHistoryTableName(exportHistory.StreamId);
        var insertSql = $"""
                    UPDATE [{tableName}] SET
                        EndTime = @EndTime,
                        TotalRows = @TotalRows,
                        Status = @Status
                    WHERE
                        StreamId = @StreamId
                        AND DataTime = @DataTime
                        AND TriggerSource = @TriggerSource
                    """;
        var command = new SqlCommand(insertSql, connection)
        {
            CommandType = CommandType.Text
        };
        command.Parameters.Add(new SqlParameter($"@StreamId", exportHistory.StreamId));
        command.Parameters.Add(new SqlParameter($"@DataTime", exportHistory.DataTime));
        command.Parameters.Add(new SqlParameter($"@TriggerSource", exportHistory.TriggerSource));
        command.Parameters.Add(new SqlParameter($"@EndTime", exportHistory.EndTime));
        command.Parameters.Add(new SqlParameter($"@TotalRows", exportHistory.TotalRows));
        command.Parameters.Add(new SqlParameter($"@Status", exportHistory.Status));

        var rowsAffected = await command.ExecuteNonQueryAsync();
        if (rowsAffected != 1)
        {
            throw new ApplicationException($"Rows affected for update of is not 1, it is {rowsAffected}.");
        }
    }

    private async Task<bool> HasExported(ExecutionContext context, SqlConnection connection, Guid streamId, DateTimeOffset dataTime, string triggerSource)
    {
        try
        {
            return await hasExported(connection, streamId, dataTime, triggerSource);
        }
        catch (SqlException writeDataException) when (writeDataException.IsTableNotFoundException())
        {
            var tableName = GetExportHistoryTableName(streamId);
            context.Log.LogDebug("Table {TableName} does not exist. Returning has exported false.", tableName);
            return false;
        }

        static async Task<bool> hasExported(SqlConnection connection, Guid streamId, DateTimeOffset dataTime, string triggerSource)
        {
            var tableName = GetExportHistoryTableName(streamId);
            var insertSql = $"""
                        SELECT COUNT(1)
                        FROM
                            [{tableName}]
                        WHERE
                            StreamId = @StreamId
                            AND DataTime = @DataTime
                            AND TriggerSource = @TriggerSource
                        """;
            var command = new SqlCommand(insertSql, connection)
            {
                CommandType = CommandType.Text
            };
            command.Parameters.Add(new SqlParameter($"@StreamId", streamId));
            command.Parameters.Add(new SqlParameter($"@DataTime", dataTime));
            command.Parameters.Add(new SqlParameter($"@TriggerSource", triggerSource));


            var count = (int)await command.ExecuteScalarAsync();
            return count > 0;
        }
    }

    private async Task EnsureHistoryTableExists(SqlConnection connection, Guid streamId)
    {
        var tableName = GetExportHistoryTableName(streamId);
        var createTableSql = $"""
                IF NOT EXISTS (SELECT * FROM SYSOBJECTS WHERE NAME='{tableName}' AND XTYPE='U')
                CREATE TABLE [{tableName}] (
                    StreamId UNIQUEIDENTIFIER NOT NULL,
                    DataTime DATETIME2 NOT NULL,
                    TriggerSource NVARCHAR(255) NOT NULL,
                    CronSchedule NVARCHAR(255) NOT NULL,
                    FilePath NVARCHAR(255) NOT NULL,
                    StartTime DATETIME2 NOT NULL,
                    EndTime DATETIME2 NULL,
                    TotalRows INT NULL,
                    Status NVARCHAR(255) NULL,
                    ExporterHostName NVARCHAR(255) NULL,
                    CONSTRAINT [PK_{tableName}] PRIMARY KEY CLUSTERED (StreamId,DataTime,TriggerSource)
                );
                """;
        var command = new SqlCommand(createTableSql, connection)
        {
            CommandType = CommandType.Text
        };
        _ = await command.ExecuteNonQueryAsync();
    }

    private static string GetExportHistoryTableName(Guid streamId)
    {
        return CacheTableHelper.GetCacheTableName(streamId) + "_ExportHistory";
    }

    private record FileMetadata(Guid StreamId, DateTimeOffset DataTime);

    private record ExportJobData(
        Guid StreamId,
        StreamModel StreamModel,
        ProviderDefinition ProviderDefinition,
        IDataLakeJobData DataLakeJobData,
        DateTimeOffset AsOfTime,
        string OutputFormat,
        string OutputFileName,
        PathProperties PathProperties);

    private record ExportHistory(
        Guid StreamId,
        DateTimeOffset DataTime,
        string TriggerSource,
        string CronSchedule,
        string FilePath,
        DateTimeOffset StartTime,
        DateTimeOffset? EndTime,
        long TotalRows,
        string Status,
        string ExporterHostName);
}
