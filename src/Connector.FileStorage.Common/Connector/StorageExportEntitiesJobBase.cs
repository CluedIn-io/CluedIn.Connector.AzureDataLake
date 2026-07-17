using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using System.Transactions;

using CluedIn.ComponentHealth.Services;
using CluedIn.ComponentHealth.Services.Models;
using CluedIn.ComponentHealth.Storage;
using CluedIn.Connector.FileStorage.Common.Connector.SqlDataWriter;
using CluedIn.Core;
using CluedIn.Core.Streams;
using CluedIn.Core.Streams.Models;
using CluedIn.Streams.StreamLog;
using CluedIn.Streams.StreamLog.History;

using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

using ProviderDefinition = CluedIn.Core.Data.Relational.ProviderDefinition;

namespace CluedIn.Connector.FileStorage.Common.Connector;

internal abstract class StorageExportEntitiesJobBase : StorageJobBase
{
    private readonly IStreamRepository _streamRepository;
    private readonly IStorageConfigurationConstants _dataLakeConstants;
    private readonly IStorageFactory _storageFactory;
    private readonly IDateTimeOffsetProvider _dateTimeOffsetProvider;
    private static readonly TimeSpan _exportTimeout = TimeSpan.MaxValue;
    private const int ExportEntitiesLockInMilliseconds = 100;
    private const string StreamIdKey = "StreamId";
    private const string DataTimeKey = "DataTime";
    private const string InstanceTimeKey = "InstanceTime";
    private const string TemporaryFileSuffix = ".tmp";

    // Status values
    private const string CompleteStatus = "Complete";
    private const string FailedStatus = "Failed";
    private const string SkippedStatus = "Skipped";

    // Export result reasons
    internal static readonly string ConnectorIsNotHealthyReason = "Connector is not healthy";
    internal static readonly string StreamNotStartedReason = "Stream is not started";
    internal static readonly string ExportedBeforeReason = "Exported before";
    internal static readonly string NoRowsReason = "No rows to be exported.";
    internal static readonly string TableNotFoundReason = "Table not found.";
    internal static readonly string NonOverwritableOutputFileExists = "Output file exists and file cannot be overwritten";

    protected StorageExportEntitiesJobBase(
        ApplicationContext appContext,
        IStreamRepository streamRepository,
        IStorageConfigurationConstants configurationConstants,
        IStorageFactory storageFactory,
        IDateTimeOffsetProvider dateTimeOffsetProvider) : base(appContext, dateTimeOffsetProvider)
    {
        _streamRepository = streamRepository ?? throw new ArgumentNullException(nameof(streamRepository));
        _dataLakeConstants = configurationConstants ?? throw new ArgumentNullException(nameof(configurationConstants));
        _storageFactory = storageFactory ?? throw new ArgumentNullException(nameof(storageFactory));
        _dateTimeOffsetProvider = dateTimeOffsetProvider ?? throw new ArgumentNullException(nameof(dateTimeOffsetProvider));
    }

    protected virtual string StreamIdDefaultStringFormat => "N";

    public override async Task DoRunAsync(ExecutionContext context, IStorageJobArgs args)
    {
        _ = await DoRunInternalAsync(context, args);
    }

    internal async Task<ExportResult> DoRunInternalAsync(ExecutionContext context, IStorageJobArgs args)
    {
        using var exportJobLoggingScope = context.Log.BeginScope(CreateLoggingScope(args));
        context.Log.LogInformation(
            "Begin export entities job '{ExportJob}' for '{StreamId}' using {Schedule} at {InstanceTime}.",
            GetType().Name,
            args.Message,
            args.Schedule,
            args.InstanceTime);

        var streamId = new Guid(args.Message);
        var streamModel = await _streamRepository.GetStreamEx(context, streamId);

        if (streamModel.Status != StreamStatus.Started)
        {
            context.Log.LogDebug("Skipping export for StreamId {StreamId} as it is not started.", streamId);
            return ExportResult.CreateSkipped(StreamNotStartedReason);
        }


        if (streamModel.ConnectorProviderDefinitionId == null)
        {
            context.Log.LogInformation("Skipping export for StreamId {StreamId} as it does not have connector provider definition id.", streamId);
            await AddErrorToStreamIngestionLog(context, streamModel, $"Skipping export for StreamId {streamId} as it does not have connector provider definition id.");
            return ExportResult.CreateSkipped("Stream does not have connector provider definition id");
        }

        if (!await IsConnectorHealthyAsync(context, streamModel.ConnectorProviderDefinitionId.Value))
        {
            context.Log.LogInformation("Skipping export for StreamId {StreamId} as provider definition {ProviderDefinitionId} is not healthy.", streamId, streamModel.ConnectorProviderDefinitionId);
            await AddErrorToStreamIngestionLog(context, streamModel, $"Skipping export for StreamId {streamId} as provider definition {streamModel.ConnectorProviderDefinitionId} is not healthy.");
            return ExportResult.CreateSkipped(ConnectorIsNotHealthyReason);
        }

        var exportJobData = await GetJobDataAsync(context, args, streamModel, "export");
        if (exportJobData == null)
        {
            context.Log.LogInformation("Skipping export for StreamId {streamId} because unable to get export data information.", streamId);
            await AddErrorToStreamIngestionLog(context, streamModel, $"Skipping export for StreamId {streamId} because unable to get export data information.");
            return ExportResult.CreateSkipped("Unable to get export data information.");
        }

        try
        {
            using var transactionScope = new TransactionScope(
                TransactionScopeOption.Required,
                _exportTimeout,
                TransactionScopeAsyncFlowOption.Enabled);
            await using var connection = new SqlConnection(exportJobData.StorageConfiguration.StreamCacheConnectionString);
            await connection.OpenAsync();

            if (!await DistributedLockHelper.TryAcquireExclusiveLock(connection, $"{GetType().Name}_{streamId}", ExportEntitiesLockInMilliseconds))
            {
                context.Log.LogInformation("Unable to acquire lock to export data for Stream '{StreamId}'. Skipping export.", streamId);
                return ExportResult.CreateSkipped("Failed to acquire lock");
            }
            return await DoRunInternalAsync(context, args, exportJobData, transactionScope, connection);
        }
        catch (Exception ex)
        {
            await AddErrorToStreamIngestionLog(context, streamModel, $"Error exporting for StreamId {exportJobData.StreamId} to file {exportJobData.OutputFileName}.", exception: ex);
            throw;
        }
    }

    private async Task<ExportResult> DoRunInternalAsync(
        ExecutionContext context,
        IStorageJobArgs args,
        ExportJobData exportJobData,
        TransactionScope transactionScope,
        SqlConnection connection)
    {
        var streamId = exportJobData.StreamId;
        var streamModel = exportJobData.StreamModel;
        var provider = exportJobData.ProviderDefinition;
        var configuration = exportJobData.StorageConfiguration;
        var asOfTime = exportJobData.AsOfTime;
        var outputFormat = exportJobData.OutputFormat;
        var outputFileName = exportJobData.OutputFileName;
        var baseDirectoryPath = exportJobData.BaseDirectoryPath;
        var outputDirectoryPath = exportJobData.OutputDirectoryPath;

        var tableName = CacheTableHelper.GetCacheTableName(streamId);

        using var storageClient = await CreateStorageClient(context, configuration);
        await storageClient.CreateDirectoryIfNotExistsAsync(outputDirectoryPath);

        if (await HasExported(context, connection, streamId, asOfTime, GetTriggerSource(args), args.Schedule))
        {
            context.Log.LogInformation("Skipping export for StreamId {StreamId} as it is not required. Reason is {Reason}", exportJobData.StreamId, ExportedBeforeReason);
            // Not logging to stream ingestion log to prevent spamming
            return ExportResult.CreateSkipped(ExportedBeforeReason);
        }

        var shouldSkipResult = await ShouldSkipExport(context, exportJobData, storageClient);
        if (shouldSkipResult.ShouldSkip)
        {
            context.Log.LogInformation("Skipping export for StreamId {StreamId} as it is not required. Reason is {Reason}", exportJobData.StreamId, shouldSkipResult.Reason);
            await AddInformationToStreamIngestionLogLocal($"Skipping export as it is not required. Reason is {shouldSkipResult.Reason}");
            return ExportResult.CreateSkipped(shouldSkipResult.Reason);
        }

        var exportHistory = new ExportHistory(
            streamId,
            DataTime: asOfTime,
            TriggerSource: GetTriggerSource(args),
            CronSchedule: args.Schedule,
            FilePath: outputFileName,
            FileFormat: outputFormat,
            StartTime: _dateTimeOffsetProvider.GetCurrentUtcTime(),
            EndTime: null,
            TotalRows: null,
            Status: "Starting",
            ExporterHostName: Dns.GetHostName());
        await InsertHistory(context, connection, exportHistory);

        var shouldProduceDelta = configuration.IsDeltaMode && !exportJobData.IsInitialExport;
        var validFrom = shouldProduceDelta ? exportJobData.LastExportedFile?.DataTime : null;

        var getDataCommandWithLimit = GetDataSql(connection, asOfTime, tableName, validFrom, limit: 1);
        var hasData = false;
        try
        {
            hasData = await HasDataAsync(getDataCommandWithLimit);
        }
        catch (Exception ex) when (ex is SqlException sqlEx && sqlEx.IsTableNotFoundException())
        {
            await UpdateHistoryWithStatus(SkippedStatus);
            context.Log.LogInformation("Skipping export for StreamId {StreamId} as it is not required. Reason is {Reason}", exportJobData.StreamId, TableNotFoundReason);
            await AddInformationToStreamIngestionLogLocal($"Skipping export as it is not required. Reason is {TableNotFoundReason}");
            return ExportResult.CreateSkipped(TableNotFoundReason);
        }

        if (!configuration.IsOverwriteEnabled && exportJobData.OutputFileExists)
        {
            await UpdateHistoryWithStatus(SkippedStatus);
            context.Log.LogWarning("Skipping export for StreamId {StreamId} as output file exists. Reason is {Reason}", exportJobData.StreamId, NonOverwritableOutputFileExists);
            await AddInformationToStreamIngestionLogLocal($"Skipping export as it is not required. Reason is {NonOverwritableOutputFileExists}");
            return ExportResult.CreateSkipped(NonOverwritableOutputFileExists);
        }

        if (configuration.IsDeltaMode && !hasData && !GetIsEmptyFileAllowed(exportJobData))
        {
            await UpdateHistoryWithStatus(SkippedStatus);
            context.Log.LogWarning("Skipping export for StreamId {StreamId} as it is not required. Reason is {Reason}", exportJobData.StreamId, NoRowsReason);
            await AddInformationToStreamIngestionLogLocal($"Skipping export as it is not required. Reason is {NoRowsReason}");
            return ExportResult.CreateSkipped(NoRowsReason);
        }

        await InitializeBaseDirectoryAsync(context, connection, exportJobData, storageClient);
        await InitializeOutputDirectoryAsync(context, connection, exportJobData, storageClient);

        var getDataCommand = GetDataSql(connection, asOfTime, tableName, validFrom);

        await using var reader = await getDataCommand.ExecuteReaderAsync();

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

        var outputFilePath = outputDirectoryPath.GetFilePath(outputFileName);
        IStorageFileClient temporaryFileClient;
        var temporaryFilePath = outputDirectoryPath.GetFilePath(temporaryOutputFileName);
        try
        {
            temporaryFileClient = await storageClient.GetFileClientAsync(temporaryFilePath);
        }
        catch (Exception getTemporaryFileClientException)
        {
            context.Log.LogWarning(
                "Error creating file client for {TemporaryOutputFileName}.",
                temporaryOutputFileName);
            transactionScope.Complete();
            transactionScope.Dispose();
            await AddErrorToStreamIngestionLog(context, streamModel, $"Error creating file client for {temporaryOutputFileName}.", exception: getTemporaryFileClientException);
            throw;
        }

        context.Log.LogInformation(
            "Begin writing to file '{OutputFileName}' using data at {DataTime} and {TemporaryOutputFileName} ({TemporaryFileClientUri}).",
            outputFileName,
            asOfTime,
            temporaryOutputFileName,
            temporaryFileClient.Uri);

        var totalRows = await writeFileContentsAsync();
        if (configuration.IsDeltaMode && totalRows == 0 && !GetIsEmptyFileAllowed(exportJobData))
        {
            context.Log.LogDebug(
                "File '{FileName}' for StreamId {StreamId} and DataTime {DataTime} has zero rows. It will be deleted because empty file is not allowed.",
                temporaryOutputFileName,
                streamId,
                asOfTime);
            await deleteFileIfExistsAsync(temporaryFilePath);
        }
        else
        {
            await setFilePropertiesAsync();
            await deleteFileIfExistsAsync(outputFilePath);
            await renameToTargetFileAsync();
            await PostExportAsync(context, exportJobData);
        }
        context.Log.LogInformation(
            "End writing to file '{OutputFileName}' using data at {DataTime} and {TemporaryOutputFileName}.",
            outputFileName,
            asOfTime,
            temporaryOutputFileName);

        await reader.CloseAsync();

        await UpdateHistoryWithStatus(CompleteStatus, totalRows: totalRows);
        transactionScope.Complete();
        transactionScope.Dispose();
        context.Log.LogInformation(
            "End export entities job '{ExportJob}' for '{StreamId}' using {Schedule}.",
            GetType().Name,
            args.Message,
            args.Schedule);

        await this.AddInformationToStreamIngestionLog(context, streamModel, $"Exported file {outputFileName} with {totalRows} rows.");
        return ExportResult.CreateSuccess(outputFilePath);

        async Task<long> writeFileContentsAsync()
        {
            var fieldNamesToUse = await GetFieldNamesAsync(context, exportJobData, configuration, fieldNames);
            var sqlDataWriter = GetSqlDataWriter(outputFormat);
            // Temporary file should always be allowed to be overwritten
            // It is only used for the current export job and will be renamed to the final output file name after the export is complete.
            await using var outputStream = await temporaryFileClient.OpenWriteAsync(true);
            return await sqlDataWriter?.WriteAsync(context, configuration, outputStream, fieldNamesToUse, exportJobData.IsInitialExport, reader);
        }

        async Task setFilePropertiesAsync()
        {
            context.Log.LogDebug(
                "Begin setting file properties to file '{OutputFileName}' StreamId {StreamId} and DataTime {DataTime}.",
                outputFileName,
                streamId,
                asOfTime);
            await temporaryFileClient.SetMetadataAsync(
                new FileMetadata(new Dictionary<string, string>
                {
                    [TransformMetadataKey(StreamIdKey)] = streamId.ToString(),
                    [TransformMetadataKey(DataTimeKey)] = asOfTime.ToString("O"),
                }));
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
            await temporaryFileClient.RenameAsync(outputFilePath);
            context.Log.LogDebug(
                "End rename temporary file {TemporaryOutputFileName} to '{OutputFileName}' for StreamId {StreamId} and DataTime {DataTime}.",
                temporaryOutputFileName,
                outputFileName,
                streamId,
                asOfTime);
        }

        async Task deleteFileIfExistsAsync(FilePath filePath)
        {
            context.Log.LogDebug(
                "Deleting file '{FileName}' for StreamId {StreamId} and DataTime {DataTime}.",
                filePath.FullPath,
                streamId,
                asOfTime);
            var targetFileClient = await storageClient.GetFileClientAsync(filePath);
            await targetFileClient.DeleteIfExistsAsync();
        }

        static SqlCommand GetDataSql(SqlConnection connection, DateTimeOffset asOfTime, string tableName, DateTimeOffset? validFrom, int? limit = null)
        {
            var limitClause = limit.HasValue ? $"TOP ({limit.Value}) " : string.Empty;
            var getDataSql = validFrom.HasValue
                ? $"SELECT {limitClause}* FROM [{tableName}] FOR SYSTEM_TIME AS OF '{asOfTime:o}' WHERE ValidFrom > @ValidFrom"
                : $"SELECT {limitClause}* FROM [{tableName}] FOR SYSTEM_TIME AS OF '{asOfTime:o}'";
            var command = new SqlCommand(getDataSql, connection)
            {
                CommandType = CommandType.Text
            };

            if (validFrom.HasValue)
            {
                command.Parameters.Add(new SqlParameter("@ValidFrom", validFrom));
            }

            return command;
        }

        static async Task<bool> HasDataAsync(SqlCommand getDataCommandWithLimit)
        {
            await using var reader = await getDataCommandWithLimit.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                return true;
            }

            return false;
        }

        async Task AddInformationToStreamIngestionLogLocal(string message)
        {
            transactionScope.Complete();
            transactionScope.Dispose();
            await this.AddInformationToStreamIngestionLog(context, streamModel, message: message);
        }

        async Task UpdateHistoryWithStatus(string status, long? totalRows = null)
        {
            await UpdateHistory(context, connection, exportHistory with
            {
                Status = status,
                TotalRows = totalRows,
                EndTime = _dateTimeOffsetProvider.GetCurrentUtcTime()
            });
        }
    }

    public override async Task<bool> CanRunAsync(ExecutionContext context, IStorageJobArgs args)
    {
        if (string.IsNullOrWhiteSpace(args.Message))
        {
            context.Log.LogDebug("Skipping job execution because StreamId is missing in job arguments.");
            return false;
        }

        var model = await _streamRepository.GetStreamEx(context, new Guid(args.Message));

        if (model?.ConnectorProviderDefinitionId == null)
        {
            return false;
        }

        var isHealthy = await IsConnectorHealthyAsync(context, model.ConnectorProviderDefinitionId.Value);
        return isHealthy;
    }

    private async Task<bool> IsConnectorHealthyAsync(
        ExecutionContext executionContext,
        Guid providerDefinitionId)
    {
        var componentHealthService = executionContext.ApplicationContext.Container.Resolve<IComponentHealthService>();
        var result = await componentHealthService.GetComponentHealth(executionContext, ComponentArea.Connector, providerDefinitionId);

        if (result.Status != ComponentHealthStatus.Healthy)
        {
            return false;
        }

        return true;
    }

    private async Task AddInformationToStreamIngestionLog(
        ExecutionContext executionContext,
        StreamModel streamModel,
        string message)
    {
        try
        {
            var streamLogService = executionContext.ApplicationContext.Container.Resolve<IStreamLogService>();
            await streamLogService.StoreHistoryLogEntryAsync(StreamHistoryLogHistoryModel.BuildInformation(
                    streamId: streamModel.Id,
                    entityId: null,
                    exportTargetId: streamModel.ConnectorProviderDefinitionId,
                    area: StreamHistoryLogAreaEnum.ExportTarget,
                    changeType: null,
                    message: message));
        }
        catch (Exception ex)
        {
            executionContext.Log.LogWarning(ex, "Failed to store stream ingestion history log entry.");
        }
    }

    private async Task AddErrorToStreamIngestionLog(
        ExecutionContext executionContext,
        StreamModel streamModel,
        string message,
        Exception? exception = null)
    {
        try
        {
            var streamLogService = executionContext.ApplicationContext.Container.Resolve<IStreamLogService>();
            await streamLogService.StoreHistoryLogEntryAsync(StreamHistoryLogHistoryModel.BuildError(
                    streamId: streamModel.Id,
                    entityId: null,
                    exportTargetId: streamModel.ConnectorProviderDefinitionId,
                    area: StreamHistoryLogAreaEnum.ExportTarget,
                    changeType: null,
                    message: message,
                    exceptions: exception == null ? Array.Empty<Exception>() : new[] { exception }));
        }
        catch (Exception ex)
        {
            executionContext.Log.LogWarning(ex, "Failed to store stream ingestion history log entry.");
        }
    }

    protected async Task<IStorageClient> CreateStorageClient(ExecutionContext context, IStorageConfiguration configuration)
    {
        return await _storageFactory.CreateStorageClient(context, configuration);
    }

    private protected virtual async Task<ShouldSkipResult> ShouldSkipExport(ExecutionContext context, ExportJobData exportJobData, IStorageClient storageClient)
    {
        if (exportJobData.OutputFileExists && exportJobData.ExistingFileMatchesExpected)
        {
            context.Log.LogInformation(
                "Output file '{OutputFileName}' exists using data at {DataTime} and job is triggered from {SchedulerType}. Skipping export.",
                exportJobData.OutputFileName,
                exportJobData.AsOfTime,
                nameof(StorageConnectorComponentBase));
            return new ShouldSkipResult(true, ExportedBeforeReason);
        }

        return new ShouldSkipResult(false, null);
    }

    private protected virtual async Task<List<string>> GetFieldNamesAsync(
        ExecutionContext context,
        ExportJobData exportJobData,
        IStorageConfiguration configuration,
        List<string> fieldNames)
    {
        return configuration.IsDeltaMode
                ? fieldNames
                : fieldNames.Where(fieldName => fieldName != StorageConfigurationConstants.ChangeTypeKey).ToList();
    }

    private protected virtual bool GetIsEmptyFileAllowed(ExportJobData exportJobData) => true;

    private protected virtual Task PostExportAsync(ExecutionContext context, ExportJobData exportJobData)
    {
        return Task.CompletedTask;
    }

    private protected virtual Task<string> GetOutputDirectoryNameAsync(
        ExecutionContext executionContext,
        IStorageConfiguration configuration,
        ExportJobDataBase exportJobData)
    {
        return Task.FromResult(string.Empty);
    }

    private protected virtual Task InitializeBaseDirectoryAsync(
        ExecutionContext context,
        SqlConnection connection,
        ExportJobData exportJobData,
        IStorageClient client)
    {
        return Task.CompletedTask;
    }

    private protected virtual Task InitializeOutputDirectoryAsync(
        ExecutionContext context,
        SqlConnection connection,
        ExportJobData exportJobData,
        IStorageClient client)
    {
        return Task.CompletedTask;
    }


    private static string GetTriggerSource(IStorageJobArgs args)
    {
        return args.IsTriggeredFromJobServer ? "JobServer" : "InternalScheduler";
    }

    private async Task<ExportJobData> GetJobDataAsync(ExecutionContext context, IStorageJobArgs args, StreamModel streamModel, string taskName)
    {
        using var exportJobLoggingScope = context.Log.BeginScope(CreateLoggingScope(args));

        if (streamModel == null)
        {
            context.Log.LogWarning("StreamModel is null. Skipping {TaskName}.", taskName);
            return null;
        }

        if (streamModel.ConnectorProviderDefinitionId == null)
        {
            context.Log.LogWarning("Stream {StreamId} does not have a ConnectorProviderDefinitionId. Skipping {TaskName}.", streamModel.Id, taskName);
            return null;
        }

        var organizationProviderDataStore = context.Organization.DataStores.GetDataStore<ProviderDefinition>();
        var streamId = streamModel.Id;

        var providerDefinitionId = streamModel.ConnectorProviderDefinitionId!.Value;
        var provider = await organizationProviderDataStore.GetByIdAsync(context, providerDefinitionId);

        if (provider == null)
        {
            context.Log.LogWarning("Unable to get provider {ProviderDefinitionId}. Skipping {TaskName}.", providerDefinitionId, taskName);
            return null;
        }

        if (provider.ProviderId != _dataLakeConstants.ProviderId)
        {
            context.Log.LogDebug(
                "Unable to get provider {ProviderDefinitionId}. Skipping {DataLakeProviderId}. Skipping {TaskName}.",
                provider.ProviderId,
                _dataLakeConstants.ProviderId,
                taskName);
            return null;
        }

        if (!provider.IsEnabled)
        {
            context.Log.LogDebug("Provider {ProviderDefinitionId} is not enabled. Skipping {TaskName}.", providerDefinitionId, taskName);
            return null;
        }

        var containerName = streamModel.ContainerName;
        var executionContext = context.ApplicationContext.CreateExecutionContext(streamModel.OrganizationId);

        var configuration = await _storageFactory.CreateStorageConfiguration(executionContext, streamModel);

        if (!configuration.IsStreamCacheEnabled)
        {
            context.Log.LogDebug("Stream cache is not enabled for stream {StreamId}. Skipping {TaskName}.", streamModel.Id, taskName);
            return null;
        }

        if (streamModel.Status != StreamStatus.Started)
        {
            context.Log.LogInformation("Stream not started for stream {StreamId}. Skipping {TaskName}.", streamModel.Id, taskName);
            return null;
        }

        await using var connection = new SqlConnection(configuration.StreamCacheConnectionString);
        await connection.OpenAsync();

        var asOfTime = GetAsOfTime(context, args, configuration);
        var outputFormat = configuration.OutputFormat.ToLowerInvariant();
        var exportJobDataBase = new ExportJobDataBase(
            streamId,
            streamModel,
            provider,
            configuration,
            asOfTime,
            OutputFormat: outputFormat,
            ContainerName: containerName);

        using var storageClient = await CreateStorageClient(context, configuration);
        var baseDirectoryPath = await storageClient.GetBaseDirectoryPathAsync();

        var outputDirectoryName = await GetOutputDirectoryNameAsync(context, configuration, exportJobDataBase);
        var outputDirectoryPath = string.IsNullOrWhiteSpace(outputDirectoryName) ? baseDirectoryPath : baseDirectoryPath.GetSubDirectoryPath(outputDirectoryName);

        var lastExportedFile = await GetLastExportedFile(context, connection, exportJobDataBase, storageClient, outputDirectoryPath);

        var isInitialExport = await GetIsInitialExport(context, exportJobDataBase, storageClient, lastExportedFile, outputDirectoryPath);
        var outputFileName = await GetOutputFileNameAsync(context, exportJobDataBase, isInitialExport, lastExportedFile, outputDirectoryPath);

        var outputFileExists = false;
        var existingFileMatchesExpected = false;
        var fileMetadata = await storageClient.GetFileMetadataAsync(outputDirectoryPath.GetFilePath(outputFileName));
        if (fileMetadata != null)
        {
            outputFileExists = true;
            if (args.IsTriggeredFromJobServer)
            {
                context.Log.LogInformation(
                    "Output file '{OutputFileName}' exists using data at {DataTime} and job is triggered from job server. Switching to using current time.",
                    outputFileName,
                    asOfTime);
                asOfTime = _dateTimeOffsetProvider.GetCurrentUtcTime();
                outputFileName = await GetOutputFileNameAsync(context, exportJobDataBase with { AsOfTime = asOfTime }, isInitialExport, lastExportedFile, outputDirectoryPath);
                outputFileExists = false;
            }
            else if (HasExportedFileBefore(streamId, asOfTime, fileMetadata.Metadata))
            {
                existingFileMatchesExpected = true;
            }
            else
            {
                context.Log.LogInformation(
                    "Output file '{OutputFileName}' exists and will be overwritten.", outputFileName);
            }
        }
        var exportJobData = new ExportJobData(
            streamId,
            streamModel,
            provider,
            configuration,
            asOfTime,
            OutputFormat: outputFormat,
            ContainerName: containerName,
            OutputFileName: outputFileName,
            BaseDirectoryPath: baseDirectoryPath,
            OutputDirectoryPath: outputDirectoryPath,
            OutputFileExists: outputFileExists,
            ExistingFileMatchesExpected: existingFileMatchesExpected,
            LastExportedFile: lastExportedFile,
            IsInitialExport: isInitialExport);
        return exportJobData;
    }

    protected virtual async Task<LastExportedFile?> GetLastExportedFile(ExecutionContext context, SqlConnection connection, ExportJobDataBase exportJobDataBase, IStorageClient storageClient, DirectoryPath outputDirectoryPath)
    {
        var lastSuccessHistory = await GetLastSuccessfulExportHistory(context, connection, exportJobDataBase.StreamId);
        var lastExportedFile = lastSuccessHistory == null ? null : new LastExportedFile(lastSuccessHistory.FilePath, lastSuccessHistory.DataTime, lastSuccessHistory.TotalRows);
        return lastExportedFile;
    }

    protected virtual Task<bool> GetIsInitialExport(ExecutionContext context, ExportJobDataBase exportJobDataBase, IStorageClient storageClient, LastExportedFile? lastExportedFile, DirectoryPath outputDirectoryPath)
    {
        return Task.FromResult(lastExportedFile == null);
    }

    private Dictionary<string, object> CreateLoggingScope(IStorageJobArgs args)
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

    public override async Task<bool> HasMissed(ExecutionContext context, IStorageJobArgs args)
    {
        var typeName = GetType().Name;
        using var exportJobLoggingScope = context.Log.BeginScope(CreateLoggingScope(args));
        context.Log.LogInformation(
            "Begin checking export entities job '{ExportJob}' for '{StreamId}' using {Schedule} at {InstanceTime}.",
            typeName,
            args.Message,
            args.Schedule,
            args.InstanceTime);

        var model = await _streamRepository.GetStreamEx(context, new Guid(args.Message));

        if (model == null)
        {
            context.Log.LogDebug("Skipping check for StreamId {StreamId} because stream could not be found.", args.Message);
            return false;
        }

        if (model.ConnectorProviderDefinitionId == null)
        {
            context.Log.LogDebug("Skipping check for StreamId {StreamId} as it does not have connector provider definition id.", model.Id);
            return false;
        }

        var isHealthy = await IsConnectorHealthyAsync(context, model.ConnectorProviderDefinitionId.Value);
        if (!isHealthy)
        {
            context.Log.LogInformation("Provider definition {ProviderDefinitionId} is not healthy. Skipping check and returning false.", model.ConnectorProviderDefinitionId);
            return false;
        }

        var exportJobData = await GetJobDataAsync(context, args, model, "checking export");
        if (exportJobData == null)
        {
            context.Log.LogDebug("Unable to get export data information. Returning job not missed.");
            return false;
        }

        var streamId = exportJobData.StreamId;
        var streamModel = exportJobData.StreamModel;
        var provider = exportJobData.ProviderDefinition;
        var configuration = exportJobData.StorageConfiguration;
        var asOfTime = exportJobData.AsOfTime;
        var outputFormat = exportJobData.OutputFormat;
        var outputFileName = exportJobData.OutputFileName;

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
        var hasMissed = !await HasExported(context, connection, streamId, asOfTime, GetTriggerSource(args), args.Schedule);
        context.Log.LogInformation(
            "End checking export entities job '{ExportJob}' for '{StreamId}' using {Schedule} at {InstanceTime}, HasMissed {HasMissed}.",
            typeName,
            args.Message,
            args.Schedule,
            args.InstanceTime,
            hasMissed);
        return hasMissed;
    }

    protected bool TryGetMetadata(IDictionary<string, string> metadata, out ExportedFileMetadata fileMetadata)
    {
        if(metadata != null
                && metadata.TryGetValue(TransformMetadataKey(StreamIdKey), out var fileStreamIdString)
                && metadata.TryGetValue(TransformMetadataKey(DataTimeKey), out var fileDataTimeString)
                && Guid.TryParse(fileStreamIdString, out var fileStreamId)
                && DateTimeOffset.TryParse(fileDataTimeString, out var fileDataTime))
        {
            fileMetadata = new ExportedFileMetadata(fileStreamId, fileDataTime);
            return true;
        }

        fileMetadata = null;
        return false;
    }

    protected virtual string TransformMetadataKey(string key)
    {
        return key;
    }

    private bool HasExportedFileBefore(Guid streamId, DateTimeOffset asOfTime, IDictionary<string, string> metadata)
    {
        return TryGetMetadata(metadata, out var fileMetadata)
                && fileMetadata.StreamId == streamId
                && fileMetadata.DataTime == asOfTime;
    }

    protected virtual Task<string> GetOutputFileNameAsync(
        ExecutionContext context,
        ExportJobDataBase exportJobDataBase,
        bool isInitialExport,
        LastExportedFile? lastExportedFile,
        DirectoryPath outputDirectoryPath)
    {
        var configuration = exportJobDataBase.StorageConfiguration;
        var streamId = exportJobDataBase.StreamId;
        var containerName = exportJobDataBase.ContainerName;
        var asOfTime = exportJobDataBase.AsOfTime;
        var outputFormat = exportJobDataBase.OutputFormat;

        if (HasCustomFileNamePattern(configuration))
        {
            return PatternHelper.ReplaceNameUsingPatternAsync(context, configuration.FileNamePattern, streamId, containerName, asOfTime, outputFormat);
        }

        return GetDefaultOutputFileNameAsync(context, exportJobDataBase);
    }

    private static bool HasCustomFileNamePattern(IStorageConfiguration configuration)
    {
        return !string.IsNullOrWhiteSpace(configuration.FileNamePattern);
    }

    protected virtual Task<string> GetDefaultOutputFileNameAsync(
        ExecutionContext context,
        ExportJobDataBase exportJobDataBase)
    {
        var fileExtension = GetFileExtension(exportJobDataBase.OutputFormat);
        var streamIdFormatted = exportJobDataBase.StreamId.ToString(StreamIdDefaultStringFormat);

        return Task.FromResult($"{streamIdFormatted}_{exportJobDataBase.AsOfTime:yyyyMMddHHmmss}.{fileExtension}");
    }

    protected virtual string GetFileExtension(string outputFormat)
    {
        return outputFormat;
    }

    private DateTimeOffset GetAsOfTime(ExecutionContext context, IStorageJobArgs args, IStorageConfiguration configuration)
    {
        if (configuration.UseCurrentTimeForExport || args.Schedule == CronSchedules.NeverCron)
        {
            context.Log.LogDebug("Using current time for export.");
            return _dateTimeOffsetProvider.GetCurrentUtcTime();
        }

        return args.InstanceTime;
    }

    protected virtual ISqlDataWriter GetSqlDataWriter(string outputFormat)
    {
        var format = outputFormat.Trim();
        if (format.Equals(StorageConfigurationConstants.OutputFormats.Csv, StringComparison.OrdinalIgnoreCase))
        {
            return new CsvSqlDataWriter();
        }
        else if (format.Equals(StorageConfigurationConstants.OutputFormats.Json, StringComparison.OrdinalIgnoreCase))
        {
            return new JsonSqlDataWriter();
        }
        else if (format.Equals(StorageConfigurationConstants.OutputFormats.Parquet, StringComparison.OrdinalIgnoreCase))
        {
            return new ParquetSqlDataWriter();
        }

        throw new NotSupportedException($"Format '{outputFormat}' is not supported.");
    }

    private static async Task InsertHistory(ExecutionContext context, SqlConnection connection, ExportHistory exportHistory)
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
                            FileFormat,
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
                            @FileFormat,
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
            command.Parameters.Add(new SqlParameter($"@FileFormat", exportHistory.FileFormat));
            command.Parameters.Add(new SqlParameter($"@StartTime", exportHistory.StartTime));
            command.Parameters.Add(new SqlParameter($"@EndTime", (object)exportHistory.EndTime ?? DBNull.Value));
            command.Parameters.Add(new SqlParameter($"@TotalRows", (object)exportHistory.TotalRows ?? DBNull.Value));
            command.Parameters.Add(new SqlParameter($"@Status", exportHistory.Status));
            command.Parameters.Add(new SqlParameter($"@ExporterHostName", exportHistory.ExporterHostName));

            var rowsAffected = await command.ExecuteNonQueryAsync();
            if (rowsAffected != 1)
            {
                throw new ApplicationException($"Rows affected for insertion of is not 1, it is {rowsAffected}.");
            }
        }
    }

    private protected virtual async Task<ExportHistory> GetLastSuccessfulExportHistory(
        ExecutionContext context,
        SqlConnection connection,
        Guid streamId)
    {
        var tableName = GetExportHistoryTableName(streamId);

        try
        {
            var getSql = $"""
                    SELECT TOP 1
                        StreamId,
                        DataTime,
                        TriggerSource,
                        CronSchedule,
                        FilePath,
                        FileFormat,
                        StartTime,
                        EndTime,
                        TotalRows,
                        Status,
                        ExporterHostName
                    FROM
                        [{tableName}]
                    WHERE StreamId = @StreamId
                        AND Status = '{CompleteStatus}'
                    ORDER BY StreamId, DataTime DESC
                    """;
            var command = new SqlCommand(getSql, connection)
            {
                CommandType = CommandType.Text
            };

            command.Parameters.Add(new SqlParameter($"@StreamId", streamId));

            await using var reader = await command.ExecuteReaderAsync();
            var exportHistoryList = new List<ExportHistory>();
            while (await reader.ReadAsync())
            {
                var dataTime = (DateTimeOffset)GetValue("DataTime", reader);
                var triggerSource = (string)GetValue("TriggerSource", reader);
                var cronSchedule = (string)GetValue("CronSchedule", reader);
                var filePath = (string)GetValue("FilePath", reader);
                var fileFormat = (string)GetValue("FileFormat", reader);
                var startTime = (DateTimeOffset)GetValue("StartTime", reader);
                var endTime = (DateTimeOffset?)GetValue("EndTime", reader);
                var totalRows = (int?)GetValue("TotalRows", reader);
                var status = (string)GetValue("status", reader);
                var exporterHostName = (string)GetValue("exporterHostName", reader);
                var history = new ExportHistory(streamId, dataTime, triggerSource, cronSchedule, filePath, fileFormat, startTime, endTime, totalRows, status, exporterHostName);
                exportHistoryList.Add(history);
            }

            return exportHistoryList.SingleOrDefault();
        }
        catch (SqlException writeDataException) when (writeDataException.IsTableNotFoundException())
        {
            return null;
        }

        object GetValue(string key, SqlDataReader reader)
        {
            var value = reader.GetValue(key);

            if (value == DBNull.Value)
            {
                return null;
            }

            return value;
        }
    }

    private static async Task UpdateHistory(ExecutionContext context, SqlConnection connection, ExportHistory exportHistory)
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
                        AND CronSchedule = @CronSchedule
                    """;
        var command = new SqlCommand(insertSql, connection)
        {
            CommandType = CommandType.Text
        };

        command.Parameters.Add(new SqlParameter($"@StreamId", exportHistory.StreamId));
        command.Parameters.Add(new SqlParameter($"@DataTime", exportHistory.DataTime));
        command.Parameters.Add(new SqlParameter($"@TriggerSource", exportHistory.TriggerSource));
        command.Parameters.Add(new SqlParameter($"@CronSchedule", exportHistory.CronSchedule));
        command.Parameters.Add(new SqlParameter($"@EndTime", exportHistory.EndTime));
        command.Parameters.Add(new SqlParameter($"@TotalRows", (object)exportHistory.TotalRows ?? DBNull.Value));
        command.Parameters.Add(new SqlParameter($"@Status", exportHistory.Status));

        var rowsAffected = await command.ExecuteNonQueryAsync();
        if (rowsAffected != 1)
        {
            throw new ApplicationException($"Rows affected for update of is not 1, it is {rowsAffected}.");
        }
    }

    private static async Task<bool> HasExported(
        ExecutionContext context,
        SqlConnection connection,
        Guid streamId,
        DateTimeOffset dataTime,
        string triggerSource,
        string cronSchedule)
    {
        try
        {
            return await hasExported(connection, streamId, dataTime, triggerSource, cronSchedule);
        }
        catch (SqlException writeDataException) when (writeDataException.IsTableNotFoundException())
        {
            var tableName = GetExportHistoryTableName(streamId);
            context.Log.LogDebug("Table {TableName} does not exist. Returning has exported false.", tableName);
            return false;
        }

        static async Task<bool> hasExported(SqlConnection connection, Guid streamId, DateTimeOffset dataTime, string triggerSource, string cronSchedule)
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
                            AND CronSchedule = @CronSchedule
                            AND (Status = '{CompleteStatus}' OR Status = '{SkippedStatus}')
                        """;
            var command = new SqlCommand(insertSql, connection)
            {
                CommandType = CommandType.Text
            };
            command.Parameters.Add(new SqlParameter($"@StreamId", streamId));
            command.Parameters.Add(new SqlParameter($"@DataTime", dataTime));
            command.Parameters.Add(new SqlParameter($"@TriggerSource", triggerSource));
            command.Parameters.Add(new SqlParameter($"@CronSchedule", cronSchedule));

            var count = (int)await command.ExecuteScalarAsync();
            return count > 0;
        }
    }

    private static async Task EnsureHistoryTableExists(SqlConnection connection, Guid streamId)
    {
        var tableName = GetExportHistoryTableName(streamId);
        var createTableSql = $"""
                IF NOT EXISTS (SELECT * FROM SYSOBJECTS WHERE NAME='{tableName}' AND XTYPE='U')
                CREATE TABLE [{tableName}] (
                    StreamId UNIQUEIDENTIFIER NOT NULL,
                    DataTime DATETIMEOFFSET NOT NULL,
                    TriggerSource NVARCHAR(255) NOT NULL,
                    CronSchedule NVARCHAR(255) NOT NULL,
                    FilePath NVARCHAR(255) NOT NULL,
                    FileFormat NVARCHAR(255) NOT NULL,
                    StartTime DATETIMEOFFSET NOT NULL,
                    EndTime DATETIMEOFFSET NULL,
                    TotalRows INT NULL,
                    Status NVARCHAR(255) NULL,
                    ExporterHostName NVARCHAR(255) NULL,
                    CONSTRAINT [PK_{tableName}] PRIMARY KEY CLUSTERED (StreamId,DataTime,TriggerSource,CronSchedule)
                );
                """;
        var command = new SqlCommand(createTableSql, connection)
        {
            CommandType = CommandType.Text
        };
        _ = await command.ExecuteNonQueryAsync();
    }

    internal static string GetExportHistoryTableName(Guid streamId)
    {
        return CacheTableHelper.GetExportHistoryTableName(streamId) + "_ExportHistory";
    }

    protected record ExportedFileMetadata(Guid StreamId, DateTimeOffset DataTime);

    internal record ExportResult(FilePath? FilePath, bool HasExported, string? Reason)
    {
        public static ExportResult CreateSkipped(string reason) => new (null, false, reason);

        public static ExportResult CreateSuccess(FilePath outputFilePath) => new(outputFilePath, true, null);
    }

    internal record ShouldSkipResult(bool ShouldSkip, string? Reason);
}
