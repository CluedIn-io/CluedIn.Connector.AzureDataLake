using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text.RegularExpressions;
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
    private readonly IDateTimeOffsetProvider _dateTimeOffsetProvider;
    private static readonly TimeSpan exportTimeout = TimeSpan.MaxValue;
    private const int ExportEntitiesLockInMilliseconds = 100;
    private const string StreamIdKey = "StreamId";
    private const string DataTimeKey = "DataTime";

    protected DataLakeExportEntitiesJobBase(
        ApplicationContext appContext,
        IStreamRepository streamRepository,
        IDataLakeClient dataLakeClient,
        IDataLakeConstants dataLakeConstants,
        IDataLakeJobDataFactory dataLakeJobDataFactory,
        IDateTimeOffsetProvider dateTimeOffsetProvider) : base(appContext)
    {
        _streamRepository = streamRepository ?? throw new ArgumentNullException(nameof(streamRepository));
        _dataLakeClient = dataLakeClient ?? throw new ArgumentNullException(nameof(dataLakeClient));
        _dataLakeConstants = dataLakeConstants ?? throw new ArgumentNullException(nameof(dataLakeConstants));
        _dataLakeJobDataFactory = dataLakeJobDataFactory ?? throw new ArgumentNullException(nameof(dataLakeJobDataFactory));
        _dateTimeOffsetProvider = dateTimeOffsetProvider ?? throw new ArgumentNullException(nameof(dateTimeOffsetProvider));
    }

    public override async Task DoRunAsync(ExecutionContext context, DataLakeJobArgs args)
    {
        var typeName = this.GetType().Name;
        using var exportJobLoggingScope = context.Log.BeginScope(new Dictionary<string, object>
        {
            [StreamIdKey] = args.Message,
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
        var outputFileName = GetOutputFileName(configuration, streamId, containerName, asOfTime, outputFormat);
        var filePathProperties = await _dataLakeClient.GetFilePathProperties(configuration, outputFileName);

        if (filePathProperties != null)
        {
            context.Log.LogDebug("Output file '{OutputFileName}' exists using data at {DataTime}. Switching to using current time", outputFileName, asOfTime);
            asOfTime = DateTime.UtcNow;
            outputFileName = GetOutputFileName(configuration, streamId, containerName, asOfTime, outputFormat);
            
            if (args.IsTriggeredFromJobServer)
            {
                context.Log.LogInformation(
                    "Output file '{OutputFileName}' exists using data at {DataTime} and job is triggered from job server. Switching to using current time.",
                    outputFileName,
                    asOfTime);
                asOfTime = _dateTimeOffsetProvider.GetCurrentUtcTime().DateTime;
                outputFileName = GetOutputFileName(streamId, asOfTime, outputFormat);
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
            ["StartTime"] = _dateTimeOffsetProvider.GetCurrentUtcTime(),
            [DataTimeKey] = asOfTime,
        });


        context.Log.LogInformation("Begin writing to file '{OutputFileName}' using data at {DataTime}.", outputFileName, asOfTime);
        var directoryClient = await _dataLakeClient.EnsureDataLakeDirectoryExist(configuration);
        var dataLakeFileClient = directoryClient.GetFileClient(outputFileName);
        await writeFileContentsAsync();
        await setFilePropertiesAsync();
        context.Log.LogInformation("End writing to file '{OutputFileName}' using data at {DataTime}.", outputFileName, asOfTime);

        context.Log.LogInformation(
            "End export entities job '{ExportJob}' for '{StreamId}' using {Schedule}.",
            typeName,
            args.Message,
            args.Schedule);


        async Task writeFileContentsAsync()
        {
            var sqlDataWriter = GetSqlDataWriter(outputFormat);
            await using var outputStream = await dataLakeFileClient.OpenWriteAsync(true);
            using var bufferedStream = new DataLakeBufferedWriteStream(outputStream);
            await sqlDataWriter?.WriteAsync(context, configuration, bufferedStream, fieldNames, reader);
        }

        async Task setFilePropertiesAsync()
        {
            context.Log.LogDebug(
                "Setting file properties to file '{OutputFileName}' StreamId {StreamId} and DataTime {DataTime}.",
                outputFileName,
                streamId,
                asOfTime);
            await dataLakeFileClient.SetMetadataAsync(
                new Dictionary<string, string>
                {
                    [StreamIdKey] = streamId.ToString(),
                    [DataTimeKey] = asOfTime.ToString("O"),
                });
        }
    }

    private static bool HasExportedFileBefore(Guid streamId, DateTime asOfTime, IDictionary<string, string> metadata)
    {
        return metadata != null
                && metadata.TryGetValue(StreamIdKey, out var fileStreamIdString)
                && metadata.TryGetValue(DataTimeKey, out var fileDataTimeString)
                && Guid.TryParse(fileStreamIdString, out var fileStreamId)
                && DateTimeOffset.TryParse(fileDataTimeString, out var fileDataTime)
                && fileStreamId == streamId
                && fileDataTime == asOfTime;
    }

    protected virtual string GetOutputFileName(IDataLakeJobData configuration, Guid streamId, string containerName, DateTime asOfTime, string outputFormat)
    {
        if (!string.IsNullOrWhiteSpace(configuration.FileNamePattern))
        {
            return GetOutputFileNameUsingPattern(configuration.FileNamePattern, streamId, containerName, asOfTime, outputFormat);
        }

        return GetDefaultOutputFileName(streamId, asOfTime, outputFormat);
    }

    protected virtual string GetDefaultOutputFileName(Guid streamId, DateTime asOfTime, string outputFormat)
    {
        var fileExtension = GetFileExtension(outputFormat);
        var outputFileName = $"{streamId}_{asOfTime:yyyyMMddHHmmss}.{fileExtension}";
        return outputFileName;
    }

    private static string GetOutputFileNameUsingPattern(string outputFileNamePattern, Guid streamId, string containerName, DateTime asOfTime, string outputFormat)
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
                ? match.Groups[2].Captures.Single().Value.Substring(1)
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

    private DateTime GetLastOccurence(ExecutionContext context, JobArgs args, IDataLakeJobData jobData)
    {
        if (jobData.UseCurrentTimeForExport
            || args.Schedule == DataLakeConstants.CronSchedules[DataLakeConstants.JobScheduleNames.Never])
        {
            context.Log.LogDebug("Using current time for export.");
            return _dateTimeOffsetProvider.GetCurrentUtcTime().DateTime;
        }

        var cronSchedule = NCrontab.CrontabSchedule.Parse(args.Schedule);
        var next = cronSchedule.GetNextOccurrence(_dateTimeOffsetProvider.GetCurrentUtcTime().DateTime.AddMinutes(1));
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
