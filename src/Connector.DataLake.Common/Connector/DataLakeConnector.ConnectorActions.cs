using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

using CluedIn.Core.Connectors.ExtendedOperations;
using CluedIn.Core.Jobs;
using CluedIn.Core.Streams;
using CluedIn.Core.Streams.Models;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

using ExecutionContext = CluedIn.Core.ExecutionContext;

namespace CluedIn.Connector.DataLake.Common.Connector;

public abstract partial class DataLakeConnector : ICustomActionConnector
{
    private const string RunExportActionName = "RunExport";
    private const string GetStreamCacheRowCountActionName = "GetStreamCacheRowCount";
    private const string GetEntityActionName = "GetEntity";
    private const string GetExportHistoryActionName = "GetExportHistory";

    public virtual async Task<GetConnectorActionsResult> GetActions(
        ExecutionContext executionContext,
        IReadOnlyStreamModel streamModel)
    {
        var containerName = streamModel.ContainerName;
        var providerDefinitionId = streamModel.ConnectorProviderDefinitionId!.Value;
        var configuration = await _dataLakeJobDataFactory.GetConfiguration(executionContext, providerDefinitionId, containerName);
        var action = new ConnectorAction(RunExportActionName, "Run Export", "Run export now", [], []);
        var streamRepository = executionContext.ApplicationContext.Container.Resolve<IStreamRepository>();

        var stream = await streamRepository.GetStream(streamModel.Id);
        var shouldShowAction = configuration.IsStreamCacheEnabled // only for streams with cache enabled
            && stream.Mode == StreamMode.Sync // only for sync streams
            && (stream.Status == StreamStatus.Started || stream.Status == StreamStatus.Paused); // only for started or paused streams
        return new GetConnectorActionsResult(streamModel.Id, shouldShowAction ? [action] : []);
    }

    public virtual async Task<ExecuteConnectorActionResult> ExecuteAction(
        ExecutionContext executionContext,
        IReadOnlyStreamModel streamModel,
        ExecuteConnectorActionRequest request)
    {
        try
        {
            if (request.ActionName == RunExportActionName)
            {
                return await ExportNow(executionContext, streamModel, request);
            }

            if (request.ActionName == GetStreamCacheRowCountActionName)
            {
                return await GetStreamCacheRowCount(executionContext, streamModel, request);
            }

            if (request.ActionName == GetEntityActionName)
            {
                return await GetEntity(executionContext, streamModel, request);
            }

            if (request.ActionName == GetExportHistoryActionName)
            {
                return await GetExportHistory(executionContext, streamModel, request);
            }

            var now = _dateTimeOffsetProvider.GetCurrentUtcTime();
            var notFoundResult = new ExtendedOperationResultEntry("Result", ExtendedOperationResultEntryType.String, "Not Found", "Connector", string.Empty);
            return new ExecuteConnectorActionResult(streamModel.Id, request.ActionName, false, false, now, null, [notFoundResult]);
        }
        catch (Exception ex)
        {
            return GetFailedResult(streamModel, request, ex);
        }
    }

    private ExecuteConnectorActionResult GetFailedResult(
        IReadOnlyStreamModel streamModel,
        ExecuteConnectorActionRequest request,
        Exception ex)
    {
        _logger.LogError(ex, "Failed to execute action {ActionName} for stream {StreamId}", request.ActionName, streamModel.Id);
        var now = _dateTimeOffsetProvider.GetCurrentUtcTime();
        var failedResult = new ExtendedOperationResultEntry("Result", ExtendedOperationResultEntryType.String, $"Failed: {ex.Message}{Environment.NewLine}{ex.StackTrace}", "Connector", string.Empty);
        return new ExecuteConnectorActionResult(streamModel.Id, request.ActionName, IsSuccessful: false, IsCompleted: true, now, now, [failedResult]);
    }

    private async Task<ExecuteConnectorActionResult> ExportNow(
        ExecutionContext executionContext,
        IReadOnlyStreamModel streamModel,
        ExecuteConnectorActionRequest request)
    {
        var startedAt = _dateTimeOffsetProvider.GetCurrentUtcTime();
        var jobArgs = new JobArgs()
        {
            OrganizationId = executionContext.Organization.Id.ToString(),
            Schedule = CronSchedules.NeverCron,
            Message = streamModel.Id.ToString(),
        };

        var exportJob = executionContext.ApplicationContext.Container.Resolve(ExportJobType) as DataLakeExportEntitiesJobBase;
        if (exportJob == null)
        {
            var failedResult = new ExtendedOperationResultEntry("Result", ExtendedOperationResultEntryType.String, "Failed to resolve export job", "Connector", string.Empty);
            return new ExecuteConnectorActionResult(streamModel.Id, request.ActionName, IsSuccessful: false, IsCompleted: true, startedAt, startedAt, [failedResult]);
        }

        await exportJob.DoRunAsync(executionContext, new DataLakeJobArgs(jobArgs, isTriggeredFromJobServer: true, startedAt));

        var successResult = new ExtendedOperationResultEntry("Result", ExtendedOperationResultEntryType.String, "Success", "Connector", string.Empty);
        var now = _dateTimeOffsetProvider.GetCurrentUtcTime();
        return new ExecuteConnectorActionResult(streamModel.Id, request.ActionName, IsSuccessful: true, IsCompleted: true, startedAt, now, [successResult]);
    }

    private async Task<ExecuteConnectorActionResult> GetStreamCacheRowCount(
        ExecutionContext executionContext,
        IReadOnlyStreamModel streamModel,
        ExecuteConnectorActionRequest request)
    {
        var startedAt = _dateTimeOffsetProvider.GetCurrentUtcTime();
        var containerName = streamModel.ContainerName;
        var providerDefinitionId = streamModel.ConnectorProviderDefinitionId!.Value;
        var configuration = await _dataLakeJobDataFactory.GetConfiguration(executionContext, providerDefinitionId, containerName);
        await using var connection = new SqlConnection(configuration.StreamCacheConnectionString);
        await connection.OpenAsync();
        var streamId = streamModel.Id;
        var tableName = CacheTableHelper.GetCacheTableName(streamId);
        var (asOfTime, isCurrentTime) = GetDataTime(request);

        var getDataSql = $"SELECT COUNT(*) FROM [{tableName}] FOR SYSTEM_TIME AS OF @AsOfTime";
        var command = new SqlCommand(getDataSql, connection)
        {
            CommandType = CommandType.Text
        };
        command.Parameters.Add(new SqlParameter("@AsOfTime", asOfTime));
        var result = await command.ExecuteScalarAsync();
        var total = Convert.ToInt32(result);

        var successResult = new ExtendedOperationResultEntry(
            "Total",
            ExtendedOperationResultEntryType.Json,
            JsonConvert.SerializeObject(new
            {
                Total = total,
                AsOfTime = asOfTime,
                IsCurrentTime = isCurrentTime,
                TableName = tableName,
                StreamId = streamId,
                ProviderDefinitionId = providerDefinitionId,
            }),
            "Connector",
            string.Empty);
        var now = _dateTimeOffsetProvider.GetCurrentUtcTime();
        return new ExecuteConnectorActionResult(streamModel.Id, request.ActionName, IsSuccessful: true, IsCompleted: true, startedAt, now, [successResult]);
    }

    private (DateTimeOffset DataTime, bool IsCurrentTime) GetDataTime(ExecuteConnectorActionRequest request)
    {
        if (request.Parameters?.TryGetValue("DataTime", out var asOfTimeObj) == true)
        {
            if (asOfTimeObj is DateTimeOffset dateTimeOffset)
            {
                return (dateTimeOffset, false);
            }

            if (asOfTimeObj is string asOfTimeString
                && DateTimeOffset.TryParse(asOfTimeString, out var parsedAsOfTime))
            {
                return (parsedAsOfTime, false);
            }
        }

        return (_dateTimeOffsetProvider.GetCurrentUtcTime(), true);
    }

    private async Task<ExecuteConnectorActionResult> GetEntity(
        ExecutionContext executionContext,
        IReadOnlyStreamModel streamModel,
        ExecuteConnectorActionRequest request)
    {
        var startedAt = _dateTimeOffsetProvider.GetCurrentUtcTime();
        var containerName = streamModel.ContainerName;
        var providerDefinitionId = streamModel.ConnectorProviderDefinitionId!.Value;
        var configuration = await _dataLakeJobDataFactory.GetConfiguration(executionContext, providerDefinitionId, containerName);
        await using var connection = new SqlConnection(configuration.StreamCacheConnectionString);
        await connection.OpenAsync();
        var streamId = streamModel.Id;
        var tableName = CacheTableHelper.GetCacheTableName(streamId);
        var (asOfTime, isCurrentTime) = GetDataTime(request);

        if (request.Parameters?.TryGetValue("EntityId", out var entityIdObj) != true
            || entityIdObj == null
            || !Guid.TryParse(entityIdObj.ToString(), out var entityId))
        {
            var failedResult = new ExtendedOperationResultEntry("Result", ExtendedOperationResultEntryType.String, "EntityId parameter is required and must be a valid GUID", "Connector", string.Empty);
            return new ExecuteConnectorActionResult(streamModel.Id, request.ActionName, IsSuccessful: false, IsCompleted: true, startedAt, _dateTimeOffsetProvider.GetCurrentUtcTime(), [failedResult]);
        }

        var getDataSql = $"SELECT * FROM [{tableName}] FOR SYSTEM_TIME AS OF @AsOfTime WHERE {DataLakeConstants.IdKey} = @{DataLakeConstants.IdKey}";
        var command = new SqlCommand(getDataSql, connection)
        {
            CommandType = CommandType.Text
        };
        command.Parameters.Add(new SqlParameter("@AsOfTime", asOfTime));
        command.Parameters.Add(new SqlParameter($"@{DataLakeConstants.IdKey}", entityId));
        await using var reader = await command.ExecuteReaderAsync();

        var entityData = await reader.ReadAsync()
            ? Enumerable.Range(0, reader.FieldCount).ToDictionary(reader.GetName, reader.GetValue)
            : [];

        var successResult = new ExtendedOperationResultEntry(
            "Entity",
            ExtendedOperationResultEntryType.Json,
            JsonConvert.SerializeObject(new
            {
                Entity = entityData,
                AsOfTime = asOfTime,
                IsCurrentTime = isCurrentTime,
                TableName = tableName,
                StreamId = streamId,
                ProviderDefinitionId = providerDefinitionId,
            }),
            "Connector",
            string.Empty);
        var now = _dateTimeOffsetProvider.GetCurrentUtcTime();
        return new ExecuteConnectorActionResult(streamModel.Id, request.ActionName, IsSuccessful: true, IsCompleted: true, startedAt, now, [successResult]);
    }

    private async Task<ExecuteConnectorActionResult> GetExportHistory(
        ExecutionContext executionContext,
        IReadOnlyStreamModel streamModel,
        ExecuteConnectorActionRequest request)
    {
        var startedAt = _dateTimeOffsetProvider.GetCurrentUtcTime();
        var containerName = streamModel.ContainerName;
        var providerDefinitionId = streamModel.ConnectorProviderDefinitionId!.Value;
        var configuration = await _dataLakeJobDataFactory.GetConfiguration(executionContext, providerDefinitionId, containerName);
        await using var connection = new SqlConnection(configuration.StreamCacheConnectionString);
        await connection.OpenAsync();
        var streamId = streamModel.Id;
        var tableName = CacheTableHelper.GetExportHistoryTableName(streamId);
        var asOfTime = _dateTimeOffsetProvider.GetCurrentUtcTime();

        var getDataSql = $"SELECT TOP (1000) * FROM [{tableName}] ORDER BY StartTime DESC";
        var command = new SqlCommand(getDataSql, connection)
        {
            CommandType = CommandType.Text
        };
        await using var reader = await command.ExecuteReaderAsync();
        var result = new List<Dictionary<string, object>>();
        while (await reader.ReadAsync())
        {
            var entityData = Enumerable.Range(0, reader.FieldCount)
                .ToDictionary(reader.GetName, reader.GetValue);
            result.Add(entityData);
        }
        var successResult = new ExtendedOperationResultEntry(
        "ExportHistory",
        ExtendedOperationResultEntryType.Json,
        JsonConvert.SerializeObject(new
        {
            History = result,
            AsOfTime = asOfTime,
            TableName = tableName,
            StreamId = streamId,
            ProviderDefinitionId = providerDefinitionId,
        }),
        "Connector",
        string.Empty);
        var now = _dateTimeOffsetProvider.GetCurrentUtcTime();
        return new ExecuteConnectorActionResult(streamModel.Id, request.ActionName, IsSuccessful: true, IsCompleted: true, startedAt, now, [successResult]);
    }

    protected abstract Type ExportJobType { get; }
}
