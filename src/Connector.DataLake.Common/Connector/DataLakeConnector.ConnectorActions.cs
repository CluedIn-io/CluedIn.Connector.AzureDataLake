using System;
using System.Threading.Tasks;

using CluedIn.Core.Connectors.ExtendedOperations;
using CluedIn.Core.Jobs;
using CluedIn.Core.Streams;
using CluedIn.Core.Streams.Models;

using Microsoft.Extensions.Logging;

using ExecutionContext = CluedIn.Core.ExecutionContext;

namespace CluedIn.Connector.DataLake.Common.Connector;

public abstract partial class DataLakeConnector : ICustomActionConnector
{
    private const string RunExportActionName = "RunExport";

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
        var failedResult = new ExtendedOperationResultEntry("Result", ExtendedOperationResultEntryType.String, $"Failed: {ex.Message}", "Connector", string.Empty);
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

    protected abstract Type ExportJobType { get; }
}
