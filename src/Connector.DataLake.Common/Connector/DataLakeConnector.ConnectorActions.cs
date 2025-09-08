using System;
using System.Threading.Tasks;

using CluedIn.Core.Connectors.ExtendedOperations;
using CluedIn.Core.Jobs;
using CluedIn.Core.Streams;
using CluedIn.Core.Streams.Models;

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
        var shouldShowAction = configuration.IsStreamCacheEnabled && stream.Mode == StreamMode.Sync && stream.Status != StreamStatus.Started;
        return new GetConnectorActionsResult(streamModel.Id, shouldShowAction ? [action] : []);
    }

    public virtual async Task<ExecuteConnectorActionResult> ExecuteAction(
        ExecutionContext executionContext,
        IReadOnlyStreamModel streamModel,
        ExecuteConnectorActionRequest request)
    {
        var startedAt = _dateTimeOffsetProvider.GetCurrentTime();
        if (request.ActionName == RunExportActionName)
        {

            var now = _dateTimeOffsetProvider.GetCurrentUtcTime();
            var jobArgs = new JobArgs()
            {
                OrganizationId = executionContext.Organization.Id.ToString(),
                Schedule = CronSchedules.NeverCron,
                Message = streamModel.Id.ToString(),
            };

            var exportJob = executionContext.ApplicationContext.Container.Resolve(ExportJobType) as DataLakeExportEntitiesJobBase;
            await exportJob.DoRunAsync(executionContext, new DataLakeJobArgs(jobArgs, isTriggeredFromJobServer: true, now));

            var successResult = new ExtendedOperationResultEntry("Result", ExtendedOperationResultEntryType.String, "Success", "Connector", string.Empty);
            return new ExecuteConnectorActionResult(streamModel?.Id ?? Guid.Empty, request.ActionName, true, false, startedAt, startedAt, [successResult]);
        }

        var notFoundResult = new ExtendedOperationResultEntry("Result", ExtendedOperationResultEntryType.String, "Not Found", "Connector", string.Empty);
        return new ExecuteConnectorActionResult(streamModel?.Id ?? Guid.Empty, request.ActionName, false, false, startedAt, null, [notFoundResult]);
    }

    protected abstract Type ExportJobType { get; }
}
