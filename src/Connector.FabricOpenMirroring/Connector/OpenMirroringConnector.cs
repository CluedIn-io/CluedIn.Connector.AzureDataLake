using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using Azure.Identity;
using Azure;

using CluedIn.Connector.DataLake.Common;
using CluedIn.Connector.DataLake.Common.Connector;
using CluedIn.Core;
using CluedIn.Core.Connectors;
using CluedIn.Core.Streams.Models;

using Microsoft.Extensions.Logging;


namespace CluedIn.Connector.FabricOpenMirroring.Connector;

public class OpenMirroringConnector : DataLakeConnector
{
    internal const string InvalidCredentialsErrorMessage = "Authentication failed due to invalid credentials.";
    internal const string InvalidWorkspaceErrorMessage = "Workspace name cannot be empty.";
    internal const string WorkspaceNotFoundErrorMessageFormat = "Workspace '{0}' is not found.";
    internal const string WorkspaceNotFoundErrorCode = "WorkspaceNotFound";

    private readonly ILogger<OpenMirroringConnector> _logger;
    private readonly OpenMirroringClient _client;
    private readonly IDateTimeOffsetProvider _dateTimeOffsetProvider;

    public OpenMirroringConnector(
        ILogger<OpenMirroringConnector> logger,
        OpenMirroringClient client,
        IOpenMirroringConstants constants,
        OpenMirroringJobDataFactory dataLakeJobDataFactory,
        IDateTimeOffsetProvider dateTimeOffsetProvider)
        : base(logger, client, constants, dataLakeJobDataFactory, dateTimeOffsetProvider)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _client = client ?? throw new ArgumentNullException(nameof(client));
        _dateTimeOffsetProvider = dateTimeOffsetProvider;
    }

    protected override async Task<ConnectionVerificationResult> VerifyDataLakeConnection(IDataLakeJobData jobData)
    {
        // There are three places where verification can be called
        // 1. Health check
        // 2. Export Target creation form
        // 3. Export Target details/update form
        // When ShouldCreateMirroredDatabase is set to true AND mirrored database is set, ideally we should NOT verify the connection during creation
        // because it certainly wouldn't have existed
        // But during update/edit, we should verify it. However, there is no way to distinguish these two cases
        if (jobData is not OpenMirroringConnectorJobData casted)
        {
            throw new ArgumentException($"Invalid job data type: {jobData.GetType().Name}. Expected: {nameof(OpenMirroringConnectorJobData)}.");
        }

        if (string.IsNullOrWhiteSpace(casted.WorkspaceName))
        {
            return CreateFailedConnectionVerification(InvalidWorkspaceErrorMessage);
        }

        var isHealthCheckVerification = IsHealthCheckVerification(casted);
        var shouldTolerateMissingDirectory = !isHealthCheckVerification && casted.ShouldCreateMirroredDatabase;
        if (shouldTolerateMissingDirectory)
        {
            if (await _client.HasValidWorkspaceAsync(jobData))
            {
                return SuccessfulConnectionVerification;
            }

            return CreateFailedConnectionVerification(InvalidWorkspaceErrorMessage);
        }

        try
        {
            if (await Client.DirectoryExists(jobData))
            {
                return SuccessfulConnectionVerification;
            }

            return CreateFailedConnectionVerification($"Directory '{jobData.RootDirectoryPath}' is not found");
        }
        catch (AuthenticationFailedException ex)
        {
            _logger.LogWarning(ex, InvalidCredentialsErrorMessage);
            return CreateFailedConnectionVerification(InvalidCredentialsErrorMessage);
        }
        catch (RequestFailedException ex) when (WorkspaceNotFoundErrorCode.Equals(ex.ErrorCode))
        {
            var errorMessage = WorkspaceNotFoundErrorMessageFormat.FormatWith(casted.WorkspaceName);
            _logger.LogWarning(ex, WorkspaceNotFoundErrorMessageFormat, casted.WorkspaceName);
            return CreateFailedConnectionVerification(errorMessage);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to check if directory exists.");
            return CreateFailedConnectionVerification(ex.Message);
        }

        static bool IsHealthCheckVerification(OpenMirroringConnectorJobData castedJobData)
        {
            return castedJobData.Configurations.TryGetValue(DataLakeConstants.ProviderDefinitionIdKey, out _);
        }
    }

    public override async Task ArchiveContainer(ExecutionContext executionContext, IReadOnlyStreamModel streamModel)
    {
        var providerDefinitionId = streamModel.ConnectorProviderDefinitionId!.Value;
        var containerName = streamModel.ContainerName;

        var jobData = await DataLakeJobDataFactory.GetConfiguration(executionContext, providerDefinitionId, containerName);
        var subDirectory = await OutputDirectoryHelper.GetSubDirectory(executionContext, jobData, streamModel.Id, containerName, _dateTimeOffsetProvider.GetCurrentUtcTime(), jobData.OutputFormat);
        await Client.DeleteDirectory(jobData, subDirectory);
        await base.ArchiveContainer(executionContext, streamModel);
    }

    public override IReadOnlyCollection<StreamMode> GetSupportedModes()
    {
        return new[] { StreamMode.Sync };
    }

    protected override Type ExportJobType => typeof(OpenMirroringExportEntitiesJob);
}
