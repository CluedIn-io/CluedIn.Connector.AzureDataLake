using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using Azure;
using Azure.Identity;

using CluedIn.Connector.FileStorage.Common;
using CluedIn.Connector.FileStorage.Common.Connector;
using CluedIn.Core;
using CluedIn.Core.Connectors;
using CluedIn.Core.Streams.Models;

using Microsoft.Extensions.Logging;


namespace CluedIn.Connector.FabricOpenMirroring.Connector;

public class OpenMirroringConnector : StorageConnectorBase
{
    internal const string InvalidCredentialsErrorMessage = "Authentication failed due to invalid credentials.";
    internal const string InvalidWorkspaceErrorMessage = "Workspace name cannot be empty.";
    internal const string WorkspaceNotFoundErrorMessageFormat = "Workspace '{0}' is not found.";
    internal const string WorkspaceNotFoundErrorCode = "WorkspaceNotFound";

    private readonly ILogger<OpenMirroringConnector> _logger;
    private readonly OpenMirroringStorageFactory _storageStorageFactory;
    private readonly IDateTimeOffsetProvider _dateTimeOffsetProvider;

    public OpenMirroringConnector(
        ILogger<OpenMirroringConnector> logger,
        ApplicationContext applicationContext,
        IOpenMirroringConfigurationConstants constants,
        OpenMirroringStorageFactory storageFactory,
        IDateTimeOffsetProvider dateTimeOffsetProvider)
        : base(logger, applicationContext, constants, storageFactory, dateTimeOffsetProvider)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _storageStorageFactory = storageFactory ?? throw new ArgumentNullException(nameof(storageFactory));
        _dateTimeOffsetProvider = dateTimeOffsetProvider ?? throw new ArgumentNullException(nameof(dateTimeOffsetProvider));
    }

    protected override async Task<ConnectionVerificationResult> VerifyDataLakeConnection(ExecutionContext executionContext, IStorageConfiguration configuration)
    {
        // There are three places where verification can be called
        // 1. Health check
        // 2. Export Target creation form
        // 3. Export Target details/update form
        // When ShouldCreateMirroredDatabase is set to true AND mirrored database is set, ideally we should NOT verify the connection during creation
        // because it certainly wouldn't have existed
        // But during update/edit, we should verify it. However, there is no way to distinguish these two cases
        if (configuration is not OpenMirroringConnectorConfiguration casted)
        {
            throw new ArgumentException($"Invalid configuration type: {configuration.GetType().Name}. Expected: {nameof(OpenMirroringConnectorConfiguration)}.");
        }

        if (string.IsNullOrWhiteSpace(casted.WorkspaceName))
        {
            return CreateFailedConnectionVerification(InvalidWorkspaceErrorMessage);
        }

        var isHealthCheckVerification = IsHealthCheckVerification(casted);
        var shouldTolerateMissingDirectory = !isHealthCheckVerification && casted.ShouldCreateMirroredDatabase;

        var client = await _storageStorageFactory.CreateStorageClient(executionContext, casted) as OpenMirroringStorageClient;
        if (shouldTolerateMissingDirectory)
        {
            if (await client.HasValidWorkspaceAsync())
            {
                return SuccessfulConnectionVerification;
            }

            return CreateFailedConnectionVerification(InvalidWorkspaceErrorMessage);
        }

        try
        {
            var basePath = await client.GetBaseDirectoryPathAsync();
            if (await client.DirectoryExistsAsync(basePath))
            {
                return SuccessfulConnectionVerification;
            }

            return CreateFailedConnectionVerification($"Directory '{basePath.Path}' is not found");
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

        static bool IsHealthCheckVerification(OpenMirroringConnectorConfiguration castedJobData)
        {
            return castedJobData.Configurations.TryGetValue(StorageConfigurationConstants.ProviderDefinitionIdKey, out _);
        }
    }

    public override async Task ArchiveContainer(ExecutionContext executionContext, IReadOnlyStreamModel streamModel)
    {
        var providerDefinitionId = streamModel.ConnectorProviderDefinitionId!.Value;
        var containerName = streamModel.ContainerName;

        var configuration = await StorageFactory.CreateStorageConfiguration(executionContext, streamModel);
        var subDirectory = await OutputDirectoryHelper.GetSubDirectory(executionContext, configuration, streamModel.Id, containerName, _dateTimeOffsetProvider.GetCurrentUtcTime(), configuration.OutputFormat);
        var client = await StorageFactory.CreateStorageClient(executionContext, configuration);
        var basePath = await client.GetBaseDirectoryPathAsync();
        await client.DeleteDirectoryAsync(basePath.GetSubDirectoryPath(subDirectory));
        await base.ArchiveContainer(executionContext, streamModel);
    }

    public override IReadOnlyCollection<StreamMode> GetSupportedModes()
    {
        return new[] { StreamMode.Sync };
    }

    protected override Type ExportJobType => typeof(OpenMirroringExportEntitiesJob);
}
