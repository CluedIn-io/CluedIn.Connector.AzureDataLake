using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using Azure;
using Azure.Identity;

using CluedIn.Connector.FileStorage.Common;
using CluedIn.Connector.FileStorage.Common.Connector;
using CluedIn.Core;
using CluedIn.Core.Configuration;
using CluedIn.Core.Data.Relational;
using CluedIn.Core.Streams.Models;

using Microsoft.Extensions.Logging;

using Neo4j.Driver;

namespace CluedIn.Connector.FabricOpenMirroring.Connector;

public class OpenMirroringConnector : StorageConnectorBase
{
    internal const string InvalidCredentialsErrorMessage = "Authentication failed due to invalid credentials.";
    internal const string InvalidWorkspaceErrorMessage = "Workspace name cannot be empty.";
    internal const string WorkspaceNotFoundErrorMessageFormat = "Workspace '{0}' is not found.";
    internal const string WorkspaceNotFoundErrorCode = "WorkspaceNotFound";
    internal const string ArtifactNotFoundErrorMessageFormat = "Mirrored Database '{0}' is not found in workspace '{1}'.";
    internal const string ArtifactNotFoundAndWillBeCreatedErrorMessageFormat = "Mirrored Database '{0}' is not found in workspace '{1}' and will be created automatically.";
    internal const string ArtifactNotFoundErrorCode = "ArtifactNotFound";
    private readonly TimeSpan _mirroredDatabaseCreationRetryInterval;
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

        var createDatabaseRetryIntervalMilliseconds = ConfigurationManagerEx.AppSettings.GetValue(constants.MirroredDatabaseCreationRetryIntervalKeyName, constants.MirroredDatabaseCreationRetryIntervalDefaultValue);

        _mirroredDatabaseCreationRetryInterval = createDatabaseRetryIntervalMilliseconds > 0
            ? TimeSpan.FromMilliseconds(createDatabaseRetryIntervalMilliseconds)
            : TimeSpan.Zero;
    }

    protected override async Task<FileStorageConnectionVerificationResult> VerifyDataLakeConnection(ExecutionContext executionContext, IStorageConfiguration configuration, bool shouldLogException)
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

        using var client = await _storageStorageFactory.CreateStorageClient(executionContext, casted) as OpenMirroringStorageClient;

        try
        {
            if (shouldTolerateMissingDirectory)
            {
                if (await client.HasValidWorkspaceAsync())
                {
                    return SuccessfulConnectionVerification;
                }

                return CreateFailedConnectionVerification(InvalidWorkspaceErrorMessage);
            }
            else
            {
                var basePath = await client.GetBaseDirectoryPathAsync();
                if (await client.DirectoryExistsAsync(basePath))
                {
                    return SuccessfulConnectionVerification;
                }

                return CreateFailedConnectionVerification($"Directory '{basePath.Path}' is not found");
            }
        }
        catch (AuthenticationFailedException ex)
        {
            if (shouldLogException)
            {
                _logger.LogWarning(ex, InvalidCredentialsErrorMessage);
            }

            return CreateFailedConnectionVerification(InvalidCredentialsErrorMessage, hasException: true);
        }
        catch (RequestFailedException ex) when (WorkspaceNotFoundErrorCode.Equals(ex.ErrorCode))
        {
            var errorMessage = WorkspaceNotFoundErrorMessageFormat.FormatWith(casted.WorkspaceName);
            if (shouldLogException)
            {
                _logger.LogWarning(ex, WorkspaceNotFoundErrorMessageFormat, casted.WorkspaceName);
            }

            return CreateFailedConnectionVerification(errorMessage, hasException: true);
        }
        catch (RequestFailedException ex) when (ArtifactNotFoundErrorCode.Equals(ex.ErrorCode))
        {
            var canCreate = isHealthCheckVerification && casted.ShouldCreateMirroredDatabase;

            if (canCreate)
            {
                // We should only try to create occasionally & not at every minute interval of health check
                // Should also not try to create user click test connection because the provider definition is not present
                canCreate = false;
                var cacheKey = $"OpenMirroringConnector_{executionContext.Organization.Id}_{casted.TenantId}_{casted.WorkspaceName}_{casted.MirroredDatabaseName}";
                _ = executionContext.ApplicationContext.System.Cache.GetItem(cacheKey, () =>
                {
                    canCreate = true;
                    return _dateTimeOffsetProvider.GetCurrentUtcTime().ToString("o");
                },
                cachePolicy: cachePolicy => cachePolicy
                    .WithAbsoluteExpiration(
                        _dateTimeOffsetProvider
                        .GetCurrentUtcTime()
                        .Add(_mirroredDatabaseCreationRetryInterval)));
            }

            var format = canCreate ? ArtifactNotFoundAndWillBeCreatedErrorMessageFormat : ArtifactNotFoundErrorMessageFormat;
            var errorMessage = format.FormatWith(casted.MirroredDatabaseName, casted.WorkspaceName);

            if (shouldLogException)
            {
                _logger.LogWarning(ex, format, casted.MirroredDatabaseName, casted.WorkspaceName);
            }

            if (canCreate)
            {
                FireAndForgetMirroredDatabaseCreation(executionContext, casted, shouldLogException);
            }

            return CreateFailedConnectionVerification(errorMessage, hasException: true);
        }
        catch (Exception ex)
        {
            if (shouldLogException)
            {
                _logger.LogWarning(ex, "Failed to check if directory exists.");
            }
            return CreateFailedConnectionVerification(ex.Message, hasException: true);
        }

        void FireAndForgetMirroredDatabaseCreation(ExecutionContext executionContext, OpenMirroringConnectorConfiguration casted, bool shouldLogException)
        {

            // Try to create the mirrored database in the background, but don't block the health check
            _ = Task.Run(async () =>
            {
                try
                {
                    await using var backgroundExecutionContext = executionContext.ApplicationContext.CreateExecutionContext(executionContext.Organization);
                    using var backgroundClient = await _storageStorageFactory.CreateStorageClient(backgroundExecutionContext, casted) as OpenMirroringStorageClient;
                    if (backgroundClient != null)
                    {
                        await TryCreateMirroredDatabase(backgroundExecutionContext, casted, backgroundClient, shouldLogException);
                    }
                }
                catch (Exception taskEx)
                {
                    if (shouldLogException)
                    {
                        executionContext.Log.LogWarning(taskEx, "Failed to create mirrored database in background task.");
                    }
                }
            });
        }

        static async Task TryCreateMirroredDatabase(ExecutionContext executionContext, OpenMirroringConnectorConfiguration casted, OpenMirroringStorageClient client, bool shouldLogException)
        {
            try
            {
                var providerDefinitionStore = executionContext.Organization.DataStores.GetDataStore<ProviderDefinition>();
                if (casted.Configurations.TryGetValue(StorageConfigurationConstants.ProviderDefinitionIdKey, out var providerDefinitionId))
                {
                    if (providerDefinitionId is string providerDefinitionIdString && Guid.TryParse(providerDefinitionIdString, out var providerDefinitionGuid))
                    {
                        var providerDefinition = await providerDefinitionStore.GetByIdAsync(executionContext, providerDefinitionGuid);

                        if (providerDefinition == null)
                        {
                            executionContext.Log.LogWarning("Unable to find provider definition with id '{ProviderDefinitionId}'. Skipping creation.", providerDefinitionGuid);
                            return;
                        }

                        if (providerDefinition.ProviderId != OpenMirroringConfigurationConstants.DataLakeProviderId)
                        {
                            executionContext.Log.LogWarning("Skipping creating of mirrored database for '{ProviderDefinitionId}' because ProviderId is not '{ProviderId}'.",
                                providerDefinitionGuid,
                                OpenMirroringConfigurationConstants.DataLakeProviderId);
                            return;
                        }

                        var result = await client.UpdateOrCreateMirroredDatabaseAsync(providerDefinitionGuid, providerDefinition.IsEnabled);
                        if (result.IsCreated)
                        {
                            executionContext.Log.LogInformation("Successfully created mirrored database '{MirroredDatabaseName}' in workspace '{WorkspaceName}'.", casted.MirroredDatabaseName, casted.WorkspaceName);
                        }
                        else if (result.IsSkipped)
                        {
                            executionContext.Log.LogWarning("Skipped creating mirrored database '{MirroredDatabaseName}' in workspace '{WorkspaceName}'.", casted.MirroredDatabaseName, casted.WorkspaceName);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                if (shouldLogException)
                {
                    executionContext.Log.LogWarning(ex, "Failed to create mirrored database.");
                }
            }
        }
    }

    public override async Task ArchiveContainer(ExecutionContext executionContext, IReadOnlyStreamModel streamModel)
    {
        var providerDefinitionId = streamModel.ConnectorProviderDefinitionId!.Value;
        var containerName = streamModel.ContainerName;

        var configuration = await StorageFactory.CreateStorageConfiguration(executionContext, streamModel);
        var subDirectory = await OutputDirectoryHelper.GetSubDirectory(executionContext, configuration, streamModel.Id, containerName, _dateTimeOffsetProvider.GetCurrentUtcTime(), configuration.OutputFormat);
        using var client = await StorageFactory.CreateStorageClient(executionContext, configuration);
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
