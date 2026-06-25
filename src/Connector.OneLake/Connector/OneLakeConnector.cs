using System;
using System.Threading.Tasks;

using Azure;
using Azure.Identity;

using CluedIn.Connector.FileStorage.Common;
using CluedIn.Connector.FileStorage.Common.Connector;
using CluedIn.Core;
using CluedIn.Core.Connectors;

using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.OneLake.Connector;

public class OneLakeConnector : StorageConnectorBase
{
    internal const string InvalidCredentialsErrorMessage = "Authentication failed due to invalid credentials.";
    internal const string InvalidWorkspaceErrorMessage = "Workspace name cannot be empty.";
    internal const string WorkspaceNotFoundErrorMessageFormat = "Workspace '{0}' is not found.";
    internal const string InvalidFolderErrorMessage = "Invalid Folder. It has to start with Files.";

    internal const string WorkspaceNotFoundErrorCode = "WorkspaceNotFound";
    internal const string ArtifactNotFoundErrorMessageFormat = "Item '{0}' with type '{1}' is not found.";
    internal const string ArtifactNotFoundErrorCode = "ArtifactNotFound";
    private readonly ILogger<OneLakeConnector> _logger;

    public OneLakeConnector(
        ILogger<OneLakeConnector> logger,
        ApplicationContext applicationContext,
        IOneLakeConfigurationConstants constants,
        OneLakeStorageFactory dataLakeStorageJobDataFactory,
        IDateTimeOffsetProvider dateTimeOffsetProvider)
        : base(logger, applicationContext, constants, dataLakeStorageJobDataFactory, dateTimeOffsetProvider)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    protected override async Task<ConnectionVerificationResult> VerifyDataLakeConnection(IDataLakeJobData jobData)
    {
        if (configuration is not OneLakeConnectorConfiguration casted)
        {
            throw new ArgumentException($"Invalid job data type: {configuration.GetType().Name}. Expected: {nameof(OneLakeConnectorConfiguration)}.");
        }

        if (string.IsNullOrWhiteSpace(casted.WorkspaceName))
        {
            return CreateFailedConnectionVerification(InvalidWorkspaceErrorMessage);
        }

        if (string.IsNullOrWhiteSpace(casted.ItemFolder) || !casted.ItemFolder.StartsWith("Files"))
        {
            return CreateFailedConnectionVerification(InvalidFolderErrorMessage);
        }

        try
        {
            return await base.VerifyDataLakeConnection(jobData);
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
                _logger.LogWarning(ex, WorkspaceNotFoundErrorMessageFormat, casted?.WorkspaceName);
            }
            return CreateFailedConnectionVerification(errorMessage, hasException: true);
        }
        catch (RequestFailedException ex) when (ArtifactNotFoundErrorCode.Equals(ex.ErrorCode))
        {
            var errorMessage = ArtifactNotFoundErrorMessageFormat.FormatWith(casted.ItemName, casted.ItemType);
            if (shouldLogException)
            {
                _logger.LogWarning(ex, ArtifactNotFoundErrorMessageFormat, casted.ItemName, casted.ItemType);
            }
        }
        catch (Exception ex)
        {
            if (shouldLogException)
            {
                _logger.LogWarning(ex, "Error when verifying datalake connection.");
            }
            return CreateFailedConnectionVerification(ex.Message, hasException: true);
        }
    }

    protected override Type ExportJobType => typeof(OneLakeExportEntitiesJob);

    protected override async Task<FileStorageConnectionVerificationResult> VerifyConnectionInternal(ExecutionContext executionContext, IStorageConfiguration configuration, bool shouldLogException)
    {
        var result = await base.VerifyConnectionInternal(executionContext, jobData, shouldLogException);

        if (result?.Success != true || !configuration.IsStreamCacheEnabled)
        {
            return result;
        }

        var casted = (OneLakeConnectorConfiguration)configuration;
        if (!casted.ShouldLoadToTable)
        {
            return result;
        }

        if (!StorageConfigurationConstants.OutputFormats.IsValid(casted.OutputFormat, isReducedSupportedFormat: true))
        {
            var supported = string.Join(',', StorageConfigurationConstants.OutputFormats.ReducedSupportedFormats);
            var errorMessage = $"Format '{configuration.OutputFormat}' is not supported. Supported formats are {supported}.";
            return CreateFailedConnectionVerification(errorMessage);
        }

        if (!casted.ShouldEscapeVocabularyKeys)
        {
            return CreateFailedConnectionVerification($"Must set {nameof(casted.ShouldEscapeVocabularyKeys)} when data should be loaded to table.");
        }

        return result;
    }
}
