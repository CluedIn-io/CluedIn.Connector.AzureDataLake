using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using Azure;
using Azure.Identity;

using CluedIn.Connector.DataLake.Common;
using CluedIn.Connector.DataLake.Common.Connector;
using CluedIn.Core;
using CluedIn.Core.Connectors;

using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.OneLake.Connector;

public class OneLakeConnector : DataLakeConnector
{
    internal const string InvalidCredentialsErrorMessage = "Authentication failed due to invalid credentials.";
    internal const string InvalidWorkspaceErrorMessage = "Workspace name cannot be empty.";
    internal const string WorkspaceNotFoundErrorMessageFormat = "Workspace '{0}' is not found.";
    internal const string InvalidFolderErrorMessage = "Invalid Folder. It has to start with Files.";

    internal const string WorkspaceNotFoundErrorCode = "WorkspaceNotFound";
    private readonly ILogger<OneLakeConnector> _logger;

    public OneLakeConnector(
        ILogger<OneLakeConnector> logger,
        OneLakeClient client,
        IOneLakeConstants constants,
        OneLakeJobDataFactory dataLakeJobDataFactory,
        IDateTimeOffsetProvider dateTimeOffsetProvider)
        : base(logger, client, constants, dataLakeJobDataFactory, dateTimeOffsetProvider)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    protected override async Task<ConnectionVerificationResult> VerifyDataLakeConnection(IDataLakeJobData jobData)
    {
        if (jobData is not OneLakeConnectorJobData casted)
        {
            throw new ArgumentException($"Invalid job data type: {jobData.GetType().Name}. Expected: {nameof(OneLakeConnectorJobData)}.");
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
            _logger.LogWarning(ex, InvalidCredentialsErrorMessage);
            return CreateFailedConnectionVerification(InvalidCredentialsErrorMessage);
        }
        catch (RequestFailedException ex) when (WorkspaceNotFoundErrorCode.Equals(ex.ErrorCode))
        {
            var errorMessage = WorkspaceNotFoundErrorMessageFormat.FormatWith(casted.WorkspaceName);
            _logger.LogWarning(ex, WorkspaceNotFoundErrorMessageFormat, casted?.WorkspaceName);
            return CreateFailedConnectionVerification(errorMessage);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Error when verifying datalake connection.");
            return CreateFailedConnectionVerification(ex.Message);
        }
    }

    protected override Type ExportJobType => typeof(OneLakeExportEntitiesJob);

    protected override async Task<ConnectionVerificationResult> VerifyConnectionInternal(ExecutionContext executionContext, IDataLakeJobData jobData)
    {
        var result = await base.VerifyConnectionInternal(executionContext, jobData);

        if (result?.Success != true || !jobData.IsStreamCacheEnabled)
        {
            return result;
        }

        var casted = (OneLakeConnectorJobData)jobData;
        if (!casted.ShouldLoadToTable)
        {
            return result;
        }

        if (!DataLakeConstants.OutputFormats.IsValid(casted.OutputFormat, isReducedSupportedFormat: true))
        {
            var supported = string.Join(',', DataLakeConstants.OutputFormats.ReducedSupportedFormats);
            var errorMessage = $"Format '{jobData.OutputFormat}' is not supported. Supported formats are {supported}.";
            return new ConnectionVerificationResult(false, errorMessage);
        }

        if (!casted.ShouldEscapeVocabularyKeys)
        {
            return new ConnectionVerificationResult(false, $"Must set {nameof(casted.ShouldEscapeVocabularyKeys)} when data should be loaded to table.");
        }

        return result;
    }
}
