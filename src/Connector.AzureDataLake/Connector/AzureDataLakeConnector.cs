using System;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

using CluedIn.Connector.FileStorage.Common;
using CluedIn.Connector.FileStorage.Common.Connector;
using CluedIn.Core;
using CluedIn.Core.Connectors;

using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.AzureDataLake.Connector;

public class AzureDataLakeConnector : StorageConnectorBase
{
    private readonly ILogger<AzureDataLakeConnector> _logger;
    private readonly IDateTimeOffsetProvider _dateTimeOffsetProvider;
    internal static readonly Regex AccountNameRegex = new("^[a-z0-9]+$", RegexOptions.Compiled);
    internal static readonly Regex FileSystemNameRegex = new("^(?=.{3,63}$)[a-z0-9]+(-[a-z0-9]+)*$", RegexOptions.Compiled);
    internal const string InvalidAuthenticationMethodErrorMessage = "Invalid authentication method";
    internal const string InvalidAccountNameErrorMessage = "Invalid storage account name. It can only contain numbers and lowercase characters.";
    internal const string InvalidAccountKeyErrorMessage = "Invalid account key. It must be a valid base64 string or a valid SAS token.";
    internal const string InvalidSasTokenTimeErrorMessage = "Invalid SAS token. It must be a valid SAS token, with valid time parameters.";
    internal const string InvalidSasTokenPermissionsErrorMessage = "Invalid SAS token. It must be a valid SAS token, with valid permissions. Read, Write, Delete, List, and Create Permissions must be present to Containers and Objects in Blob Service";
    internal const string InvalidCredentialsErrorMessage = "Invalid storage account credentials.";
    internal const string InvalidFileSystemNameErrorMessage = "Invalid file system name. Please refer to https://learn.microsoft.com/en-us/rest/api/storageservices/naming-and-referencing-containers--blobs--and-metadata#container-names for more information";
    internal const string InvalidDirectoryNameErrorMessage = "Invalid directory name. Please refer to https://learn.microsoft.com/en-us/rest/api/storageservices/naming-and-referencing-containers--blobs--and-metadata#directory-names for more information";

    public AzureDataLakeConnector(
        ILogger<AzureDataLakeConnector> logger,
        ApplicationContext applicationContext,
        IAzureDataLakeConfigurationConstants constants,
        AzureDataLakeStorageFactory storageFactory,
        IDateTimeOffsetProvider dateTimeOffsetProvider)
        : base(logger, applicationContext, constants, storageFactory, dateTimeOffsetProvider)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _dateTimeOffsetProvider = dateTimeOffsetProvider ?? throw new ArgumentNullException(nameof(dateTimeOffsetProvider));
    }

    protected override Type ExportJobType => typeof(AzureDataLakeExportEntitiesJob);

    protected override async Task<FileStorageConnectionVerificationResult> VerifyDataLakeConnection(ExecutionContext executionContext, IStorageConfiguration configuration, bool shouldLogException)
    {
        if (configuration is not AzureDataLakeConnectorConfiguration casted)
        {
            throw new ArgumentException($"Invalid configuration type: {configuration.GetType().Name}. Expected: {nameof(AzureDataLakeConnectorConfiguration)}.");
        }

        if (!IsValidAccountName())
        {
            return CreateFailedConnectionVerification(InvalidAccountNameErrorMessage);
        }

        if (!Enum.TryParse<AuthenticationMethods>(casted.AuthenticationMethod, out var authMethod))
        {
            return CreateFailedConnectionVerification(InvalidAuthenticationMethodErrorMessage);
        }

        if (authMethod == AuthenticationMethods.SharedKey)
        {
            if (AzureDataLakeConnectorConfiguration.IsSharedAccessKey(casted.AccountKey))
            {
                if (!casted.IsValidSharedKey())
                {
                    return CreateFailedConnectionVerification(InvalidAccountKeyErrorMessage);
                }
            }
            else
            {
                if (!casted.IsValidSasTokenTime(_dateTimeOffsetProvider))
                {
                    return CreateFailedConnectionVerification(InvalidSasTokenTimeErrorMessage);
                }
                else if (!casted.IsValidSasTokenPermissions())
                {
                    return CreateFailedConnectionVerification(InvalidSasTokenPermissionsErrorMessage);
                }
            }
        }

        if (!IsValidFileSystemName())
        {
            return CreateFailedConnectionVerification(InvalidFileSystemNameErrorMessage);
        }

        if (!IsValidDirectoryName())
        {
            return CreateFailedConnectionVerification(InvalidDirectoryNameErrorMessage);
        }

        try
        {
            return await base.VerifyDataLakeConnection(executionContext, configuration, shouldLogException);
        }
        catch (Exception ex)
        {
            if (shouldLogException)
            {
                _logger.LogWarning(ex, "Error when verifying datalake connection.");
            }
            return CreateFailedConnectionVerification(InvalidCredentialsErrorMessage, hasException: true);
        }

        bool IsValidAccountName()
        {
            return !string.IsNullOrWhiteSpace(casted.AccountName) && AccountNameRegex.IsMatch(casted.AccountName);
        }

        bool IsValidFileSystemName()
        {
            return !string.IsNullOrWhiteSpace(casted.FileSystemName) && FileSystemNameRegex.IsMatch(casted.FileSystemName);
        }

        bool IsValidDirectoryName()
        {
            if (string.IsNullOrWhiteSpace(casted.DirectoryName))
            {
                return false;
            }

            var segments = casted.DirectoryName.Split('/');
            return segments.All(segment => segment.Length > 0 && !segment.EndsWith("."));
        }
    }
}
