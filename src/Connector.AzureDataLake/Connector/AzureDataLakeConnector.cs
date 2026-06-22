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
    internal static readonly Regex AccountNameRegex = new("^[a-z0-9]+$", RegexOptions.Compiled);
    internal static readonly Regex FileSystemNameRegex = new("^(?=.{3,63}$)[a-z0-9]+(-[a-z0-9]+)*$", RegexOptions.Compiled);
    internal const string InvalidAccountNameErrorMessage = "Invalid storage account name. It can only contain numbers and lowercase characters.";
    internal const string InvalidAccountKeyErrorMessage = "Invalid account key. It must be a valid base64 string.";
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

        if (!IsValidAccountKey())
        {
            return CreateFailedConnectionVerification(InvalidAccountKeyErrorMessage);
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

        bool IsValidAccountKey()
        {
            return !string.IsNullOrWhiteSpace(casted.AccountKey) && IsBase64String(casted.AccountKey);
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

    private static bool IsBase64String(string base64)
    {
        var buffer = new Span<byte>(new byte[base64.Length]);
        return Convert.TryFromBase64String(base64, buffer, out _);
    }
}
