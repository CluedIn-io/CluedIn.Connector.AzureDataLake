using System;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

using Azure.Storage.Files.DataLake;

using CluedIn.Connector.DataLake.Common;
using CluedIn.Connector.DataLake.Common.Connector;
using CluedIn.Core;
using CluedIn.Core.Connectors;

using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.AzureDataLake.Connector;

public class AzureDataLakeConnector : DataLakeConnector
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
        AzureDataLakeClient client,
        IAzureDataLakeConstants constants,
        AzureDataLakeJobDataFactory dataLakeJobDataFactory,
        IDateTimeOffsetProvider dateTimeOffsetProvider)
        : base(logger, client, constants, dataLakeJobDataFactory, dateTimeOffsetProvider)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    protected override Type ExportJobType => typeof(AzureDataLakeExportEntitiesJob);

    protected override async Task<ConnectionVerificationResult> VerifyDataLakeConnection(IDataLakeJobData jobData)
    {

        var options = new DataLakeClientOptions();
        this._logger.LogCritical("VERSION!!!!!!!!  {Version} {VersionInt}", options.Version.ToString(), options.Version);
        if (jobData is not AzureDataLakeConnectorJobData casted)
        {
            throw new ArgumentException($"Invalid job data type: {jobData.GetType().Name}. Expected: {nameof(AzureDataLakeConnectorJobData)}.");
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
            return await base.VerifyDataLakeConnection(jobData);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Error when verifying datalake connection.");
            return CreateFailedConnectionVerification(InvalidCredentialsErrorMessage);
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
