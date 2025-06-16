using CluedIn.Connector.DataLake.Common;
using System.Threading.Tasks;
using System;

using CluedIn.Connector.DataLake.Common.Connector;
using CluedIn.Core;
using CluedIn.Core.Connectors;

using Microsoft.Extensions.Logging;
using System.Text.RegularExpressions;

namespace CluedIn.Connector.AzureDataLake.Connector;

public class AzureDataLakeConnector : DataLakeConnector
{
    private readonly ILogger<AzureDataLakeConnector> _logger;
    internal static readonly Regex _accountNameRegex = new Regex("^[a-z0-9]+$", RegexOptions.Compiled);
    internal const string InvalidAccountNameErrorMessage = "Invalid storage account name. It can only contain numbers and lowercase characters.";
    internal const string InvalidAccountKeyErrorMessage = "Invalid account key. It must be a valid base64 string.";
    internal const string InvalidCredentialsErrorMessage = "Invalid storage account credentials.";

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

    protected override async Task<ConnectionVerificationResult> VerifyDataLakeConnection(IDataLakeJobData jobData)
    {
        if (jobData is not AzureDataLakeConnectorJobData casted)
        {
            throw new ArgumentException($"Invalid job data type: {jobData.GetType().Name}. Expected: {nameof(AzureDataLakeConnectorJobData)}.");
        }

        if (string.IsNullOrWhiteSpace(casted.AccountName) || !_accountNameRegex.IsMatch(casted.AccountName))
        {
            return CreateFailedConnectionVerification(InvalidAccountNameErrorMessage);
        }

        if (string.IsNullOrWhiteSpace(casted.AccountKey) || !IsBase64String(casted.AccountKey))
        {
            return CreateFailedConnectionVerification(InvalidAccountKeyErrorMessage);
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
    }

    private static bool IsBase64String(string base64)
    {
        var buffer = new Span<byte>(new byte[base64.Length]);
        return Convert.TryFromBase64String(base64, buffer, out int bytesParsed);
    }
}
