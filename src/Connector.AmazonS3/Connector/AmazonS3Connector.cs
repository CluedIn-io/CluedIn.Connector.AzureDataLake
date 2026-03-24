using System;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

using CluedIn.Connector.DataLake.Common;
using CluedIn.Connector.DataLake.Common.Connector;
using CluedIn.Core;
using CluedIn.Core.Connectors;

using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.AmazonS3.Connector;

public class AmazonS3Connector : DataLakeConnector
{
    private readonly ILogger<AmazonS3Connector> _logger;
    internal static readonly Regex BucketNameRegex = new("^(?=.{3,63}$)[a-z0-9]+([-.][a-z0-9]+)*$", RegexOptions.Compiled);
    internal const string InvalidAccessKeyErrorMessage = "Invalid access key. It cannot be empty.";
    internal const string InvalidSecretKeyErrorMessage = "Invalid secret key. It cannot be empty.";
    internal const string InvalidBucketNameErrorMessage = "Invalid bucket name. Please refer to https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html for more information.";
    internal const string InvalidRegionErrorMessage = "Invalid region. It cannot be empty.";
    internal const string InvalidCredentialsErrorMessage = "Invalid S3 credentials.";

    public AmazonS3Connector(
        ILogger<AmazonS3Connector> logger,
        AmazonS3StorageClient client,
        IAmazonS3Constants constants,
        AmazonS3JobDataFactory dataLakeJobDataFactory,
        IDateTimeOffsetProvider dateTimeOffsetProvider)
        : base(logger, client, constants, dataLakeJobDataFactory, dateTimeOffsetProvider)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    protected override Type ExportJobType => typeof(AmazonS3ExportEntitiesJob);

    protected override async Task<ConnectionVerificationResult> VerifyDataLakeConnection(IDataLakeJobData jobData)
    {
        if (jobData is not AmazonS3ConnectorJobData casted)
        {
            throw new ArgumentException($"Invalid job data type: {jobData.GetType().Name}. Expected: {nameof(AmazonS3ConnectorJobData)}.");
        }

        if (string.IsNullOrWhiteSpace(casted.AccessKey))
        {
            return CreateFailedConnectionVerification(InvalidAccessKeyErrorMessage);
        }

        if (string.IsNullOrWhiteSpace(casted.SecretKey))
        {
            return CreateFailedConnectionVerification(InvalidSecretKeyErrorMessage);
        }

        if (!IsValidBucketName(casted.BucketName))
        {
            return CreateFailedConnectionVerification(InvalidBucketNameErrorMessage);
        }

        if (string.IsNullOrWhiteSpace(casted.Region))
        {
            return CreateFailedConnectionVerification(InvalidRegionErrorMessage);
        }

        try
        {
            return await base.VerifyDataLakeConnection(jobData);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Error when verifying S3 connection.");
            return CreateFailedConnectionVerification(InvalidCredentialsErrorMessage);
        }
    }

    private static bool IsValidBucketName(string bucketName)
    {
        return !string.IsNullOrWhiteSpace(bucketName) && BucketNameRegex.IsMatch(bucketName);
    }
}
