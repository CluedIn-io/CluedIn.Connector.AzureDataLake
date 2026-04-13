using System;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

using Amazon.S3;

using CluedIn.Connector.FileStorage.Common;
using CluedIn.Connector.FileStorage.Common.Connector;
using CluedIn.Core;
using CluedIn.Core.Connectors;

using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.AmazonS3.Connector;

public class AmazonS3Connector : StorageConnectorBase
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
        ApplicationContext applicationContext,
        IAmazonS3ConfigurationConstants constants,
        AmazonS3StorageFactory storageFactory,
        IDateTimeOffsetProvider dateTimeOffsetProvider)
        : base(logger, applicationContext, constants, storageFactory, dateTimeOffsetProvider)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    protected override Type ExportJobType => typeof(AmazonS3ExportEntitiesJob);

    protected override async Task<ConnectionVerificationResult> VerifyDataLakeConnection(ExecutionContext executionContext, IStorageConfiguration configuration)
    {
        if (configuration is not AmazonS3ConnectorConfiguration casted)
        {
            throw new ArgumentException($"Invalid job data type: {configuration.GetType().Name}. Expected: {nameof(AmazonS3ConnectorConfiguration)}.");
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
            return await base.VerifyDataLakeConnection(executionContext, configuration);
        }
        catch (AmazonS3Exception s3Ex) when (IsAuthenticationError(s3Ex))
        {
            _logger.LogWarning(s3Ex, "S3 authentication error when verifying connection.");
            return CreateFailedConnectionVerification(InvalidCredentialsErrorMessage);
        }
        catch (AmazonS3Exception s3Ex)
        {
            _logger.LogWarning(s3Ex, "S3 error when verifying connection.");
            return CreateFailedConnectionVerification($"S3 error: {s3Ex.Message}");
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Error when verifying S3 connection.");
            return CreateFailedConnectionVerification($"Failed to connect to S3: {ex.Message}");
        }
    }

    private static bool IsAuthenticationError(AmazonS3Exception ex)
    {
        return ex.ErrorCode is "InvalidAccessKeyId" or "SignatureDoesNotMatch";
    }

    private static bool IsValidBucketName(string bucketName)
    {
        return !string.IsNullOrWhiteSpace(bucketName) && BucketNameRegex.IsMatch(bucketName);
    }
}
