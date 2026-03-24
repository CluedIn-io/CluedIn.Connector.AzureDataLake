using System;

using Amazon.S3;

using CluedIn.Connector.DataLake.Common.Connector;

namespace CluedIn.Connector.AmazonS3.Connector;

internal class AmazonS3StorageDirectoryClient : IStorageDirectoryClient
{
    private readonly IAmazonS3 _s3Client;
    private readonly string _bucketName;
    private readonly string _directoryPrefix;

    public AmazonS3StorageDirectoryClient(IAmazonS3 s3Client, string bucketName, string directoryPrefix)
    {
        _s3Client = s3Client ?? throw new ArgumentNullException(nameof(s3Client));
        _bucketName = bucketName ?? throw new ArgumentNullException(nameof(bucketName));
        _directoryPrefix = directoryPrefix ?? string.Empty;
    }

    public IStorageFileClient GetFileClient(string fileName)
    {
        var key = string.IsNullOrEmpty(_directoryPrefix)
            ? fileName
            : $"{_directoryPrefix.TrimEnd('/')}/{fileName}";

        return new AmazonS3StorageFileClient(_s3Client, _bucketName, key);
    }
}
