using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

using Amazon.S3;
using Amazon.S3.Model;

using CluedIn.Connector.DataLake.Common.Connector;

namespace CluedIn.Connector.AmazonS3.Connector;

internal class AmazonS3StorageFileClient : IStorageFileClient
{
    private readonly IAmazonS3 _s3Client;
    private readonly string _bucketName;
    private readonly string _key;

    public AmazonS3StorageFileClient(IAmazonS3 s3Client, string bucketName, string key)
    {
        _s3Client = s3Client ?? throw new ArgumentNullException(nameof(s3Client));
        _bucketName = bucketName ?? throw new ArgumentNullException(nameof(bucketName));
        _key = key ?? throw new ArgumentNullException(nameof(key));
    }

    public string Uri => $"s3://{_bucketName}/{_key}";

    public string Path => _key;

    public async Task<Stream> OpenWriteAsync(bool overwrite, CancellationToken cancellationToken = default)
    {
        return new AmazonS3WriteStream(_s3Client, _bucketName, _key);
    }

    public async Task DeleteIfExistsAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            await _s3Client.DeleteObjectAsync(_bucketName, _key, cancellationToken);
        }
        catch (AmazonS3Exception ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            // File doesn't exist, nothing to delete
        }
    }

    public async Task SetMetadataAsync(IDictionary<string, string> metadata, CancellationToken cancellationToken = default)
    {
        try
        {
            var getRequest = new GetObjectMetadataRequest
            {
                BucketName = _bucketName,
                Key = _key,
            };
            var existingMetadata = await _s3Client.GetObjectMetadataAsync(getRequest, cancellationToken);

            var copyRequest = new CopyObjectRequest
            {
                SourceBucket = _bucketName,
                SourceKey = _key,
                DestinationBucket = _bucketName,
                DestinationKey = _key,
                MetadataDirective = S3MetadataDirective.REPLACE,
            };

            foreach (var kvp in metadata)
            {
                copyRequest.Metadata[kvp.Key] = kvp.Value;
            }

            await _s3Client.CopyObjectAsync(copyRequest, cancellationToken);
        }
        catch (AmazonS3Exception ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            // Object doesn't exist yet, metadata will be set when the file is uploaded
        }
    }

    public async Task RenameAsync(string targetPath, CancellationToken cancellationToken = default)
    {
        var copyRequest = new CopyObjectRequest
        {
            SourceBucket = _bucketName,
            SourceKey = _key,
            DestinationBucket = _bucketName,
            DestinationKey = targetPath,
            MetadataDirective = S3MetadataDirective.COPY,
        };

        await _s3Client.CopyObjectAsync(copyRequest, cancellationToken);
        await _s3Client.DeleteObjectAsync(_bucketName, _key, cancellationToken);
    }
}
