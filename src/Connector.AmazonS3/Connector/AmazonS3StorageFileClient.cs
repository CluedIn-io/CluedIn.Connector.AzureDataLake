using System;
using System.IO;
using System.Threading.Tasks;

using Amazon.S3;
using Amazon.S3.Model;

using CluedIn.Connector.FileStorage.Common;

namespace CluedIn.Connector.AmazonS3.Connector;

internal class AmazonS3StorageFileClient : IStorageFileClient
{
    private readonly FilePath _filePath;
    private readonly IAmazonS3 _s3Client;
    private readonly string _bucketName;

    public AmazonS3StorageFileClient(IAmazonS3 s3Client, string bucketName, FilePath filePath)
    {
        _filePath = filePath ?? throw new ArgumentNullException(nameof(filePath));
        _s3Client = s3Client ?? throw new ArgumentNullException(nameof(s3Client));
        _bucketName = bucketName;
    }

    public Uri Uri => new Uri(_filePath.GetS3Url(_bucketName));

    public Task DeleteAsync()
    {
        return DeleteIfExistsAsync();
    }

    public async Task DeleteIfExistsAsync()
    {
        try
        {
            await _s3Client.DeleteObjectAsync(_bucketName, _filePath.GetKey());
        }
        catch (AmazonS3Exception ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            // File doesn't exist, nothing to delete
        }
    }

    public async Task<bool> ExistsAsync()
    {
        try
        {
            await _s3Client.GetObjectMetadataAsync(_bucketName, _filePath.GetKey());
            return true;
        }
        catch (AmazonS3Exception ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            return false;
        }
    }

    public async Task<Stream> OpenWriteAsync(bool overwrite)
    {
        return new AmazonS3WriteStream(_s3Client, _bucketName, _filePath.GetKey());
    }

    public async Task RenameAsync(FilePath targetPath)
    {
        var key = _filePath.GetKey();

        var copyRequest = new CopyObjectRequest
        {
            SourceBucket = _bucketName,
            SourceKey = key,
            DestinationBucket = _bucketName,
            DestinationKey = targetPath.GetKey(),
            MetadataDirective = S3MetadataDirective.COPY,
        };

        await _s3Client.CopyObjectAsync(copyRequest);
        await _s3Client.DeleteObjectAsync(_bucketName, key);
    }

    public async Task SetMetadataAsync(FileMetadata fileMetadata)
    {
        try
        {
            var key = _filePath.GetKey();
            var getRequest = new GetObjectMetadataRequest
            {
                BucketName = _bucketName,
                Key = key,
            };
            var existingMetadata = await _s3Client.GetObjectMetadataAsync(getRequest);

            var copyRequest = new CopyObjectRequest
            {
                SourceBucket = _bucketName,
                SourceKey = key,
                DestinationBucket = _bucketName,
                DestinationKey = key,
                MetadataDirective = S3MetadataDirective.REPLACE,
            };

            foreach (var kvp in fileMetadata.Metadata)
            {
                copyRequest.Metadata[kvp.Key] = kvp.Value;
            }

            await _s3Client.CopyObjectAsync(copyRequest);
        }
        catch (AmazonS3Exception ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            // Object doesn't exist yet, metadata will be set when the file is uploaded
        }
    }
}
