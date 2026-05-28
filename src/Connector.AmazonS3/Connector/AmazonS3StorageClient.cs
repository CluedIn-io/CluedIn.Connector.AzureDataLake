using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading.Tasks;

using Amazon;
using Amazon.S3;
using Amazon.S3.Model;

using CluedIn.Connector.FileStorage.Common;
using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.AmazonS3.Connector;

internal class AmazonS3StorageClient : IStorageClient
{
    private readonly ILogger<AmazonS3StorageClient> _logger;
    private readonly ILoggerFactory _loggerFactory;
    private readonly AmazonS3ConnectorConfiguration _configuration;
    private readonly IAmazonS3 _s3Client;
    private bool _disposed;

    public AmazonS3StorageClient(
        ILogger<AmazonS3StorageClient> logger,
        ILoggerFactory loggerFactory,
        AmazonS3ConnectorConfiguration configuration)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _loggerFactory = loggerFactory ?? throw new ArgumentNullException(nameof(loggerFactory));
        _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        _s3Client = GetS3Client(_configuration);
    }

    public async Task CreateDirectoryIfNotExistsAsync(DirectoryPath directoryPath)
    {
        // S3 doesn't require directory creation - directories are virtual.
        // We just verify the bucket is accessible.
        // Verify bucket accessibility by listing zero objects
        try
        {
            await _s3Client.ListObjectsV2Async(new ListObjectsV2Request
            {
                BucketName = _configuration.BucketName,
                MaxKeys = 0,
            });
        }
        catch (AmazonS3Exception ex)
        {
            _logger.LogError(ex, "An error occurred while trying to get directory information.");
            throw;
        }
    }

    public async Task DeleteDirectoryAsync(DirectoryPath directoryPath)
    {
        var prefix = directoryPath.GetPrefix();

        if (string.IsNullOrEmpty(prefix))
        {
            return;
        }

        var listRequest = new ListObjectsV2Request
        {
            BucketName = _configuration.BucketName,
            Prefix = prefix + "/",
        };

        ListObjectsV2Response response;
        do
        {
            response = await _s3Client.ListObjectsV2Async(listRequest);

            if (response.S3Objects.Any())
            {
                var deleteRequest = new DeleteObjectsRequest
                {
                    BucketName = _configuration.BucketName,
                    Objects = response.S3Objects
                        .Select(o => new KeyVersion { Key = o.Key })
                        .ToList(),
                };

                await _s3Client.DeleteObjectsAsync(deleteRequest);
            }

            listRequest.ContinuationToken = response.NextContinuationToken;
        } while (response?.IsTruncated == true);
    }

    public async Task DeleteFileAsync(FilePath filePath)
    {
        var key = filePath.GetKey();

        var response = await _s3Client.DeleteObjectAsync(_configuration.BucketName, key);

        if ((int)response.HttpStatusCode < (int)HttpStatusCode.OK || // 200
            (int)response.HttpStatusCode >= (int)HttpStatusCode.MultipleChoices) // 300
        {
            throw new Exception($"S3 DeleteObject returned {response.HttpStatusCode}");
        }
    }

    public async Task<bool> DirectoryExistsAsync(DirectoryPath directory)
    {
        var prefix = directory.GetPrefix();

        var listRequest = new ListObjectsV2Request
        {
            BucketName = _configuration.BucketName,
            Prefix = string.IsNullOrEmpty(prefix) ? null : prefix + "/",
            MaxKeys = 1,
        };

        try
        {
            var response = await _s3Client.ListObjectsV2Async(listRequest);
            return response.S3Objects.Any();
        }
        catch (AmazonS3Exception ex) when (ex.StatusCode == HttpStatusCode.NotFound)
        {
            return false;
        }
    }

    public async Task<bool> FileExistsAsync(FilePath filePath)
    {
        var key = filePath.GetKey();

        try
        {
            await _s3Client.GetObjectMetadataAsync(_configuration.BucketName, key);
            return true;
        }
        catch (AmazonS3Exception ex) when (ex.StatusCode == HttpStatusCode.NotFound)
        {
            return false;
        }
    }

    public Task<DirectoryPath> GetBaseDirectoryPathAsync()
    {
        return Task.FromResult(new DirectoryPath(_configuration.RootDirectoryPath));
    }

    public Task<IStorageFileClient> GetFileClientAsync(FilePath filePath)
    {
        return Task.FromResult<IStorageFileClient>(
            new AmazonS3StorageFileClient(
                _loggerFactory.CreateLogger<AmazonS3StorageFileClient>(),
                _loggerFactory,
                _s3Client,
                _configuration.BucketName,
                filePath));
    }

    public async Task<FileMetadata> GetFileMetadataAsync(FilePath filePath)
    {
        var key = filePath.GetKey();

        try
        {
            var metadata = await _s3Client.GetObjectMetadataAsync(_configuration.BucketName, key);
            var metadataDict = new Dictionary<string, string>();
            foreach (var metaKey in metadata.Metadata.Keys)
            {
                metadataDict[metaKey.Replace("x-amz-meta-", string.Empty)] = metadata.Metadata[metaKey];
            }

            return new FileMetadata(metadataDict);
        }
        catch (AmazonS3Exception ex) when (ex.StatusCode == HttpStatusCode.NotFound)
        {
            return null;
        }
    }

    public async Task<IEnumerable<FullyQualifiedFilePath>> GetFilesInDirectoryAsync(DirectoryPath directoryPath)
    {
        var prefix = directoryPath.GetPrefix();

        var listRequest = new ListObjectsV2Request
        {
            BucketName = _configuration.BucketName,
            Prefix = string.IsNullOrEmpty(prefix) ? null : prefix + "/",
        };

        var result = new List<FullyQualifiedFilePath>();
        ListObjectsV2Response response;
        do
        {
            response = await _s3Client.ListObjectsV2Async(listRequest);

            foreach (var s3Object in response.S3Objects)
            {
                if (!s3Object.Key.EndsWith("/"))
                {
                    var fileName = Path.GetFileName(s3Object.Key);
                    var filePath = new FilePath(fileName, directoryPath);
                    result.Add(new FullyQualifiedFilePath (fileName, directoryPath, filePath.GetS3Url(_configuration.BucketName)));
                }
            }

            listRequest.ContinuationToken = response.NextContinuationToken;
        } while (response?.IsTruncated == true);

        return result;
    }

    public async Task SaveDataAsync(FilePath filePath, string content, string contentType)
    {
        await using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(content));
        var putRequest = new PutObjectRequest
        {
            BucketName = _configuration.BucketName,
            Key = filePath.GetKey(),
            InputStream = stream,
            ContentType = contentType,
        };

        await _s3Client.PutObjectAsync(putRequest);
    }

    public async Task VerifyConnectionAsync()
    {
        await CreateDirectoryIfNotExistsAsync(await GetBaseDirectoryPathAsync());
    }

    private static IAmazonS3 GetS3Client(AmazonS3ConnectorConfiguration configuration)
    {
        var region = RegionEndpoint.GetBySystemName(configuration.Region);
        return new AmazonS3Client(configuration.AccessKey, configuration.SecretKey, region);
    }
    public void Dispose()
    {
        Dispose(true);
        // Prevent the GC from calling the finalizer
        GC.SuppressFinalize(this);
    }

    protected virtual void Dispose(bool disposing)
    {
        if (_disposed)
            return;

        if (disposing)
        {
            _s3Client?.Dispose();
        }

        _disposed = true;
    }
}
