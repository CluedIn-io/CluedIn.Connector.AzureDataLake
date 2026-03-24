using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

using Amazon;
using Amazon.S3;
using Amazon.S3.Model;

using CluedIn.Connector.DataLake.Common;
using CluedIn.Connector.DataLake.Common.Connector;
using CluedIn.Core.Connectors;

namespace CluedIn.Connector.AmazonS3.Connector;

public class AmazonS3Client : IExternalFileStorageClient
{
    private IAmazonS3 GetS3Client(IDataLakeJobData configuration)
    {
        var casted = CastJobData(configuration);
        var region = RegionEndpoint.GetBySystemName(casted.Region);
        return new AmazonS3Client(casted.AccessKey, casted.SecretKey, region);
    }

    private static AmazonS3ConnectorJobData CastJobData(IDataLakeJobData jobData)
    {
        if (jobData is not AmazonS3ConnectorJobData castedJobData)
        {
            throw new ApplicationException($"Provided job data is not of expected type '{typeof(AmazonS3ConnectorJobData)}'. It is '{jobData.GetType()}'.");
        }
        return castedJobData;
    }

    private static string GetPrefix(IDataLakeJobData configuration, string subDirectory = null)
    {
        var root = configuration.RootDirectoryPath?.TrimEnd('/') ?? string.Empty;

        if (!string.IsNullOrEmpty(subDirectory))
        {
            root = string.IsNullOrEmpty(root)
                ? subDirectory.TrimEnd('/')
                : $"{root}/{subDirectory.TrimEnd('/')}";
        }

        return root;
    }

    private static string GetObjectKey(IDataLakeJobData configuration, string fileName)
    {
        var prefix = GetPrefix(configuration);
        return string.IsNullOrEmpty(prefix) ? fileName : $"{prefix}/{fileName}";
    }

    public async Task<IStorageDirectoryClient> EnsureDirectoryExist(IDataLakeJobData configuration)
    {
        return await EnsureDirectoryExist(configuration, string.Empty);
    }

    public async Task<IStorageDirectoryClient> EnsureDirectoryExist(IDataLakeJobData configuration, string subDirectory)
    {
        // S3 doesn't require directory creation - directories are virtual.
        // We just verify the bucket exists.
        var s3Client = GetS3Client(configuration);
        var casted = CastJobData(configuration);

        try
        {
            await s3Client.EnsureBucketExistsAsync(casted.BucketName);
        }
        catch
        {
            // Bucket might already exist or we might not have permission to create it.
            // The actual operations will fail if the bucket truly doesn't exist.
        }

        var prefix = GetPrefix(configuration, subDirectory);
        return new AmazonS3StorageDirectoryClient(s3Client, casted.BucketName, prefix);
    }

    public async Task SaveData(IDataLakeJobData configuration, string content, string fileName, string contentType)
    {
        var s3Client = GetS3Client(configuration);
        var casted = CastJobData(configuration);
        var key = GetObjectKey(configuration, fileName);

        using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(content));
        var putRequest = new PutObjectRequest
        {
            BucketName = casted.BucketName,
            Key = key,
            InputStream = stream,
            ContentType = contentType,
        };

        await s3Client.PutObjectAsync(putRequest);
    }

    public async Task DeleteDirectory(IDataLakeJobData configuration, string subDirectory)
    {
        var s3Client = GetS3Client(configuration);
        var casted = CastJobData(configuration);
        var prefix = GetPrefix(configuration, subDirectory);

        if (string.IsNullOrEmpty(prefix))
        {
            return;
        }

        var listRequest = new ListObjectsV2Request
        {
            BucketName = casted.BucketName,
            Prefix = prefix + "/",
        };

        ListObjectsV2Response response;
        do
        {
            response = await s3Client.ListObjectsV2Async(listRequest);

            if (response.S3Objects.Any())
            {
                var deleteRequest = new DeleteObjectsRequest
                {
                    BucketName = casted.BucketName,
                    Objects = response.S3Objects.Select(o => new KeyVersion { Key = o.Key }).ToList(),
                };

                await s3Client.DeleteObjectsAsync(deleteRequest);
            }

            listRequest.ContinuationToken = response.NextContinuationToken;
        } while (response.IsTruncated);
    }

    public async Task DeleteFile(IDataLakeJobData configuration, string fileName)
    {
        var s3Client = GetS3Client(configuration);
        var casted = CastJobData(configuration);
        var key = GetObjectKey(configuration, fileName);

        var response = await s3Client.DeleteObjectAsync(casted.BucketName, key);

        if ((int)response.HttpStatusCode < 200 || (int)response.HttpStatusCode >= 300)
        {
            throw new Exception($"S3 DeleteObject returned {response.HttpStatusCode}");
        }
    }

    public Task<bool> FileInPathExists(IDataLakeJobData configuration, string fileName)
    {
        return FileInPathExists(configuration, fileName, string.Empty);
    }

    public async Task<bool> FileInPathExists(IDataLakeJobData configuration, string fileName, string subDirectory)
    {
        var s3Client = GetS3Client(configuration);
        var casted = CastJobData(configuration);
        var prefix = GetPrefix(configuration, subDirectory);
        var key = string.IsNullOrEmpty(prefix) ? fileName : $"{prefix}/{fileName}";

        try
        {
            await s3Client.GetObjectMetadataAsync(casted.BucketName, key);
            return true;
        }
        catch (AmazonS3Exception ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            return false;
        }
    }

    public Task<IStorageFileProperties> GetFilePathProperties(IDataLakeJobData configuration, string fileName)
    {
        return GetFilePathProperties(configuration, fileName, string.Empty);
    }

    public async Task<IStorageFileProperties> GetFilePathProperties(IDataLakeJobData configuration, string fileName, string subDirectory)
    {
        var s3Client = GetS3Client(configuration);
        var casted = CastJobData(configuration);
        var prefix = GetPrefix(configuration, subDirectory);
        var key = string.IsNullOrEmpty(prefix) ? fileName : $"{prefix}/{fileName}";

        try
        {
            var metadata = await s3Client.GetObjectMetadataAsync(casted.BucketName, key);
            var metadataDict = new Dictionary<string, string>();
            foreach (var metaKey in metadata.Metadata.Keys)
            {
                metadataDict[metaKey.Replace("x-amz-meta-", string.Empty)] = metadata.Metadata[metaKey];
            }

            return new AmazonS3StorageFileProperties(
                metadataDict,
                metadata.LastModified,
                metadata.ContentLength);
        }
        catch (AmazonS3Exception ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            return null;
        }
    }

    public Task<bool> DirectoryExists(IDataLakeJobData configuration)
    {
        return DirectoryExists(configuration, string.Empty);
    }

    public async Task<bool> DirectoryExists(IDataLakeJobData configuration, string subDirectory)
    {
        var s3Client = GetS3Client(configuration);
        var casted = CastJobData(configuration);
        var prefix = GetPrefix(configuration, subDirectory);

        var listRequest = new ListObjectsV2Request
        {
            BucketName = casted.BucketName,
            Prefix = string.IsNullOrEmpty(prefix) ? null : prefix + "/",
            MaxKeys = 1,
        };

        try
        {
            var response = await s3Client.ListObjectsV2Async(listRequest);
            return response.S3Objects.Any();
        }
        catch (AmazonS3Exception ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            return false;
        }
    }

    public async Task<IEnumerable<IConnectorContainer>> GetFilesInDirectory(IDataLakeJobData configuration, string subDirectory = null)
    {
        var s3Client = GetS3Client(configuration);
        var casted = CastJobData(configuration);
        var prefix = GetPrefix(configuration, subDirectory);

        var listRequest = new ListObjectsV2Request
        {
            BucketName = casted.BucketName,
            Prefix = string.IsNullOrEmpty(prefix) ? null : prefix + "/",
        };

        var result = new List<IConnectorContainer>();
        ListObjectsV2Response response;
        do
        {
            response = await s3Client.ListObjectsV2Async(listRequest);

            foreach (var s3Object in response.S3Objects)
            {
                if (!s3Object.Key.EndsWith("/"))
                {
                    var fileName = Path.GetFileName(s3Object.Key);
                    result.Add(new DataLakeContainer
                    {
                        Name = fileName,
                        FullyQualifiedName = $"s3://{casted.BucketName}/{s3Object.Key}",
                    });
                }
            }

            listRequest.ContinuationToken = response.NextContinuationToken;
        } while (response.IsTruncated);

        return result;
    }
}
