using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

using Azure.Identity;
using Azure.Storage;
using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Models;

using CluedIn.Connector.FileStorage.Common;

using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.DataLake.Common.Connector;

internal class DataLakeStorageClient : IStorageClient
{
    private readonly ILogger<DataLakeStorageClient> _logger;
    private readonly IDataLakeStorageConfiguration _storageConfiguration;
    private bool _disposed;

    public DataLakeStorageClient(ILogger<DataLakeStorageClient> logger, IDataLakeStorageConfiguration storageConfiguration)
    {
        _logger = logger;
        _storageConfiguration = storageConfiguration;
    }

    public async Task DeleteDirectoryAsync(DirectoryPath directoryPath)
    {
        var directoryClient = await GetDirectoryClientAsync(directoryPath, createIfNotExists: false);
        if (directoryClient == null)
        {
            return;
        }

        await directoryClient.DeleteIfExistsAsync();
    }

    public async Task DeleteFileAsync(FilePath filePath)
    {
        var fileClient = await GetFileClientAsync(filePath, createDirectoryIfNotExists: false);

        if (fileClient == null)
        {
            return;
        }

        await fileClient.DeleteIfExistsAsync();
    }

    public async Task<bool> DirectoryExistsAsync(DirectoryPath directoryPath)
    {
        var directoryClient = await GetDirectoryClientAsync(directoryPath, createIfNotExists: false);

        return directoryClient != null;
    }

    public async Task<bool> FileExistsAsync(FilePath filePath)
    {
        var fileClient = await GetFileClientAsync(filePath, createDirectoryIfNotExists: false);
        if (fileClient == null)
        {
            return false;
        }

        return await fileClient.ExistsAsync();
    }

    public Task<DirectoryPath> GetBaseDirectoryPathAsync()
    {
        return Task.FromResult(new DirectoryPath(_storageConfiguration.RootDirectoryPath));
    }

    public async Task<IStorageFileClient> GetFileClientAsync(FilePath filePath)
    {
        var directoryClient = await GetDirectoryClientAsync(filePath.DirectoryPath, createIfNotExists: false);
        if (directoryClient == null)
        {
            throw new InvalidOperationException("Directory client cannot be null when trying to get file client.");
        }
        return new DataLakeStorageFileClient(filePath,directoryClient.GetFileClient(filePath.Name));
    }

    public async Task<FileMetadata> GetFileMetadataAsync(FilePath filePath)
    {
        var fileClient = await GetFileClientAsync(filePath, createDirectoryIfNotExists: false);

        if (fileClient == null)
        {
            return null;
        }

        if (!await fileClient.ExistsAsync())
        {
            return null;
        }

        var properties = await fileClient.GetPropertiesAsync();

        if (properties == null)
        {
            return null;
        }

        return new FileMetadata(properties.Value.Metadata);
    }

    public async Task<IEnumerable<FullyQualifiedFilePath>> GetFilesInDirectoryAsync(DirectoryPath directoryPath)
    {
        var directoryClient = await GetDirectoryClientAsync(directoryPath, createIfNotExists: false);

        if (directoryClient == null)
        {
            return [];
        }

        var files = directoryClient.GetPathsAsync().GetAsyncEnumerator();
        await files.MoveNextAsync();
        var item = files.Current;

        var result = new List<FullyQualifiedFilePath>();
        while (item != null)
        {
            if (!item.IsDirectory.GetValueOrDefault())
            {
                result.Add(
                    new FullyQualifiedFilePath(
                        Path.GetFileName(item.Name),
                        directoryPath,
                        directoryClient.Uri.ToString() + "/" + Path.GetFileName(item.Name)));
            }

            if (!await files.MoveNextAsync())
            {
                break;
            }

            item = files.Current;
        }

        return result;
    }

    public async Task SaveDataAsync(FilePath filePath, string content, string contentType)
    {
        await using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(content));

        var dataLakeFileClient = await GetFileClientAsync(filePath, createDirectoryIfNotExists: true);
        var options = new DataLakeFileUploadOptions
        {
            HttpHeaders = new PathHttpHeaders { ContentType = contentType }
        };

        var response = await dataLakeFileClient.UploadAsync(stream, options);

        if (response?.Value == null)
        {
            throw new Exception($"{nameof(DataLakeFileClient)}.{nameof(DataLakeFileClient.UploadAsync)} did not return a valid path");
        }
    }

    public async Task VerifyConnectionAsync()
    {
        await EnsureDataLakeDirectoryExist(await GetBaseDirectoryPathAsync());
    }

    public async Task CreateDirectoryIfNotExistsAsync(DirectoryPath directoryPath)
    {
        await EnsureDataLakeDirectoryExist(directoryPath);
    }

    protected async Task<DataLakeFileClient> GetFileClientAsync(FilePath filePath, bool createDirectoryIfNotExists)
    {
        var directoryClient = await GetDirectoryClientAsync(filePath.DirectoryPath, createIfNotExists: createDirectoryIfNotExists);

        if (directoryClient == null)
        {
            if (createDirectoryIfNotExists)
            {
                throw new ApplicationException("Unable to access files in the directory because the directory does not exist.");
            }

            return null;
        }

        return directoryClient.GetFileClient(filePath.Name);
    }

    protected async Task<DataLakeDirectoryClient> GetDirectoryClientAsync(
        DirectoryPath directoryPath,
        bool createIfNotExists)
    {
        var filesystemClient = await GetFileSystemClientAsync(createIfNotExists);
        if (filesystemClient == null)
        {
            if (createIfNotExists)
            {
                throw new ApplicationException("Unable to access files in the directory because the file system does not exist.");
            }

            return null;
        }

        var directoryClient = await GetDirectoryClientAsync(filesystemClient, directoryPath, createIfNotExists);
        if (directoryClient == null)
        {
            if (createIfNotExists)
            {
                throw new ApplicationException("Unable to access files in the directory because the directory does not exist.");
            }

            return null;
        }

        return directoryClient;
    }

    protected async Task<DataLakeDirectoryClient> GetDirectoryClientAsync(
        DataLakeFileSystemClient fileSystemClient,
        DirectoryPath directoryPath,
        bool createIfNotExists)
    {
        var targetPath = directoryPath.Path;
        var rootDirectoryPath = await GetBaseDirectoryPathAsync();

        if (!targetPath.StartsWith(rootDirectoryPath.Path))
        {
            throw new ApplicationException("Unable to access files in the directory because the provided directory path is not under the root directory path specified in the job data.");
        }

        var directoryClient = fileSystemClient.GetDirectoryClient(targetPath);

        var exists = await directoryClient.ExistsAsync();
        if (!exists)
        {
            if (createIfNotExists)
            {
                return await fileSystemClient.CreateDirectoryAsync(directoryClient.Path);
            }

            return null;
        }

        return directoryClient;
    }

    protected async Task<DataLakeFileSystemClient> GetFileSystemClientAsync(bool createIfNotExists)
    {
        var dataLakeServiceClient = await GetDataLakeServiceClientAsync();
        var fileSystemName = _storageConfiguration.FileSystemName;
        var fileSystemClient = dataLakeServiceClient.GetFileSystemClient(fileSystemName);

        var exists = await fileSystemClient.ExistsAsync();
        if (!exists)
        {
            if (createIfNotExists)
            {
                return await dataLakeServiceClient.CreateFileSystemAsync(fileSystemName);
            }

            return null;
        }

        return fileSystemClient;
    }

    private async Task<DataLakeDirectoryClient> EnsureDataLakeDirectoryExist(DirectoryPath directoryPath)
    {
        return await GetDirectoryClientAsync(directoryPath, createIfNotExists: true);
    }

    protected virtual async Task<DataLakeServiceClient> GetDataLakeServiceClientAsync()
    {
        switch (_storageConfiguration)
        {
            case IAzureSharedKeyCredentialConfiguration sharedKeyCredential:
                return new DataLakeServiceClient(
                            new Uri(await GetStorageUrlAsync()),
                            new StorageSharedKeyCredential(sharedKeyCredential.AccountName, sharedKeyCredential.AccountKey));
            case IAzureServicePrincipalCredentialConfiguration servicePrincipalCredential:
                {
                    var dataLakeServiceClient = new DataLakeServiceClient(
                        new Uri(await GetStorageUrlAsync()),
                        new ClientSecretCredential(servicePrincipalCredential.TenantId, servicePrincipalCredential.ClientId, servicePrincipalCredential.ClientSecret));
                    return dataLakeServiceClient;
                }
            default:
                throw new NotSupportedException($"Unable to create datalake service client from type {_storageConfiguration.GetType()}");
        }
    }

    protected virtual Task<string> GetStorageUrlAsync()
    {
        return Task.FromResult(_storageConfiguration.StorageUri);
    }

    protected static TStorageConfiguration CastConfiguration<TStorageConfiguration>(IStorageConfiguration configuration) where TStorageConfiguration : class, IStorageConfiguration
    {
        if (configuration is not TStorageConfiguration castedJobData)
        {
            throw new ApplicationException($"Provided configuration is not of expected type '{typeof(TStorageConfiguration)}'. It is '{configuration.GetType()}'.");
        }

        return castedJobData;
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
        }

        _disposed = true;
    }
}
