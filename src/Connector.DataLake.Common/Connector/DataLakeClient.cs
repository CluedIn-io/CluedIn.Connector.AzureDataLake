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

using Parquet.Schema;

namespace CluedIn.Connector.DataLake.Common.Connector;

internal class DataLakeClient : IStorageClient
{
    private readonly ILogger<DataLakeClient> _logger;
    private readonly IStorageConfiguration _storageConfiguration;

    public DataLakeClient(ILogger<DataLakeClient> logger, IStorageConfiguration storageConfiguration)
    {
        _logger = logger;
        _storageConfiguration = storageConfiguration;
    }

    public async Task DeleteDirectory(DirectoryPath directoryPath)
    {
        var directoryClient = await GetDirectoryClientAsync(directoryPath, createIfNotExists: false);

        await directoryClient?.DeleteIfExistsAsync();
    }

    public async Task DeleteFile(FilePath filePath)
    {
        var fileClient = await GetFileClientAsync(filePath, createDirectoryIfNotExists: false);

        await fileClient.DeleteIfExistsAsync();
    }

    public async Task<bool> DirectoryExists(DirectoryPath directoryPath)
    {
        var directoryClient = await GetDirectoryClientAsync(directoryPath, createIfNotExists: false);

        return directoryClient != null;
    }

    public async Task<bool> FileExists(FilePath filePath)
    {
        var fileClient = await GetFileClientAsync(filePath, createDirectoryIfNotExists: false);
        if (fileClient == null)
        {
            return false;
        }

        return await fileClient.ExistsAsync();
    }

    public Task<DirectoryPath> GetBaseDirectoryPath()
    {
        return Task.FromResult(new DirectoryPath(_storageConfiguration.RootDirectoryPath));
    }

    public async Task<IStorageFileClient> GetFileClient(FilePath filePath)
    {
        var directoryClient = await GetDirectoryClientAsync(filePath.DirectoryPath, createIfNotExists: false);
        return new FileClient(filePath,directoryClient.GetFileClient(filePath.Name));
    }

    public async Task<FileMetadata> GetFileMetadata(FilePath filePath)
    {
        var fileClient = await GetFileClientAsync(filePath, createDirectoryIfNotExists: false);

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

    public async Task<IEnumerable<FullyQualifiedFilePath>> GetFilesInDirectory(DirectoryPath directoryPath)
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

    public async Task SaveData(FilePath filePath, string content, string contentType)
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

    public async Task VerifyConnection()
    {
        await EnsureDataLakeDirectoryExist(await GetBaseDirectoryPath());
    }

    public async Task CreateDirectoryIfNotExists(DirectoryPath directoryPath)
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
        var rootDirectoryPath = _storageConfiguration.RootDirectoryPath;

        if (!targetPath.StartsWith(rootDirectoryPath))
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
        var dataLakeServiceClient = GetDataLakeServiceClient();
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

    protected virtual DataLakeServiceClient GetDataLakeServiceClient()
    {
        switch (_storageConfiguration)
        {
            case IAzureSharedKeyCredentialConfiguration sharedKeyCredential:
                return new DataLakeServiceClient(
                            new Uri(sharedKeyCredential.StorageUri),
                            new StorageSharedKeyCredential(sharedKeyCredential.AccountName, sharedKeyCredential.AccountKey));
            case IAzureServicePrincipalCredentialConfiguration servicePrincipalCredential:
                {
                    var dataLakeServiceClient = new DataLakeServiceClient(
                        new Uri(servicePrincipalCredential.StorageUri),
                        new ClientSecretCredential(servicePrincipalCredential.TenantId, servicePrincipalCredential.ClientId, servicePrincipalCredential.ClientSecret));
                    return dataLakeServiceClient;
                }
            default:
                throw new NotSupportedException($"Unable to create datalake service client from type {_storageConfiguration.GetType()}");
        }
    }
    protected static TStorageConfiguration CastConfiguration<TStorageConfiguration>(IStorageConfiguration configuration) where TStorageConfiguration : class, IStorageConfiguration
    {
        if (configuration is not TStorageConfiguration castedJobData)
        {
            throw new ApplicationException($"Provided configuration is not of expected type '{typeof(TStorageConfiguration)}'. It is '{configuration.GetType()}'.");
        }

        return castedJobData;
    }

    internal class FileClient : IStorageFileClient
    {
        private readonly FilePath _filePath;
        private DataLakeFileClient _fileClient;

        public FileClient(FilePath filePath, DataLakeFileClient fileClient)
        {
            _filePath = filePath ?? throw new ArgumentNullException(nameof(filePath));
            _fileClient = fileClient ?? throw new ArgumentNullException(nameof(fileClient));
        }

        public Uri Uri => _fileClient.Uri;

        public string Path => _fileClient.Path;

        public async Task DeleteAsync()
        {
            await _fileClient.DeleteAsync();
        }

        public async Task DeleteIfExistsAsync()
        {
            await _fileClient?.DeleteIfExistsAsync();
        }

        public async Task<bool> ExistsAsync()
        {
            return await _fileClient.ExistsAsync();
        }

        public async Task<Stream> OpenWriteAsync(bool overwrite)
        {
            await using var outputStream = await _fileClient.OpenWriteAsync(overwrite);
            return new DataLakeBufferedWriteStream(outputStream);
        }

        public async Task RenameAsync(string value)
        {
            await _fileClient.RenameAsync(value);
        }

        public async Task SetMetadataAsync(Dictionary<string, string> metadata)
        {
            await _fileClient.SetMetadataAsync(metadata);
        }
    }
}
