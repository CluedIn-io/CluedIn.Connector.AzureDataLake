//using System;
//using System.Collections.Generic;
//using System.IO;
//using System.Threading;
//using System.Threading.Tasks;

//using Azure;
//using Azure.Storage.Files.DataLake;

//using CluedIn.Core.Connectors;

//namespace CluedIn.Connector.DataLake.Common.Connector;

//public class ADLSFileClient : IDataLakeFileClient
//{
//    /// <summary>
//    /// Opens a stream for writing to the Data Lake file, safely handling the case where
//    /// OpenWriteAsync fails with a 404 because the file does not yet exist.
//    /// This pattern is necessary due to the DataLake SDK's internal GetProperties check.
//    /// </summary>
//    /// <param name="fileClient">The DataLakeFileClient instance.</param>
//    /// <param name="overwrite">Whether to overwrite the file if it exists.</param>
//    /// <param name="options">Optional parameters for the write operation.</param>
//    /// <param name="cancellationToken">Cancellation token.</param>
//    /// <returns>A writable Stream for the file.</returns>
//    public async Task<Stream> OpenWriteAsync(bool overwrite)
//    {
//        try
//        {
//            // 1. Attempt the standard OpenWriteAsync call.
//            return await fileClient.OpenWriteAsync(overwrite, options, cancellationToken);
//        }
//        catch (RequestFailedException ex) when (ex.Status == 404)
//        {
//            // This is the specific error when GetPropertiesAsync fails because the file doesn't exist.

//            await fileClient.CreateAsync(cancellationToken: cancellationToken);

//            // Retry the OpenWriteAsync call. This time, GetPropertiesAsync should succeed
//            // (because the file exists now), and the stream will be opened.
//            // We pass the original 'overwrite' flag here to handle the stream logic correctly.
//            return await fileClient.OpenWriteAsync(overwrite, options, cancellationToken);
//        }
//    }
//}

//public abstract class DataLakeClient : IDataLakeClient
//{
//    public Task<IDataLakeDirectoryClient> EnsureDataLakeDirectoryExist(IDataLakeJobData configuration)
//    {
//        return EnsureDataLakeDirectoryExist(configuration, string.Empty);
//    }

//    public async Task<IDataLakeDirectoryClient> EnsureDataLakeDirectoryExist(IDataLakeJobData configuration, string subDirectory)
//    {
//        var fileSystemClient = await GetFileSystemClientAsync(configuration, ensureExists: true);
//        var directoryClient = await GetDirectoryClientAsync(configuration, fileSystemClient, subDirectory, ensureExists: true);

//        return directoryClient;
//    }

//    public async Task DeleteDirectory(IDataLakeJobData configuration, string subDirectory)
//    {
//        var fileSystemClient = await GetFileSystemClientAsync(configuration, ensureExists: true);
//        var directoryClient = await GetDirectoryClientAsync(configuration, fileSystemClient, subDirectory, ensureExists: true);

//        await directoryClient.DeleteIfExistsAsync();
//    }

//    public async Task SaveData(IDataLakeJobData configuration, string content, string fileName, string contentType)
//    {
//        using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(content));
//        var directoryClient = await EnsureDataLakeDirectoryExist(configuration);

//        var dataLakeFileClient = directoryClient.GetFileClient(fileName);
//        var options = new DataLakeFileUploadOptions
//        {
//            HttpHeaders = new PathHttpHeaders { ContentType = contentType }
//        };

//        var response = await dataLakeFileClient.UploadAsync(stream, options);

//        if (response?.Value == null)
//        {
//            throw new Exception($"{nameof(IDataLakeFileClient)}.{nameof(IDataLakeFileClient.UploadAsync)} did not return a valid path");
//        }
//    }

//    public async Task DeleteFile(IDataLakeJobData configuration, string fileName)
//    {
//        var directoryClient = await EnsureDataLakeDirectoryExist(configuration);
//        var dataLakeFileClient = directoryClient.GetFileClient(fileName);

//        var response = await dataLakeFileClient.DeleteAsync();

//        if (response.Status != 200)
//        {
//            throw new Exception($"{nameof(IDataLakeFileClient)}.{nameof(IDataLakeFileClient.DeleteAsync)} returned {response.Status}");
//        }
//    }

//    protected abstract DataLakeServiceClient GetDataLakeServiceClient(IDataLakeJobData configuration);

//    protected static TJobData CastJobData<TJobData>(IDataLakeJobData jobData) where TJobData : class, IDataLakeJobData
//    {
//        if (jobData is not TJobData castedJobData)
//        {
//            throw new ApplicationException($"Provided job data is not of expected type '{typeof(TJobData)}'. It is '{jobData.GetType()}'.");
//        }

//        return castedJobData;
//    }

//    public Task<bool> FileInPathExists(IDataLakeJobData configuration, string fileName)
//    {
//        return FileInPathExists(configuration, fileName, string.Empty);
//    }

//    public async Task<bool> FileInPathExists(IDataLakeJobData configuration, string fileName, string subDirectory)
//    {
//        var fileSystemClient = await GetFileSystemClientAsync(configuration, ensureExists: false);
//        if (!await fileSystemClient.ExistsAsync())
//        {
//            return false;
//        }

//        var directoryClient = await GetDirectoryClientAsync(configuration, fileSystemClient, subDirectory, ensureExists: false);
//        if (!await directoryClient.ExistsAsync())
//        {
//            return false;
//        }

//        var dataLakeFileClient = directoryClient.GetFileClient(fileName);
//        return await dataLakeFileClient.ExistsAsync();
//    }

//    public Task<bool> DirectoryExists(IDataLakeJobData configuration)
//    {
//        return DirectoryExists(configuration, string.Empty);
//    }

//    public async Task<bool> DirectoryExists(IDataLakeJobData configuration, string subDirectory)
//    {
//        var fileSystemClient = await GetFileSystemClientAsync(configuration, ensureExists: false);
//        if (!await fileSystemClient.ExistsAsync())
//        {
//            return false;
//        }

//        var directoryClient = await GetDirectoryClientAsync(configuration, fileSystemClient, subDirectory, ensureExists: false);
//        return await directoryClient.ExistsAsync();
//    }

//    public Task<FileMetadata> GetFileMetadata(IDataLakeJobData configuration, string fileName)
//    {
//        return GetFilePathProperties(configuration, fileName, string.Empty);
//    }

//    public async Task<FileMetadata> GetFileMetadata(IDataLakeJobData configuration, string fileName, string subDirectory)
//    {
//        var fileSystemClient = await GetFileSystemClientAsync(configuration, ensureExists: false);

//        if (!await fileSystemClient.ExistsAsync())
//        {
//            return null;
//        }

//        var directoryClient = await GetDirectoryClientAsync(configuration, fileSystemClient, subDirectory, ensureExists: false);
//        if (!await directoryClient.ExistsAsync())
//        {
//            return null;
//        }

//        var dataLakeFileClient = directoryClient.GetFileClient(fileName);
//        if (!await dataLakeFileClient.ExistsAsync())
//        {
//            return null;
//        }

//        var properties = await dataLakeFileClient.GetPropertiesAsync();

//        if (properties == null)
//        {
//            return null;
//        }

//        return new FileMetadata(properties.Value.Metadata);
//    }

//    private async Task<DataLakeDirectoryClient> GetDirectoryClientAsync(
//        IDataLakeJobData configuration,
//        DataLakeFileSystemClient fileSystemClient,
//        string subDirectory,
//        bool ensureExists)
//    {
//        var directory = configuration.RootDirectoryPath;
//        var directoryClient = fileSystemClient.GetDirectoryClient(directory);
//        if (string.IsNullOrWhiteSpace(subDirectory))
//        {
//            return directoryClient;
//        }

//        directoryClient = directoryClient.GetSubDirectoryClient(subDirectory);

//        if (ensureExists && !await directoryClient.ExistsAsync())
//        {
//            directoryClient = await fileSystemClient.CreateDirectoryAsync(directoryClient.Path);
//        }

//        return directoryClient;
//    }

//    protected async Task<DataLakeFileSystemClient> GetFileSystemClientAsync(
//        IDataLakeJobData configuration,
//        bool ensureExists)
//    {
//        var dataLakeServiceClient = GetDataLakeServiceClient(configuration);
//        var fileSystemName = configuration.FileSystemName;
//        var dataLakeFileSystemClient = dataLakeServiceClient.GetFileSystemClient(fileSystemName);
//        if (ensureExists && !await dataLakeFileSystemClient.ExistsAsync())
//        {
//            dataLakeFileSystemClient = await dataLakeServiceClient.CreateFileSystemAsync(fileSystemName);
//        }

//        return dataLakeFileSystemClient;
//    }

//    public async Task<IEnumerable<IConnectorContainer>> GetFilesInDirectory(IDataLakeJobData configuration, string subDirectory = null)
//    {
//        var serviceClient = GetDataLakeServiceClient(configuration);
//        var fileSystemName = configuration.FileSystemName;
//        var fileSystemClient = serviceClient.GetFileSystemClient(fileSystemName);

//        if (!await fileSystemClient.ExistsAsync())
//        {
//            return null;
//        }

//        var directory = configuration.RootDirectoryPath;
//        if (!string.IsNullOrEmpty(subDirectory))
//            directory = Path.Combine(directory, subDirectory);

//        var directoryClient = fileSystemClient.GetDirectoryClient(directory);
//        if (!await directoryClient.ExistsAsync())
//        {
//            return null;
//        }

//        var files = directoryClient.GetPathsAsync().GetAsyncEnumerator();
//        await files.MoveNextAsync();
//        var item = files.Current;

//        var result = new List<IConnectorContainer>();
//        while (item != null)
//        {
//            if (!item.IsDirectory.GetValueOrDefault())
//            {
//                result.Add(new DataLakeContainer
//                {
//                    Name = Path.GetFileName(item.Name),
//                    FullyQualifiedName = directoryClient.Uri.ToString() + "/" + Path.GetFileName(item.Name)
//                });
//            }

//            if (!await files.MoveNextAsync())
//            {
//                break;
//            }

//            item = files.Current;
//        }

//        return result;
//    }
//}
