using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Azure;
using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Models;
using CluedIn.Core.Connectors;
using Serilog;

namespace CluedIn.Connector.DataLake.Common.Connector
{
    public abstract class DataLakeClient : IDataLakeClient
    {
        public Task<DataLakeDirectoryClient> EnsureDataLakeDirectoryExist(IDataLakeJobData configuration)
        {
            return EnsureDataLakeDirectoryExist(configuration, string.Empty);
        }

        public async Task<DataLakeDirectoryClient> EnsureDataLakeDirectoryExist(IDataLakeJobData configuration, string subDirectory)
        {
            var fileSystemClient = await GetFileSystemClientAsync(configuration, ensureExists: true);
            var directoryClient = await GetDirectoryClientAsync(configuration, fileSystemClient, subDirectory, ensureExists: true);

            return directoryClient;
        }

        public async Task DeleteDirectory(IDataLakeJobData configuration, string subDirectory)
        {
            var fileSystemClient = await GetFileSystemClientAsync(configuration, ensureExists: true);
            var directoryClient = await GetDirectoryClientAsync(configuration, fileSystemClient, subDirectory, ensureExists: true);

            await directoryClient.DeleteIfExistsAsync();
        }

        public async Task SaveData(IDataLakeJobData configuration, string content, string fileName, string contentType)
        {
            using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(content));
            var directoryClient = await EnsureDataLakeDirectoryExist(configuration);

            var dataLakeFileClient = directoryClient.GetFileClient(fileName);
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

        public async Task DeleteFile(IDataLakeJobData configuration, string fileName)
        {
            var directoryClient = await EnsureDataLakeDirectoryExist(configuration);
            var dataLakeFileClient = directoryClient.GetFileClient(fileName);

            var response = await dataLakeFileClient.DeleteAsync();

            if (response.Status != 200)
            {
                throw new Exception($"{nameof(DataLakeFileClient)}.{nameof(DataLakeFileClient.DeleteAsync)} returned {response.Status}");
            }
        }

        protected abstract DataLakeServiceClient GetDataLakeServiceClient(IDataLakeJobData configuration);

        protected static TJobData CastJobData<TJobData>(IDataLakeJobData jobData) where TJobData : class, IDataLakeJobData
        {
            if (jobData is not TJobData castedJobData)
            {
                throw new ApplicationException($"Provided job data is not of expected type '{typeof(TJobData)}'. It is '{jobData.GetType()}'.");
            }

            return castedJobData;
        }

        public Task<bool> FileInPathExists(IDataLakeJobData configuration, string fileName)
        {
            return FileInPathExists(configuration, fileName, string.Empty);
        }

        public async Task<bool> FileInPathExists(IDataLakeJobData configuration, string fileName, string subDirectory)
        {
            var fileSystemClient = await GetFileSystemClientAsync(configuration, ensureExists: false);
            if (!await fileSystemClient.ExistsAsync())
            {
                return false;
            }

            var directoryClient = await GetDirectoryClientAsync(configuration, fileSystemClient, subDirectory, ensureExists: false);
            if (!await directoryClient.ExistsAsync())
            {
                return false;
            }

            var dataLakeFileClient = directoryClient.GetFileClient(fileName);
            return await dataLakeFileClient.ExistsAsync();
        }

        public Task<bool> DirectoryExists(IDataLakeJobData configuration)
        {
            return DirectoryExists(configuration, string.Empty);
        }

        public async Task<bool> DirectoryExists(IDataLakeJobData configuration, string subDirectory)
        {
            var fileSystemClient = await GetFileSystemClientAsync(configuration, ensureExists: false);
            if (!await fileSystemClient.ExistsAsync())
            {
                return false;
            }

            var directoryClient = await GetDirectoryClientAsync(configuration, fileSystemClient, subDirectory, ensureExists: false);
            return await directoryClient.ExistsAsync();
        }

        public Task<PathProperties> GetFilePathProperties(IDataLakeJobData configuration, string fileName)
        {
            return GetFilePathProperties(configuration, fileName, string.Empty);
        }

        public async Task<PathProperties> GetFilePathProperties(IDataLakeJobData configuration, string fileName, string subDirectory)
        {
            var fileSystemClient = await GetFileSystemClientAsync(configuration, ensureExists: false);

            if (!await fileSystemClient.ExistsAsync())
            {
                return null;
            }

            var directoryClient = await GetDirectoryClientAsync(configuration, fileSystemClient, subDirectory, ensureExists: false);
            if (!await directoryClient.ExistsAsync())
            {
                return null;
            }

            var dataLakeFileClient = directoryClient.GetFileClient(fileName);
            if (!await dataLakeFileClient.ExistsAsync())
            {
                return null;
            }

            return await dataLakeFileClient.GetPropertiesAsync();
        }

        private async Task<DataLakeDirectoryClient> GetDirectoryClientAsync(
            IDataLakeJobData configuration,
            DataLakeFileSystemClient fileSystemClient,
            string subDirectory,
            bool ensureExists)
        {
            // ... (Initialization and logging remain the same)
            var directory = configuration.RootDirectoryPath;
            var directoryClient = fileSystemClient.GetDirectoryClient(directory);

            // Log the initial directory calculation
            Log.Logger.Information("Requested directory path relative to file system: {RootDirectory} with subdirectory {SubDirectory}.",
                directory, subDirectory);

            if (!string.IsNullOrWhiteSpace(subDirectory))
            {
                directoryClient = directoryClient.GetSubDirectoryClient(subDirectory);
            }

            // --- START OF REPLACEMENT LOGIC ---
            if (ensureExists)
            {
                var fullRelativePath = directoryClient.Path;
                Log.Logger.Information("Starting recursive path creation for full path: {FullPath}", fullRelativePath);

                if (!string.IsNullOrWhiteSpace(fullRelativePath))
                {
                    var pathSegments = fullRelativePath.Split('/');
                    var currentPath = "";

                    foreach (var segment in pathSegments)
                    {
                        if (string.IsNullOrWhiteSpace(segment))
                            continue;

                        currentPath = string.IsNullOrEmpty(currentPath) ? segment : $"{currentPath}/{segment}";
                        var segmentClient = fileSystemClient.GetDirectoryClient(currentPath);

                        Log.Logger.Information("Checking/creating directory segment: {CurrentPath}", currentPath);

                        var response = await segmentClient.CreateIfNotExistsAsync();
                        var rawResponse = response.GetRawResponse();
                        var statusCode = rawResponse.Status;

                        if (statusCode == 201)
                        {
                            Log.Logger.Information("Successfully CREATED directory segment: {CurrentPath}", currentPath);
                        }
                        else if (statusCode == 409) // Most common "already exists" for this operation
                        {
                            Log.Logger.Information("Directory segment already exists (409 Conflict): {CurrentPath}", currentPath);
                        }
                        else if (statusCode == 400 &&
                                 rawResponse.Headers.TryGetValue("x-ms-error-code", out var errorCode) &&
                                 errorCode == "OperationNotAllowedOnThePath")
                        {
                            // This specifically indicates the directory *is* there, but the operation 
                            // (which includes setting default properties) failed because of an existing state.
                            Log.Logger.Information("OperationNotAllowedOnThePath: Path exists or is immutable: {CurrentPath}", currentPath);
                        }
                        else if (statusCode == 200) // Less common "already exists" (as discussed)
                        {
                            Log.Logger.Information("Directory segment already exists (200 OK): {CurrentPath}", currentPath);
                        }
                        else
                        {
                            // All other unexpected failures (Auth, Internal Server Error, etc.)
                            Log.Logger.Error("Failed to ensure directory segment exists. Path: {CurrentPath}, Status Code: {StatusCode}",
                                currentPath, statusCode);

                            // Use RequestFailedException(rawResponse) to capture the full error details
                            throw new RequestFailedException(rawResponse);
                        }
                    }
                }
                Log.Logger.Information("Recursive path creation complete. Full path is guaranteed to exist.");
            }
            // --- END OF REPLACEMENT LOGIC ---

            Log.Logger.Information("Returning directory client for URI: {DirectoryUri}", directoryClient.Uri);
            return directoryClient;
        }

        protected async Task<DataLakeFileSystemClient> GetFileSystemClientAsync(
            IDataLakeJobData configuration,
            bool ensureExists)
        {
            var dataLakeServiceClient = GetDataLakeServiceClient(configuration);
            var fileSystemName = configuration.FileSystemName;
            var dataLakeFileSystemClient = dataLakeServiceClient.GetFileSystemClient(fileSystemName);
            if (ensureExists && !await dataLakeFileSystemClient.ExistsAsync())
            {
                dataLakeFileSystemClient = await dataLakeServiceClient.CreateFileSystemAsync(fileSystemName);
            }

            return dataLakeFileSystemClient;
        }

        public async Task<IEnumerable<IConnectorContainer>> GetFilesInDirectory(IDataLakeJobData configuration, string subDirectory = null)
        {
            var serviceClient = GetDataLakeServiceClient(configuration);
            var fileSystemName = configuration.FileSystemName;
            var fileSystemClient = serviceClient.GetFileSystemClient(fileSystemName);

            if (!await fileSystemClient.ExistsAsync())
            {
                return null;
            }

            var directory = configuration.RootDirectoryPath;
            if (!string.IsNullOrEmpty(subDirectory))
                directory = Path.Combine(directory, subDirectory);

            var directoryClient = fileSystemClient.GetDirectoryClient(directory);
            if (!await directoryClient.ExistsAsync())
            {
                return null;
            }

            var files = directoryClient.GetPathsAsync().GetAsyncEnumerator();
            await files.MoveNextAsync();
            var item = files.Current;

            var result = new List<IConnectorContainer>();
            while (item != null)
            {
                if (!item.IsDirectory.GetValueOrDefault())
                {
                    result.Add(new DataLakeContainer
                    {
                        Name = Path.GetFileName(item.Name),
                        FullyQualifiedName = directoryClient.Uri.ToString() + "/" + Path.GetFileName(item.Name)
                    });
                }

                if (!await files.MoveNextAsync())
                {
                    break;
                }

                item = files.Current;
            }

            return result;
        }
    }
}
