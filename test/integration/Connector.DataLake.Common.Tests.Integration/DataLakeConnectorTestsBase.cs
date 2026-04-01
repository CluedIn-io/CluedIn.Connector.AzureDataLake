using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Models;

using CluedIn.Connector.FileStorage.Common;
using CluedIn.Connector.FileStorage.Common.Connector;
using CluedIn.Core;

using Newtonsoft.Json;

using Xunit.Abstractions;

namespace CluedIn.Connector.DataLake.Common.Tests.Integration;

public abstract partial class DataLakeConnectorTestsBase<TConnector, TClientFactory, TConfigurationConstants>
    : StorageConnectorTestsBase<TConnector, TClientFactory, TConfigurationConstants>
    where TConnector : StorageConnectorBase
    where TClientFactory : class, IStorageFactory
    where TConfigurationConstants : class, IStorageConfigurationConstants
{
    public DataLakeConnectorTestsBase(ITestOutputHelper testOutputHelper) : base(testOutputHelper)
    {
    }

    private protected abstract DataLakeServiceClient GetDataLakeClient(SetupContainerResult setupContainerResult);

    private protected override async Task CleanUpExportedFile(SetupContainerResult setupContainerResult, ExportedFilePath filePath)
    {
        var directoryClient = GetDataLakeDirectoryClient(setupContainerResult);
        await directoryClient.DeleteIfExistsAsync();
        await WaitForFileToBeDeleted(setupContainerResult, filePath);
    }

    private protected override async Task CleanUpAfterImmediateOutputTest(SetupContainerResult setupContainerResult)
    {
        var client = GetDataLakeClient(setupContainerResult);
        var fileSystemName = GetFileSystemName(setupContainerResult);
        var directoryName = GetDirectoryName(setupContainerResult);
        if (!IsFixedFileSystem)
        {
            await DeleteFileSystem(client, fileSystemName);
        }
        else
        {
            await DeleteDirectory(client, fileSystemName, directoryName);
        }
    }

    private protected override async Task CleanUpAfterFileOutputTest(SetupContainerResult setupContainerResult)
    {
        await CleanUpAfterImmediateOutputTest(setupContainerResult);
        var streamModel = setupContainerResult.StreamModel;
        var jobData = setupContainerResult.StorageConfiguration;
        await DeleteTable(streamModel.Id, jobData.StreamCacheConnectionString);
    }

    private protected override async Task WaitForFileToBeDeleted(SetupContainerResult setupContainerResult, ExportedFilePath path)
    {
        var d = DateTime.Now;
        while (true)
        {
            if (DateTime.Now > d.AddSeconds(30))
            {
                throw new TimeoutException("Timeout waiting for file to be deleted");
            }

            var client = GetDataLakeClient(setupContainerResult);
            var fileSystemName = GetFileSystemName(setupContainerResult);
            if (!IsFixedFileSystem)
            {
                if (client.GetFileSystems().All(fs => fs.Name != fileSystemName))
                {
                    continue;
                }
            }

            var fileSystemClient = client.GetFileSystemClient(fileSystemName);
            var directoryName = GetDirectoryName(setupContainerResult);

            var directoryClient = fileSystemClient.GetDirectoryClient(directoryName);
            if (!await directoryClient.ExistsAsync())
            {
                break;
            }

            var paths = directoryClient.GetPaths(recursive: true)
                .Where(p => p.IsDirectory == false)
                .Where(p => p.Name.Contains(directoryName))
                .Select(p => p.Name)
                .ToArray();

            if (paths.Contains(path.Name))
            {
                await Task.Delay(1000);
                continue;
            }

            break;
        }
    }

    private protected override async Task<Stream> GetContents(
        SetupContainerResult setupContainerResult,
        ExportedFilePath path)
    {
        var fileClient = GetFileClient(setupContainerResult, path);
        var memoryStream = new MemoryStream();
        var fileStream = await fileClient.OpenReadAsync();
        await fileStream.CopyToAsync(memoryStream);
        memoryStream.Position = 0;
        return memoryStream;
    }

    private protected virtual string GetFileSystemName(SetupContainerResult setupContainerResult)
    {
        return (setupContainerResult.StorageConfiguration as IDataLakeStorageConfiguration).FileSystemName;
    }

    private protected abstract string GetDirectoryName(SetupContainerResult setupContainerResult);

    private protected DataLakeFileClient GetFileClient(SetupContainerResult setupContainerResult, ExportedFilePath path)
    {
        var client = GetDataLakeClient(setupContainerResult);
        var fileSystemName = GetFileSystemName(setupContainerResult);
        var fsClient = client.GetFileSystemClient(fileSystemName);
        var directoryClient = fsClient.GetDirectoryClient(path.DirectoryPath);
        var fileClient = directoryClient.GetFileClient(path.Name);
        return fileClient;
    }

    private protected virtual DataLakeDirectoryClient GetDataLakeDirectoryClient(SetupContainerResult setupContainerResult)
    {
        var client = GetDataLakeClient(setupContainerResult);
        var fileSystemName = GetFileSystemName(setupContainerResult);
        var fsClient = client.GetFileSystemClient(fileSystemName);
        var directoryName = GetDirectoryName(setupContainerResult);
        return fsClient.GetDirectoryClient(directoryName);
    }

    protected static async Task DeleteFileSystem(DataLakeServiceClient client, string fileSystemName)
    {
        if (client.GetFileSystems().All(fs => fs.Name != fileSystemName))
        {
            return;
        }
        var fsClient = client.GetFileSystemClient(fileSystemName);
        await fsClient.DeleteIfExistsAsync();
    }

    protected static async Task DeleteDirectory(DataLakeServiceClient client, string fileSystemName, string directoryName)
    {
        var fsClient = client.GetFileSystemClient(fileSystemName);
        var directoryClient = fsClient.GetDirectoryClient(directoryName);

        await directoryClient.DeleteIfExistsAsync();
    }

    private protected override async Task<ExportedFilePath> WaitForFileToBeCreated(
        SetupContainerResult setupContainerResult,
        Func<IList<ExportedFilePath>, IList<ExportedFilePath>> filterPaths = null,
        Func<SetupContainerResult, string> getDirectoryName = null)
    {
        ExportedFilePath path;
        var d = DateTime.Now;
        while (true)
        {
            if (DateTime.Now > d.AddSeconds(30))
            {
                throw new TimeoutException();
            }

            var client = GetDataLakeClient(setupContainerResult);
            var fileSystemName = (setupContainerResult.StorageConfiguration as IDataLakeStorageConfiguration).FileSystemName;
            if (!IsFixedFileSystem)
            {
                if (client.GetFileSystems().All(fs => fs.Name != fileSystemName))
                {
                    continue;
                }
            }

            var fileSystemClient = client.GetFileSystemClient(fileSystemName);

            if (!await fileSystemClient.ExistsAsync())
            {
                continue;
            }
            var directoryName = getDirectoryName == null ? GetDirectoryName(setupContainerResult) : getDirectoryName(setupContainerResult);
            var directoryClient = fileSystemClient.GetDirectoryClient(directoryName);
            if (!await directoryClient.ExistsAsync())
            {
                continue;
            }

            IList<PathItem> dataLakePaths = [..directoryClient.GetPaths(recursive: true)
                .Where(p => p.IsDirectory == false)
                .Where(p => p.Name.Contains(directoryName))];

            var paths = dataLakePaths.Select(p =>
            {
                var fileName = Path.GetFileName(p.Name);
                var directoryPath = p.Name[0..^fileName.Length];
                var trimmedDirectoryPath = directoryPath.EndsWith("/") ? directoryPath[0..^1] : directoryPath;
                return new ExportedFilePath(fileName, trimmedDirectoryPath, p.ContentLength);
            }).ToList();

            paths = filterPaths == null ? paths : filterPaths(paths).ToList();

            if (paths.Count == 0)
            {
                await Task.Delay(1000);
                continue;
            }

            if (paths.Count > 1)
            {
                TestOutputHelper.WriteLine("Found multiple paths: {0}.", JsonConvert.SerializeObject(paths, Formatting.Indented));
            }
            path = paths.Single();

            if (path.ContentLength > 0)
            {
                break;
            }
        }
        return path;
    }

    private protected override async Task<DateTimeOffset> GetFileDataTime(ExecuteExportArg executeExportArg, ExportedFilePath path)
    {
        var fileClient = GetFileClient(executeExportArg.SetupContainerResult, path);
        var fileProperties = await fileClient.GetPropertiesAsync();
        var fileMetadata = fileProperties.Value.Metadata;
        var fileDataTime = fileMetadata["DataTime"];

        return DateTimeOffset.Parse(fileDataTime);
    }

    protected virtual bool IsFixedFileSystem => false;

}
