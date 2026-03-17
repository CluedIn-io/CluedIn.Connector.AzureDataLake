using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

using CluedIn.Core.Connectors;

namespace CluedIn.Connector.DataLake.Common.Connector;

public interface IDataLakeDirectoryClient
{
    IDataLakeFileClient GetFileClient(string path);
}

public interface IDataLakeFileClient
{
    Uri Uri { get; }
    string Path { get; }

    Task DeleteIfExistsAsync();
    Task<bool> ExistsAsync();
    Task<Stream> OpenWriteAsync(bool v);
    Task RenameAsync(string value);
    Task SetMetadataAsync(Dictionary<string, string> dictionary);
}
public interface IDataLakeClient
{
    //Task<DataLakeDirectoryClient> EnsureDataLakeDirectoryExist(IDataLakeJobData configuration);
    //Task<DataLakeDirectoryClient> EnsureDataLakeDirectoryExist(IDataLakeJobData configuration, string subDirectory);
    Task<IDataLakeDirectoryClient> EnsureDataLakeDirectoryExist(IDataLakeJobData configuration);
    Task<IDataLakeDirectoryClient> EnsureDataLakeDirectoryExist(IDataLakeJobData configuration, string subDirectory);
    Task SaveData(IDataLakeJobData configuration, string content, string fileName, string contentType);
    Task DeleteDirectory(IDataLakeJobData configuration, string subDirectory);
    Task DeleteFile(IDataLakeJobData configuration, string fileName);
    Task<bool> FileInPathExists(IDataLakeJobData configuration, string fileName);
    Task<bool> FileInPathExists(IDataLakeJobData configuration, string fileName, string subDirectory);
    //Task<PathProperties> GetFilePathProperties(IDataLakeJobData configuration, string fileName);
    //Task<PathProperties> GetFilePathProperties(IDataLakeJobData configuration, string fileName, string subDirectory);
    Task<FileMetadata> GetFileMetadata(IDataLakeJobData configuration, string fileName);
    Task<FileMetadata> GetFileMetadata(IDataLakeJobData configuration, string fileName, string subDirectory);
    Task<bool> DirectoryExists(IDataLakeJobData configuration);
    Task<bool> DirectoryExists(IDataLakeJobData configuration, string subDirectory);
    Task<IEnumerable<IConnectorContainer>> GetFilesInDirectory(IDataLakeJobData configuration, string subDirectory = null);
}

public record FileMetadata(Dictionary<string, string> Metadata);
