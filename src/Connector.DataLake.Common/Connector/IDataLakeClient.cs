using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

using CluedIn.Core.Connectors;

namespace CluedIn.Connector.DataLake.Common.Connector;

//public interface IDataLakeDirectoryClient
//{
//    //IDataLakeFileClient GetFileClient(string path);
//}

public interface IDataLakeFileClient
{
    Uri Uri { get; }
    string Path { get; }

    Task DeleteAsync();
    Task DeleteIfExistsAsync();
    Task<bool> ExistsAsync();
    Task<Stream> OpenWriteAsync(bool overwrite);
    Task RenameAsync(string value);
    Task SetMetadataAsync(Dictionary<string, string> metadata);
}
public interface IDataLakeClient
{
    Task SaveData(DataLakeFilePath filePath, string content, string contentType);
    Task DeleteDirectory(DataLakeDirectoryPath directoryPath);
    Task DeleteFile(DataLakeFilePath filePath);
    Task<bool> FileExists(DataLakeFilePath filePath);
    Task<FileMetadata> GetFileMetadata(DataLakeFilePath filePath);
    Task<bool> DirectoryExists(DataLakeDirectoryPath directory);
    Task<IEnumerable<DataLakeFilePath>> GetFilesInDirectory(DataLakeDirectoryPath directoryPath);
    Task<IDataLakeFileClient> GetFileClient(DataLakeFilePath directoryPath);
}

public record FileMetadata(IDictionary<string, string> Metadata);
public record DataLakeDirectoryPath(string Path);
public record DataLakeFilePath(string Name, DataLakeDirectoryPath Directory);
