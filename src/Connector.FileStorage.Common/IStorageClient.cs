using System.Collections.Generic;
using System.Threading.Tasks;

namespace CluedIn.Connector.FileStorage.Common;
public interface IStorageClient
{
    Task<DirectoryPath> GetBaseDirectoryPath();

    Task SaveData(FilePath filePath, string content, string contentType);

    Task DeleteDirectory(DirectoryPath directoryPath);

    Task DeleteFile(FilePath filePath);

    Task<bool> FileExists(FilePath filePath);

    Task<FileMetadata> GetFileMetadata(FilePath filePath);

    Task<bool> DirectoryExists(DirectoryPath directory);

    Task<IEnumerable<FullyQualifiedFilePath>> GetFilesInDirectory(DirectoryPath directoryPath);

    Task<IStorageFileClient> GetFileClient(FilePath filePath);

    Task VerifyConnection();

    Task CreateDirectoryIfNotExists(DirectoryPath directoryPath);
}
