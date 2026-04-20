using System.Collections.Generic;
using System.Threading.Tasks;

namespace CluedIn.Connector.FileStorage.Common;
public interface IStorageClient
{
    Task<DirectoryPath> GetBaseDirectoryPathAsync();

    Task SaveDataAsync(FilePath filePath, string content, string contentType);

    Task DeleteDirectoryAsync(DirectoryPath directoryPath);

    Task DeleteFileAsync(FilePath filePath);

    Task<bool> FileExistsAsync(FilePath filePath);

    Task<FileMetadata> GetFileMetadataAsync(FilePath filePath);

    Task<bool> DirectoryExistsAsync(DirectoryPath directory);

    Task<IEnumerable<FullyQualifiedFilePath>> GetFilesInDirectoryAsync(DirectoryPath directoryPath);

    Task<IStorageFileClient> GetFileClientAsync(FilePath filePath);

    Task VerifyConnectionAsync();

    Task CreateDirectoryIfNotExistsAsync(DirectoryPath directoryPath);
}
