using System;
using System.IO;
using System.Threading.Tasks;
using Azure.Storage.Files.DataLake;
using CluedIn.Connector.FileStorage.Common;

namespace CluedIn.Connector.DataLake.Common.Connector;

internal class DataLakeStorageFileClient : IStorageFileClient
{
    private readonly FilePath _filePath;
    private DataLakeFileClient _fileClient;

    public DataLakeStorageFileClient(FilePath filePath, DataLakeFileClient fileClient)
    {
        _filePath = filePath ?? throw new ArgumentNullException(nameof(filePath));
        _fileClient = fileClient ?? throw new ArgumentNullException(nameof(fileClient));
    }

    public Uri Uri => _fileClient.Uri;

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

    public async Task RenameAsync(FilePath targetPath)
    {
        await _fileClient.RenameAsync(targetPath.FullPath);
    }

    public async Task SetMetadataAsync(FileMetadata fileMetadata)
    {
        await _fileClient.SetMetadataAsync(fileMetadata.Metadata);
    }
}
