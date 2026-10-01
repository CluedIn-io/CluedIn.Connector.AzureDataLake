using System;
using System.IO;
using System.Threading.Tasks;

using CluedIn.Connector.FileStorage.Common;

namespace CluedIn.Connector.Snowflake.Connector;

// No real bytes ever flow through this client - see SnowflakeStorageClient for why. It only
// exists to satisfy the base export pipeline's file-writing contract.
internal class SnowflakeStorageFileClient : IStorageFileClient
{
    private readonly FilePath _filePath;

    public SnowflakeStorageFileClient(FilePath filePath)
    {
        _filePath = filePath ?? throw new ArgumentNullException(nameof(filePath));
    }

    public Uri Uri => new($"snowflake://{Uri.EscapeDataString(_filePath.FullPath)}");

    public Task DeleteAsync() => Task.CompletedTask;

    public Task DeleteIfExistsAsync() => Task.CompletedTask;

    public Task<bool> ExistsAsync() => Task.FromResult(false);

    public Task<Stream> OpenWriteAsync(bool overwrite) => Task.FromResult<Stream>(new MemoryStream());

    public Task RenameAsync(FilePath targetPath) => Task.CompletedTask;

    public Task SetMetadataAsync(FileMetadata fileMetadata) => Task.CompletedTask;
}
