using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace CluedIn.Connector.FileStorage.Common;

public interface IStorageFileClient
{
    Uri Uri { get; }

    Task DeleteAsync();

    Task DeleteIfExistsAsync();

    Task<bool> ExistsAsync();

    Task<Stream> OpenWriteAsync(bool overwrite);

    Task RenameAsync(FilePath targetPath);

    Task SetMetadataAsync(FileMetadata fileMetadata);
}
