using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace CluedIn.Connector.DataLake.Common.Connector;

/// <summary>
/// Represents a storage-agnostic file client abstraction for writing and managing files
/// in an external storage system (e.g., Azure Data Lake, S3).
/// </summary>
public interface IStorageFileClient
{
    /// <summary>
    /// Opens a writable stream to the file.
    /// </summary>
    Task<Stream> OpenWriteAsync(bool overwrite, CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes the file if it exists.
    /// </summary>
    Task DeleteIfExistsAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Sets metadata on the file.
    /// </summary>
    Task SetMetadataAsync(IDictionary<string, string> metadata, CancellationToken cancellationToken = default);

    /// <summary>
    /// Renames (moves) the file to the target path.
    /// </summary>
    Task RenameAsync(string targetPath, CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets the URI of the file.
    /// </summary>
    string Uri { get; }

    /// <summary>
    /// Gets the path of the file relative to the storage container.
    /// </summary>
    string Path { get; }
}
