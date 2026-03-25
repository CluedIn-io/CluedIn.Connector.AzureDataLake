namespace CluedIn.Connector.DataLake.Common.Connector;

/// <summary>
/// Represents a storage-agnostic directory client abstraction for managing directories
/// in an external storage system (e.g., Azure Data Lake, S3).
/// </summary>
public interface IStorageDirectoryClient
{
    /// <summary>
    /// Gets a file client for the specified file name within this directory.
    /// </summary>
    IStorageFileClient GetFileClient(string fileName);
}
