using System.Collections.Generic;
using System.Threading.Tasks;

using CluedIn.Core.Connectors;

namespace CluedIn.Connector.DataLake.Common.Connector;

/// <summary>
/// Represents a storage-agnostic client for interacting with external file storage systems
/// (e.g., Azure Data Lake, Amazon S3). This is the primary abstraction used by
/// <see cref="DataLakeConnector"/> and <see cref="DataLakeExportEntitiesJobBase"/>.
/// </summary>
public interface IExternalFileStorageClient
{
    Task<IStorageDirectoryClient> EnsureDirectoryExist(IDataLakeJobData configuration);
    Task<IStorageDirectoryClient> EnsureDirectoryExist(IDataLakeJobData configuration, string subDirectory);
    Task SaveData(IDataLakeJobData configuration, string content, string fileName, string contentType);
    Task DeleteDirectory(IDataLakeJobData configuration, string subDirectory);
    Task DeleteFile(IDataLakeJobData configuration, string fileName);
    Task<bool> FileInPathExists(IDataLakeJobData configuration, string fileName);
    Task<bool> FileInPathExists(IDataLakeJobData configuration, string fileName, string subDirectory);
    Task<IStorageFileProperties> GetFilePathProperties(IDataLakeJobData configuration, string fileName);
    Task<IStorageFileProperties> GetFilePathProperties(IDataLakeJobData configuration, string fileName, string subDirectory);
    Task<bool> DirectoryExists(IDataLakeJobData configuration);
    Task<bool> DirectoryExists(IDataLakeJobData configuration, string subDirectory);
    Task<IEnumerable<IConnectorContainer>> GetFilesInDirectory(IDataLakeJobData configuration, string subDirectory = null);
}
