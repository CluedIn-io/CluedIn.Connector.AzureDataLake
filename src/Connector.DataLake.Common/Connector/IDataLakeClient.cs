using System.Threading.Tasks;

using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Models;

namespace CluedIn.Connector.DataLake.Common.Connector;

public interface IDataLakeClient
{
    Task<DataLakeDirectoryClient> EnsureDataLakeDirectoryExist(IDataLakeJobData configuration);
    Task<DataLakeDirectoryClient> EnsureDataLakeDirectoryExist(IDataLakeJobData configuration, string subDirectory);
    Task SaveData(IDataLakeJobData configuration, string content, string fileName, string contentType);
    Task DeleteDirectory(IDataLakeJobData configuration, string subDirectory);
    Task DeleteFile(IDataLakeJobData configuration, string fileName);
    Task<bool> FileInPathExists(IDataLakeJobData configuration, string fileName);
    Task<bool> FileInPathExists(IDataLakeJobData configuration, string fileName, string subDirectory);
    Task<PathProperties> GetFilePathProperties(IDataLakeJobData configuration, string fileName);
    Task<PathProperties> GetFilePathProperties(IDataLakeJobData configuration, string fileName, string subDirectory);
    Task<bool> DirectoryExists(IDataLakeJobData configuration);
    Task<bool> DirectoryExists(IDataLakeJobData configuration, string subDirectory);
}
