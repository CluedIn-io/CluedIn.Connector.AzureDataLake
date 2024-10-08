using System.Collections.Generic;
using System.Threading.Tasks;

using Azure.Storage.Files.DataLake;
using CluedIn.Core.Connectors;
using CluedIn.Core.Streams.Models;

namespace CluedIn.Connector.DataLake.Common.Connector;

public interface IDataLakeClient
{
    Task<DataLakeDirectoryClient> EnsureDataLakeDirectoryExist(IDataLakeJobData configuration);
    Task SaveData(IDataLakeJobData configuration, string content, string fileName, string contentType);
    Task DeleteFile(IDataLakeJobData configuration, string fileName);
    Task<bool> FileInPathExists(IDataLakeJobData configuration, string fileName);
    Task<IEnumerable<IConnectorContainer>> GetFilesInDirectory(IDataLakeJobData configuration, string subDirectory = null);
}
