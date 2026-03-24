using System.Collections.Generic;
using System.Threading.Tasks;

using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Models;
using CluedIn.Core.Connectors;

namespace CluedIn.Connector.DataLake.Common.Connector;

public interface IDataLakeClient : IExternalFileStorageClient
{
    Task<DataLakeDirectoryClient> EnsureDataLakeDirectoryExist(IDataLakeJobData configuration);
    Task<DataLakeDirectoryClient> EnsureDataLakeDirectoryExist(IDataLakeJobData configuration, string subDirectory);
    Task<PathProperties> GetDataLakeFilePathProperties(IDataLakeJobData configuration, string fileName);
    Task<PathProperties> GetDataLakeFilePathProperties(IDataLakeJobData configuration, string fileName, string subDirectory);
}
