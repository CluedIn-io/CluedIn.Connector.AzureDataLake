using Azure.Storage.Files.DataLake;

using System.IO;
using System.Threading.Tasks;

namespace CluedIn.Connector.AzureDataLake.Connector
{
    public interface IAzureDataLakeClient
    {
        Task<DataLakeDirectoryClient> EnsureDataLakeDirectoryExist(AzureDataLakeConnectorJobData configuration);
        Task SaveData(AzureDataLakeConnectorJobData configuration, string content, string fileName);
        Task DeleteFile(AzureDataLakeConnectorJobData configuration, string fileName);
        Task SaveData(AzureDataLakeConnectorJobData configuration, Stream stream, string fileName);
    }
}
