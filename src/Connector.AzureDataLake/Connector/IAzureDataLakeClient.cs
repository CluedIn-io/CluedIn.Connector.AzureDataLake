using System.Threading.Tasks;

using Azure.Storage.Files.DataLake;

namespace CluedIn.Connector.AzureDataLake.Connector
{
    public interface IAzureDataLakeClient
    {
        Task<DataLakeDirectoryClient> EnsureDataLakeDirectoryExist(AzureDataLakeConnectorJobData configuration);
        Task SaveData(AzureDataLakeConnectorJobData configuration, string content, string fileName, string contentType);
        Task DeleteFile(AzureDataLakeConnectorJobData configuration, string fileName);
    }
}
