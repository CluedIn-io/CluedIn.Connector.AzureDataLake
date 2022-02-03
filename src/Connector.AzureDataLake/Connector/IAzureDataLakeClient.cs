using Azure.Storage.Files.DataLake;
using System.Threading.Tasks;

namespace CluedIn.Connector.AzureDataLake.Connector
{
    public interface IAzureDataLakeClient
    {
        Task<DataLakeDirectoryClient> EnsureDataLakeDirectoryExist(AzureDataLakeConnectorJobData configuration);
        Task SaveData(AzureDataLakeConnectorJobData configuration, string content);
    }
}
