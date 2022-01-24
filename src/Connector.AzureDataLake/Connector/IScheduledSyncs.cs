using System.Threading.Tasks;

namespace CluedIn.Connector.AzureDataLake.Connector
{
    public interface IScheduledSyncs
    {
        Task Sync();
    }
}
