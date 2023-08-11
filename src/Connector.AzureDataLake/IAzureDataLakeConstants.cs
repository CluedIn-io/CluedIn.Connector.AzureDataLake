
namespace CluedIn.Connector.AzureDataLake
{
    public interface IAzureDataLakeConstants : IConfigurationConstants
    {
        /// <summary>
        /// Environment key name for cache sync interval
        /// </summary>
        string CacheSyncIntervalKeyName { get; }

        /// <summary>
        /// Environment key name for cache records threshold
        /// </summary>
        string CacheRecordsThresholdKeyName { get; }
    }
}
