
namespace CluedIn.Connector.FileStorage.Common
{
    public interface IStorageConfigurationConstants : IConfigurationConstants
    {
        /// <summary>
        /// Environment key name for cache sync interval
        /// </summary>
        string CacheSyncIntervalKeyName { get; }

        /// <summary>
        /// Default value for Cache sync interval in milliseconds
        /// </summary>
        int CacheSyncIntervalDefaultValue { get; }

        /// <summary>
        /// Environment key name for cache records threshold
        /// </summary>
        string CacheRecordsThresholdKeyName { get; }

        /// <summary>
        /// Default value for Cache records threshold
        /// </summary>
        int CacheRecordsThresholdDefaultValue { get; }

        /// <summary>
        /// Environment key name for Cache buffer strategy
        /// </summary>
        string CacheBufferStrategyKeyName { get; }

        /// <summary>
        /// Default value for Cache buffer strategy
        /// </summary>
        /// <summary>
        /// Environment key name for the minimum interval between repeated error logs from health-check connection verification.
        /// </summary>
        string HealthCheckErrorLogIntervalKeyName { get; }

        /// <summary>
        /// Default value for health-check error log interval in milliseconds.
        /// </summary>
        int HealthCheckErrorLogIntervalDefaultValue { get; }
    }
}
