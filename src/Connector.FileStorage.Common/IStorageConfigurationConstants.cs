
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
        string CacheBufferStrategyDefaultValue { get; }

        /// <summary>
        /// Environment key name for the minimum interval between repeated error logs from health-check connection verification.
        /// </summary>
        string HealthCheckErrorLogIntervalKeyName { get; }

        /// <summary>
        /// Default value for health-check error log interval in milliseconds.
        /// </summary>
        int HealthCheckErrorLogIntervalDefaultValue { get; }
        /// <summary>
        /// Environment key name for how many times the write of an entity is retried when it fails with a transient SQL Server error
        /// (a timeout, a deadlock, a lost connection and the like), before it is returned to the pipeline as a ReQueue. 0 turns retrying off.
        /// </summary>
        string StoreRetryCountKeyName { get; }

        /// <summary>
        /// Default value for how many times the write of an entity is retried.
        /// </summary>
        int StoreRetryCountDefaultValue { get; }

        /// <summary>
        /// Environment key name for the delay, in milliseconds, before each retry of the write of an entity.
        /// </summary>
        string StoreRetryDelayKeyName { get; }

        /// <summary>
        /// Default value for the delay before each retry of the write of an entity, in milliseconds.
        /// </summary>
        int StoreRetryDelayDefaultValue { get; }
    }
}
