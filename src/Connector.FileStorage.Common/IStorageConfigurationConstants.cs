
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
        /// Environment key name for how long a write into the stream cache waits while an export asks the writers to slow down.
        /// </summary>
        string ThrottleStreamCacheWriteDelayKeyName { get; }

        /// <summary>
        /// Default value for the throttled write delay in milliseconds (0 doesn't delay).
        /// </summary>
        int ThrottleStreamCacheWriteDelayDefaultValue { get; }

        /// <summary>
        /// Environment key name for how long after an export's request the writes into the stream cache are delayed.
        /// </summary>
        string ThrottleStreamCacheDurationKeyName { get; }

        /// <summary>
        /// Default value for the throttle duration in milliseconds.
        /// </summary>
        int ThrottleStreamCacheDurationDefaultValue { get; }

        /// <summary>
        /// Environment key name for how often an export asks the writers into the stream cache to slow down.
        /// </summary>
        string ThrottleStreamCacheRequestIntervalKeyName { get; }

        /// <summary>
        /// Default value for the throttle request interval in milliseconds (0 doesn't ask).
        /// </summary>
        int ThrottleStreamCacheRequestIntervalDefaultValue { get; }
    }
}
