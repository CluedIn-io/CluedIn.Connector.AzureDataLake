
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
        /// Environment key name for the switch that batches the stream cache writes of concurrent entities
        /// into one connection and transaction (and set based statements) instead of one each. Only streams that use soft delete are batched.
        /// </summary>
        string CacheBatchingEnabledKeyName { get; }

        /// <summary>
        /// Default value for the stream cache write batching switch.
        /// </summary>
        bool CacheBatchingEnabledDefaultValue { get; }

        /// <summary>
        /// Environment key name for the maximum number of entities written to the stream cache in one batch.
        /// </summary>
        string CacheBatchSizeKeyName { get; }

        /// <summary>
        /// Default value for the maximum number of entities written to the stream cache in one batch.
        /// </summary>
        int CacheBatchSizeDefaultValue { get; }

        /// <summary>
        /// Environment key name for how long, in milliseconds, a batch of entities written to the stream cache waits for more
        /// entities before it is written. 0 writes what is there without waiting.
        /// </summary>
        string CacheBatchTimeoutKeyName { get; }

        /// <summary>
        /// Default value for how long a batch of entities written to the stream cache waits for more entities, in milliseconds.
        /// </summary>
        int CacheBatchTimeoutDefaultValue { get; }

        /// <summary>
        /// Environment key name for the number of threads that write batches of entities to the stream cache at the same time.
        /// </summary>
        string CacheBatchOutputThreadsKeyName { get; }

        /// <summary>
        /// Default value for the number of threads that write batches of entities to the stream cache at the same time.
        /// </summary>
        int CacheBatchOutputThreadsDefaultValue { get; }
    }
}
