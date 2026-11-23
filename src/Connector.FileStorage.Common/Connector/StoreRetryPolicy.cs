using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.FileStorage.Common.Connector;

/// <summary>
/// Retries the write of an entity that failed, a number of times and with a delay before each retry, before the failure is
/// returned to the pipeline (as a ReQueue). Both are settings of the connector (see
/// <see cref="IStorageConfigurationConstants.StoreRetryCountKeyName"/> and <see cref="IStorageConfigurationConstants.StoreRetryDelayKeyName"/>).
/// Only a failure that can go away on its own is retried: a SQL Server error that is transient (a timeout, a deadlock, a
/// connection that was lost or could not be made, a server that is busy or failing over, see <see cref="IsTransientSqlErrorNumber"/>).
/// What retrying cannot fix (a login or permission that is refused, a database or object that is not there, an invalid
/// statement, a violated constraint, a value that does not convert) and what is not a SQL Server error is returned at once.
/// </summary>
internal sealed class StoreRetryPolicy
{
    // The numbers of the SQL Server errors that can go away on their own. Others are not retried: they are what retrying cannot fix.
    private static readonly HashSet<int> _transientSqlErrorNumbers =
    [
        -2, // The timeout period elapsed (a command or a connection timeout)
        258, // The wait operation timed out
        1205, // Chosen as the victim of a deadlock
        1222, // The lock request timeout was exceeded
        -1, // A network-related or instance-specific error (the server could not be reached)
        2, // The server was not found or was not accessible
        53, // The network path was not found
        64, // The connection was forcibly closed
        233, // The connection was closed by the server (no process on the other end of the pipe)
        10053, // The connection was aborted
        10054, // The connection was reset by the peer
        10060, // The connection attempt timed out
        10061, // The connection was refused (the server may be restarting)
        10928, // The resource limit of the Azure SQL database was reached
        10929, // The Azure SQL database is too busy
        40143, // The Azure SQL connection could not be initialized
        40197, // The Azure SQL service had an error processing the request
        40501, // The Azure SQL service is busy
        40613, // The Azure SQL database is not currently available
        49918, // Azure SQL cannot process the request, not enough resources
        49919, // Azure SQL cannot process the create or update request, too many operations
        49920, // Azure SQL cannot process the request, too many operations
    ];

    private readonly Func<TimeSpan, Task> _delay;
    private readonly Func<Exception, bool> _isRetryable;

    internal StoreRetryPolicy(int retryCount, TimeSpan retryDelay, Func<TimeSpan, Task> delay = null, Func<Exception, bool> isRetryable = null)
    {
        RetryCount = Math.Max(0, retryCount);
        RetryDelay = retryDelay < TimeSpan.Zero ? TimeSpan.Zero : retryDelay;
        _delay = delay ?? Task.Delay;
        _isRetryable = isRetryable ?? IsRetryable;
    }

    /// <summary>
    /// How many times a failed write is retried after the first attempt (so 3 retries are 4 attempts). 0 turns retrying off.
    /// </summary>
    internal int RetryCount { get; }

    /// <summary>
    /// The delay before each retry.
    /// </summary>
    internal TimeSpan RetryDelay { get; }

    /// <summary>
    /// Whether the exception, or the one it wraps, is a SQL Server error that can go away on its own.
    /// </summary>
    internal static bool IsRetryable(Exception exception)
    {
        return exception switch
        {
            null => false,
            SqlException sqlException => IsTransientSqlErrorNumber(sqlException.Number)
                || sqlException.Errors.Cast<SqlError>().Any(error => IsTransientSqlErrorNumber(error.Number)),
            AggregateException aggregateException => aggregateException.InnerExceptions.Any(IsRetryable),
            _ => IsRetryable(exception.InnerException),
        };
    }

    /// <summary>
    /// Whether a SQL Server error number is one of those that can go away on its own.
    /// </summary>
    internal static bool IsTransientSqlErrorNumber(int number) => _transientSqlErrorNumbers.Contains(number);

    /// <summary>
    /// Runs the operation. When it throws a failure that can go away on its own, it is run again after the delay, up to
    /// <see cref="RetryCount"/> times. The exception of the last attempt is thrown. Any other failure is thrown at once.
    /// </summary>
    internal async Task<T> RunAsync<T>(Func<Task<T>> operation, ILogger logger)
    {
        for (var retry = 0; ; retry++)
        {
            try
            {
                return await operation();
            }
            catch (Exception ex) when (retry < RetryCount && _isRetryable(ex))
            {
                logger.LogWarning(
                    ex,
                    "Failed to store the entity (attempt {Attempt} of {Attempts}). Retrying in {Delay}.",
                    retry + 1,
                    RetryCount + 1,
                    RetryDelay);
                await _delay(RetryDelay);
            }
        }
    }
}
