using System.Data;
using System.Threading.Tasks;

using Microsoft.Data.SqlClient;

namespace CluedIn.Connector.FileStorage.Common.Connector;

internal class DistributedLockHelper
{
    internal static async Task<bool> TryAcquireExclusiveLock(SqlConnection connection, string resourceName, int timeOutInMilliseconds)
    {
        using var lockCommand = new SqlCommand("sp_getapplock", connection);
        return await TryAcquireExclusiveLock(lockCommand, resourceName, timeOutInMilliseconds);
    }

    internal static async Task<bool> TryAcquireExclusiveLock(SqlTransaction transaction, string resourceName, int timeOutInMilliseconds)
    {
        using var lockCommand = new SqlCommand("sp_getapplock", transaction.Connection, transaction);
        return await TryAcquireExclusiveLock(lockCommand, resourceName, timeOutInMilliseconds);
    }

    /// <summary>
    /// Acquires an exclusive lock owned by the connection's session rather than by a transaction. It needs no transaction
    /// and is held until <see cref="ReleaseSessionLock"/> is called or the session ends.
    /// </summary>
    internal static async Task<bool> TryAcquireExclusiveSessionLock(SqlConnection connection, string resourceName, int timeOutInMilliseconds)
    {
        using var lockCommand = new SqlCommand("sp_getapplock", connection);
        return await TryAcquireExclusiveLock(lockCommand, resourceName, timeOutInMilliseconds, "Session");
    }

    internal static async Task ReleaseSessionLock(SqlConnection connection, string resourceName)
    {
        using var releaseCommand = new SqlCommand("sp_releaseapplock", connection)
        {
            CommandType = CommandType.StoredProcedure
        };
        releaseCommand.Parameters.Add(new SqlParameter("@Resource", SqlDbType.NVarChar, 255) { Value = resourceName });
        releaseCommand.Parameters.Add(new SqlParameter("@LockOwner", SqlDbType.NVarChar, 32) { Value = "Session" });
        _ = await releaseCommand.ExecuteNonQueryAsync();
    }

    private static async Task<bool> TryAcquireExclusiveLock(SqlCommand lockCommand, string resourceName, int timeOutInMilliseconds, string lockOwner = "Transaction")
    {
        lockCommand.CommandType = CommandType.StoredProcedure;
        lockCommand.Parameters.Add(new SqlParameter("@Resource", SqlDbType.NVarChar, 255) { Value = resourceName });
        lockCommand.Parameters.Add(new SqlParameter("@LockMode", SqlDbType.NVarChar, 32) { Value = "Exclusive" });
        lockCommand.Parameters.Add(new SqlParameter("@LockOwner", SqlDbType.NVarChar, 32) { Value = lockOwner });
        lockCommand.Parameters.Add(new SqlParameter("@LockTimeout", SqlDbType.Int) { Value = timeOutInMilliseconds });
        lockCommand.Parameters.Add(new SqlParameter("@Result", SqlDbType.Int) { Direction = ParameterDirection.ReturnValue });

        _ = await lockCommand.ExecuteNonQueryAsync();

        var queryResult = (int)lockCommand.Parameters["@Result"].Value;
        var acquiredLock = queryResult >= 0;
        return acquiredLock;
    }
}
