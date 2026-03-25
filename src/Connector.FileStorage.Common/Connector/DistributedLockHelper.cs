using System.Data;
using System.Threading.Tasks;

using Microsoft.Data.SqlClient;

namespace CluedIn.Connector.DataLake.Common.Connector;

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

    private static async Task<bool> TryAcquireExclusiveLock(SqlCommand lockCommand, string resourceName, int timeOutInMilliseconds)
    {
        lockCommand.CommandType = CommandType.StoredProcedure;
        lockCommand.Parameters.Add(new SqlParameter("@Resource", SqlDbType.NVarChar, 255) { Value = resourceName });
        lockCommand.Parameters.Add(new SqlParameter("@LockMode", SqlDbType.NVarChar, 32) { Value = "Exclusive" });
        lockCommand.Parameters.Add(new SqlParameter("@LockTimeout", SqlDbType.Int) { Value = timeOutInMilliseconds });
        lockCommand.Parameters.Add(new SqlParameter("@Result", SqlDbType.Int) { Direction = ParameterDirection.ReturnValue });

        _ = await lockCommand.ExecuteNonQueryAsync();

        var queryResult = (int)lockCommand.Parameters["@Result"].Value;
        var acquiredLock = queryResult >= 0;
        return acquiredLock;
    }
}
