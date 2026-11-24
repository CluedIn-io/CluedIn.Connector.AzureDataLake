using System;
using System.Threading.Tasks;

using Microsoft.Data.SqlClient;

namespace CluedIn.Connector.FileStorage.Common.Connector;

/// <summary>
/// Reads a query on its own connection under snapshot isolation.
/// </summary>
/// <remarks>
/// A snapshot read neither blocks nor is blocked by the writers of the cache table, and it sees a system-versioned
/// table and its history table as they were at the same moment, so no row is returned twice or missed.
/// The connection does not enlist in the ambient transaction scope of the export.
/// It needs ALLOW_SNAPSHOT_ISOLATION on the database, <see cref="TryOpenAsync"/> returns null when it is off.
/// </remarks>
internal sealed class SnapshotSqlDataReader : IAsyncDisposable
{
    private readonly SqlConnection _connection;
    private readonly SqlTransaction _transaction;
    private readonly SqlCommand _command;

    private SnapshotSqlDataReader(SqlConnection connection, SqlTransaction transaction, SqlCommand command, SqlDataReader reader)
    {
        _connection = connection;
        _transaction = transaction;
        _command = command;
        Reader = reader;
    }

    public SqlDataReader Reader { get; }

    public static async Task<SnapshotSqlDataReader> TryOpenAsync(string connectionString, Func<SqlConnection, SqlCommand> createCommand)
    {
        var connectionStringBuilder = new SqlConnectionStringBuilder(connectionString) { Enlist = false };
        var connection = new SqlConnection(connectionStringBuilder.ConnectionString);
        SqlTransaction transaction = null;
        SqlCommand command = null;
        try
        {
            await connection.OpenAsync();

            // Without the option the first read of a row fails, after the reader has been returned, so ask for it first.
            if (!await IsSnapshotIsolationEnabledAsync(connection))
            {
                await DisposeAsync(connection, transaction, command);
                return null;
            }

            transaction = (SqlTransaction)await connection.BeginTransactionAsync(System.Data.IsolationLevel.Snapshot);
            command = createCommand(connection);
            command.Transaction = transaction;
            var reader = await command.ExecuteReaderAsync();
            return new SnapshotSqlDataReader(connection, transaction, command, reader);
        }
        catch
        {
            await DisposeAsync(connection, transaction, command);
            throw;
        }
    }

    public async ValueTask DisposeAsync()
    {
        await Reader.DisposeAsync();
        await DisposeAsync(_connection, _transaction, _command);
    }

    private static async Task<bool> IsSnapshotIsolationEnabledAsync(SqlConnection connection)
    {
        // 1 is ON, the states in between are while the option is being turned on or off
        await using var command = new SqlCommand("SELECT snapshot_isolation_state FROM sys.databases WHERE database_id = DB_ID()", connection);
        return Convert.ToInt32(await command.ExecuteScalarAsync()) == 1;
    }

    private static async Task DisposeAsync(SqlConnection connection, SqlTransaction transaction, SqlCommand command)
    {
        if (command != null)
        {
            await command.DisposeAsync();
        }

        if (transaction != null)
        {
            await transaction.DisposeAsync();
        }

        await connection.DisposeAsync();
    }
}
