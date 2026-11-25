using System;
using System.Data;
using System.Threading.Tasks;

using Microsoft.Data.SqlClient;

namespace CluedIn.Connector.FileStorage.Common.Connector;

/// <summary>
/// Reads a query in a transaction of its own in which a system-versioned table and its history table are read as of the same
/// moment, so no row is returned twice or missed.
/// </summary>
/// <remarks>
/// The transaction uses snapshot isolation, in which the read neither blocks nor is blocked by the writers of the cache table. It
/// needs ALLOW_SNAPSHOT_ISOLATION on the database. Without it the transaction is serializable and blocks the writers until it ends.
/// </remarks>
internal sealed class SnapshotSqlDataReader : IAsyncDisposable
{
    private readonly SqlConnection _connection;
    private readonly SqlTransaction _transaction;
    private readonly SqlCommand _command;
    private bool _disposed;

    private SnapshotSqlDataReader(SqlConnection connection, SqlTransaction transaction, SqlCommand command, SqlDataReader reader, bool isSnapshotIsolation)
    {
        _connection = connection;
        _transaction = transaction;
        _command = command;
        Reader = reader;
        IsSnapshotIsolation = isSnapshotIsolation;
    }

    public SqlDataReader Reader { get; }

    public bool IsSnapshotIsolation { get; }

    /// <summary>
    /// Opens the reader on a connection that has no transaction. The connection is not disposed with the reader.
    /// </summary>
    public static async Task<SnapshotSqlDataReader> OpenAsync(SqlConnection connection, Func<SqlConnection, SqlCommand> createCommand)
    {
        // Without the option the first read of a row fails, after the reader has been returned, so ask for it first.
        var isSnapshotIsolation = await IsSnapshotIsolationEnabledAsync(connection);
        var transaction = (SqlTransaction)await connection.BeginTransactionAsync(isSnapshotIsolation ? IsolationLevel.Snapshot : IsolationLevel.Serializable);
        SqlCommand command = null;
        try
        {
            command = createCommand(connection);
            command.Transaction = transaction;
            var reader = await command.ExecuteReaderAsync();
            return new SnapshotSqlDataReader(connection, transaction, command, reader, isSnapshotIsolation);
        }
        catch
        {
            await DisposeAsync(connection, transaction, command);
            throw;
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        await Reader.DisposeAsync();
        await DisposeAsync(_connection, _transaction, _command);
    }

    public static async Task<bool> IsSnapshotIsolationEnabledAsync(SqlConnection connection)
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

        await transaction.DisposeAsync();

        // The isolation level of the session outlives the transaction, and the connection is used again after the read.
        if (connection.State == ConnectionState.Open)
        {
            try
            {
                await using var resetCommand = new SqlCommand("SET TRANSACTION ISOLATION LEVEL READ COMMITTED", connection);
                await resetCommand.ExecuteNonQueryAsync();
            }
            catch (SqlException)
            {
                // the connection is broken and must not hide the failure of the read
            }
        }
    }
}
