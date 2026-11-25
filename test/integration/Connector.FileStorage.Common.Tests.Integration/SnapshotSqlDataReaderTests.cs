using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using CluedIn.Connector.FileStorage.Common.Connector;

using Microsoft.Data.SqlClient;

using Xunit;

namespace CluedIn.Connector.FileStorage.Common.Tests.Integration;

/// <summary>
/// Reads a real system-versioned table as of a point in time while other connections update it.
/// Needs INTEGRATIONTEST_STREAMCACHE, a database with ALLOW_SNAPSHOT_ISOLATION on, and
/// INTEGRATIONTEST_STREAMCACHE_NOSNAPSHOT, one with it off (both base64 encoded connection strings).
/// </summary>
public class SnapshotSqlDataReaderTests
{
    // the values of sys.dm_exec_sessions.transaction_isolation_level
    private const short ReadCommittedLevel = 2;
    private const short SerializableLevel = 4;
    private const short SnapshotLevel = 5;

    private const string AsOfSql = "SELECT * FROM [{0}] FOR SYSTEM_TIME AS OF @AsOfTime";

    private readonly ITestOutputHelper _output;

    public SnapshotSqlDataReaderTests(ITestOutputHelper output)
    {
        _output = output;
    }

    [Fact]
    public async Task OpenAsync_WhenRowsAreUpdatedDuringTheRead_ReturnsEveryRowAsOfTheTimeExactlyOnce()
    {
        await AssertSnapshotIsolationIsOnAsync();
        const int rowCount = 30_000;
        await using var table = await TestTable.CreateAsync(rowCount);
        var initialIds = await table.GetIdsAsync();
        await Task.Delay(TimeSpan.FromSeconds(1.5));
        var asOfTime = await table.GetServerTimeAsync();

        using var stopWritersCts = new CancellationTokenSource();
        var writers = Enumerable.Range(0, 2)
            .Select(_ => Task.Run(() => table.UpdateRandomRowsUntilAsync(rowCount, stopWritersCts.Token)))
            .ToList();

        var returnedIds = new List<Guid>();
        await using (var readConnection = await table.OpenConnectionAsync())
        await using (var snapshotReader = await SnapshotSqlDataReader.OpenAsync(readConnection, connection => table.CreateAsOfCommand(connection, asOfTime)))
        {
            Assert.True(snapshotReader.IsSnapshotIsolation);
            while (await snapshotReader.Reader.ReadAsync())
            {
                returnedIds.Add(snapshotReader.Reader.GetGuid(snapshotReader.Reader.GetOrdinal("Id")));

                // a slow reader, like the export writing a file, so that many updates happen during the read
                if (returnedIds.Count % 200 == 0)
                {
                    await Task.Delay(1);
                }
            }
        }

        stopWritersCts.Cancel();
        var updateStatements = (await Task.WhenAll(writers)).Sum();
        _output.WriteLine($"Rows returned: {returnedIds.Count}. Update statements during the read: {updateStatements}.");

        Assert.Equal(rowCount, returnedIds.Count);
        Assert.Equal(rowCount, returnedIds.Distinct().Count());
        Assert.True(initialIds.SetEquals(returnedIds), "A row that existed at the time was not returned.");
    }

    [Fact]
    public async Task OpenAsync_WhenTheReadIsOpen_DoesNotBlockWriters()
    {
        await AssertSnapshotIsolationIsOnAsync();
        await using var table = await TestTable.CreateAsync(1_000);
        await Task.Delay(TimeSpan.FromSeconds(1.5));
        var asOfTime = await table.GetServerTimeAsync();

        await using var readConnection = await table.OpenConnectionAsync();
        await using var snapshotReader = await SnapshotSqlDataReader.OpenAsync(readConnection, connection => table.CreateAsOfCommand(connection, asOfTime));
        Assert.True(await snapshotReader.Reader.ReadAsync());

        // the read is still open, a write from another connection must not wait for it
        var started = DateTime.UtcNow;
        await table.ExecuteAsync($"SET LOCK_TIMEOUT 3000; UPDATE [{table.Name}] SET [Value] = 1");
        var elapsed = DateTime.UtcNow - started;
        _output.WriteLine($"Updating every row while the read was open took {elapsed.TotalMilliseconds:F0} ms.");
        Assert.True(elapsed < TimeSpan.FromSeconds(3), "The update had to wait for the read.");
    }

    [Theory]
    [InlineData(true, SnapshotLevel)]
    [InlineData(false, SerializableLevel)]
    public async Task OpenAsync_ReadsInASnapshotOrSerializable_AndLeavesTheConnectionReadCommitted(bool isSnapshotIsolationEnabled, short expectedLevel)
    {
        const string levelSql = "SELECT transaction_isolation_level FROM sys.dm_exec_sessions WHERE session_id = @@SPID";
        await using var connection = new SqlConnection(isSnapshotIsolationEnabled ? ConnectionString : StreamCacheTestConnection.ConnectionStringWithoutSnapshotIsolation);
        await connection.OpenAsync();

        await using (var snapshotReader = await SnapshotSqlDataReader.OpenAsync(connection, dataConnection => new SqlCommand(levelSql, dataConnection)))
        {
            Assert.Equal(isSnapshotIsolationEnabled, snapshotReader.IsSnapshotIsolation);
            Assert.True(await snapshotReader.Reader.ReadAsync());
            Assert.Equal(expectedLevel, snapshotReader.Reader.GetInt16(0));
        }

        await using var command = new SqlCommand(levelSql, connection);
        Assert.Equal(ReadCommittedLevel, (short)await command.ExecuteScalarAsync());
    }

    private static string ConnectionString => StreamCacheTestConnection.ConnectionString;

    // fails and does not skip, so that a pipeline database without the option cannot make these tests pass by being skipped
    private static async Task AssertSnapshotIsolationIsOnAsync()
    {
        await using var connection = new SqlConnection(ConnectionString);
        await connection.OpenAsync();
        Assert.True(await SnapshotSqlDataReader.IsSnapshotIsolationEnabledAsync(connection), "The test needs ALLOW_SNAPSHOT_ISOLATION ON on the INTEGRATIONTEST_STREAMCACHE database.");
    }

    private sealed class TestTable : IAsyncDisposable
    {
        private TestTable(string name, string connectionString)
        {
            Name = name;
            ConnectionString = connectionString;
        }

        public string Name { get; }

        public string ConnectionString { get; }

        public static async Task<TestTable> CreateAsync(int rowCount, string connectionString = null)
        {
            var table = new TestTable($"SnapshotReaderTest_{Guid.NewGuid():N}", connectionString ?? SnapshotSqlDataReaderTests.ConnectionString);
            await table.ExecuteAsync($"""
                CREATE TABLE [{table.Name}] (
                    [Id] UNIQUEIDENTIFIER NOT NULL,
                    [Seq] INT NOT NULL,
                    [Value] INT NOT NULL,
                    [Pad] NVARCHAR(200) NULL,
                    [ValidFrom] DATETIME2 GENERATED ALWAYS AS ROW START HIDDEN,
                    [ValidTo] DATETIME2 GENERATED ALWAYS AS ROW END HIDDEN,
                    PERIOD FOR SYSTEM_TIME(ValidFrom, ValidTo),
                    CONSTRAINT [PK_{table.Name}] PRIMARY KEY CLUSTERED ([Id])
                ) WITH (SYSTEM_VERSIONING = ON (HISTORY_TABLE = dbo.[{table.Name}_History]));
                CREATE INDEX [IX_Seq] ON [{table.Name}] ([Seq]);
                INSERT [{table.Name}] ([Id], [Seq], [Value], [Pad])
                SELECT TOP ({rowCount}) NEWID(), ROW_NUMBER() OVER (ORDER BY (SELECT 1)), 0, REPLICATE(N'p', 100)
                FROM sys.all_objects AS a CROSS JOIN sys.all_objects AS b;
                """);
            return table;
        }

        public SqlCommand CreateAsOfCommand(SqlConnection connection, DateTimeOffset asOfTime)
        {
            var command = new SqlCommand(string.Format(AsOfSql, Name), connection);
            command.Parameters.Add(new SqlParameter("@AsOfTime", System.Data.SqlDbType.DateTime2) { Value = asOfTime.UtcDateTime });
            return command;
        }

        public async Task<SqlConnection> OpenConnectionAsync()
        {
            var connection = new SqlConnection(ConnectionString);
            await connection.OpenAsync();
            return connection;
        }

        public async Task ExecuteAsync(string sql)
        {
            await using var connection = await OpenConnectionAsync();
            await using var command = new SqlCommand(sql, connection) { CommandTimeout = 120 };
            await command.ExecuteNonQueryAsync();
        }

        public async Task<HashSet<Guid>> GetIdsAsync()
        {
            await using var connection = await OpenConnectionAsync();
            await using var command = new SqlCommand($"SELECT [Id] FROM [{Name}]", connection);
            await using var reader = await command.ExecuteReaderAsync();
            var ids = new HashSet<Guid>();
            while (await reader.ReadAsync())
            {
                ids.Add(reader.GetGuid(0));
            }

            return ids;
        }

        public async Task<DateTimeOffset> GetServerTimeAsync()
        {
            await using var connection = await OpenConnectionAsync();
            await using var command = new SqlCommand("SELECT SYSUTCDATETIME()", connection);
            var utc = (DateTime)await command.ExecuteScalarAsync();
            return new DateTimeOffset(DateTime.SpecifyKind(utc, DateTimeKind.Utc));
        }

        public async Task<int> UpdateRandomRowsUntilAsync(int rowCount, CancellationToken cancellationToken)
        {
            var random = new Random();
            var statements = 0;
            await using var connection = await OpenConnectionAsync();
            await using var command = new SqlCommand($"UPDATE [{Name}] SET [Value] = [Value] + 1 WHERE [Seq] BETWEEN @From AND @To", connection);
            var from = command.Parameters.Add("@From", System.Data.SqlDbType.Int);
            var to = command.Parameters.Add("@To", System.Data.SqlDbType.Int);
            while (!cancellationToken.IsCancellationRequested)
            {
                var start = random.Next(1, rowCount - 100);
                from.Value = start;
                to.Value = start + 99;
                try
                {
                    await command.ExecuteNonQueryAsync(CancellationToken.None);
                    statements++;
                }
                catch (SqlException ex) when (ex.Number is 1205 or 13535)
                {
                    // deadlock between two writers, or two writers of a temporal table at the same moment, neither is about the reader
                }
            }

            return statements;
        }

        public async ValueTask DisposeAsync()
        {
            await using var connection = await OpenConnectionAsync();
            await using var command = new SqlCommand($"""
                ALTER TABLE [{Name}] SET (SYSTEM_VERSIONING = OFF);
                DROP TABLE IF EXISTS [{Name}];
                DROP TABLE IF EXISTS [{Name}_History];
                """, connection);
            await command.ExecuteNonQueryAsync();
        }
    }
}
