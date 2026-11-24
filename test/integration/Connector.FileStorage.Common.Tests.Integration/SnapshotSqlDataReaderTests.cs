using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;

using CluedIn.Connector.FileStorage.Common.Connector;

using Microsoft.Data.SqlClient;

using Xunit;

namespace CluedIn.Connector.FileStorage.Common.Tests.Integration;

/// <summary>
/// Reads a real system-versioned table as of a point in time while other connections update it.
/// Needs INTEGRATIONTEST_STREAMCACHE (a base64 encoded SQL Server connection string).
/// Most tests need ALLOW_SNAPSHOT_ISOLATION on that database and are skipped without it,
/// the one test for a database without it is skipped when it is on.
/// </summary>
public class SnapshotSqlDataReaderTests
{
    private const string AsOfSql = "SELECT * FROM [{0}] FOR SYSTEM_TIME AS OF @AsOfTime";

    private readonly ITestOutputHelper _output;

    public SnapshotSqlDataReaderTests(ITestOutputHelper output)
    {
        _output = output;
    }

    [Fact]
    public async Task TryOpenAsync_WhenRowsAreUpdatedDuringTheRead_ReturnsEveryRowAsOfTheTimeExactlyOnce()
    {
        await SkipUnlessSnapshotIsolationIs(enabled: true);
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
        await using (var snapshotReader = await SnapshotSqlDataReader.TryOpenAsync(ConnectionString, connection => table.CreateAsOfCommand(connection, asOfTime)))
        {
            Assert.NotNull(snapshotReader);
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

        await stopWritersCts.CancelAsync();
        var updateStatements = (await Task.WhenAll(writers)).Sum();
        _output.WriteLine($"Rows returned: {returnedIds.Count}. Update statements during the read: {updateStatements}.");

        Assert.Equal(rowCount, returnedIds.Count);
        Assert.Equal(rowCount, returnedIds.Distinct().Count());
        Assert.True(initialIds.SetEquals(returnedIds), "A row that existed at the time was not returned.");
    }

    [Fact]
    public async Task TryOpenAsync_WhenTheReadIsOpenInsideASerializableScope_DoesNotBlockWriters()
    {
        await SkipUnlessSnapshotIsolationIs(enabled: true);
        await using var table = await TestTable.CreateAsync(1_000);
        await Task.Delay(TimeSpan.FromSeconds(1.5));
        var asOfTime = await table.GetServerTimeAsync();

        using var scope = new TransactionScope(
            TransactionScopeOption.Required,
            new TransactionOptions { IsolationLevel = System.Transactions.IsolationLevel.Serializable },
            TransactionScopeAsyncFlowOption.Enabled);
        await using var snapshotReader = await SnapshotSqlDataReader.TryOpenAsync(ConnectionString, connection => table.CreateAsOfCommand(connection, asOfTime));
        Assert.NotNull(snapshotReader);
        Assert.True(await snapshotReader.Reader.ReadAsync());

        // the read is still open, a write from another connection must not wait for it
        var started = DateTime.UtcNow;
        using (var suppress = new TransactionScope(TransactionScopeOption.Suppress, TransactionScopeAsyncFlowOption.Enabled))
        {
            await table.ExecuteAsync($"SET LOCK_TIMEOUT 3000; UPDATE [{table.Name}] SET [Value] = 1");
            suppress.Complete();
        }

        var elapsed = DateTime.UtcNow - started;
        _output.WriteLine($"Updating every row while the read was open took {elapsed.TotalMilliseconds:F0} ms.");
        Assert.True(elapsed < TimeSpan.FromSeconds(3), "The update had to wait for the read.");
    }

    [Fact]
    public async Task TryOpenAsync_WhenSnapshotIsolationIsNotEnabled_ReturnsNull()
    {
        await SkipUnlessSnapshotIsolationIs(enabled: false);
        await using var table = await TestTable.CreateAsync(10);
        var asOfTime = await table.GetServerTimeAsync();

        await using var snapshotReader = await SnapshotSqlDataReader.TryOpenAsync(ConnectionString, connection => table.CreateAsOfCommand(connection, asOfTime));

        Assert.Null(snapshotReader);
    }

    private static string ConnectionString => StreamCacheTestConnection.ConnectionString;

    private static async Task SkipUnlessSnapshotIsolationIs(bool enabled)
    {
        await using var connection = new SqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = new SqlCommand("SELECT snapshot_isolation_state FROM sys.databases WHERE database_id = DB_ID()", connection);
        var isEnabled = Convert.ToInt32(await command.ExecuteScalarAsync()) == 1;
        Assert.SkipUnless(isEnabled == enabled, $"The test needs ALLOW_SNAPSHOT_ISOLATION {(enabled ? "ON" : "OFF")} on the database.");
    }

    private sealed class TestTable : IAsyncDisposable
    {
        private TestTable(string name)
        {
            Name = name;
        }

        public string Name { get; }

        public static async Task<TestTable> CreateAsync(int rowCount)
        {
            var table = new TestTable($"SnapshotReaderTest_{Guid.NewGuid():N}");
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
