using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using CluedIn.Connector.FileStorage.Common.Connector;

using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

using Xunit;

namespace CluedIn.Connector.FileStorage.Common.Tests.Integration;

/// <summary>
/// Needs INTEGRATIONTEST_STREAMCACHE (a base64 encoded SQL Server connection string).
/// The tests for a database with and without ALLOW_SNAPSHOT_ISOLATION are skipped when the database is the other way.
/// </summary>
public class SnapshotIsolationCheckTests
{
    [Fact]
    public async Task LogIfNotEnabledAsync_WhenSnapshotIsolationIsEnabled_LogsNothing()
    {
        await SkipUnlessSnapshotIsolationIs(enabled: true);
        var logger = new CapturingLogger();

        await SnapshotIsolationCheck.LogIfNotEnabledAsync(logger, "TestConnector", ConnectionString);

        Assert.Empty(logger.Entries);
    }

    [Fact]
    public async Task LogIfNotEnabledAsync_WhenSnapshotIsolationIsNotEnabled_LogsAnError()
    {
        await SkipUnlessSnapshotIsolationIs(enabled: false);
        var logger = new CapturingLogger();
        var databaseName = new SqlConnectionStringBuilder(ConnectionString).InitialCatalog;

        await SnapshotIsolationCheck.LogIfNotEnabledAsync(logger, "TestConnector", ConnectionString);

        var entry = Assert.Single(logger.Entries);
        Assert.Equal(LogLevel.Error, entry.Level);
        Assert.Contains("TestConnector", entry.Message);
        Assert.Contains("snapshot isolation is not enabled", entry.Message);
        Assert.Contains($"ALTER DATABASE [{databaseName}] SET ALLOW_SNAPSHOT_ISOLATION ON", entry.Message);
    }

    [Fact]
    public async Task LogIfNotEnabledAsync_WhenTheDatabaseCannotBeReached_LogsAWarningAndDoesNotThrow()
    {
        var logger = new CapturingLogger();
        var unreachable = new SqlConnectionStringBuilder { DataSource = "127.0.0.1,1", InitialCatalog = "NoDatabase", ConnectTimeout = 2, Encrypt = SqlConnectionEncryptOption.Optional }.ConnectionString;

        await SnapshotIsolationCheck.LogIfNotEnabledAsync(logger, "TestConnector", unreachable);

        var entry = Assert.Single(logger.Entries);
        Assert.Equal(LogLevel.Warning, entry.Level);
        Assert.NotNull(entry.Exception);
    }

    private static string ConnectionString => StreamCacheTestConnection.ConnectionString;

    private static async Task SkipUnlessSnapshotIsolationIs(bool enabled)
    {
        await using var connection = new SqlConnection(ConnectionString);
        await connection.OpenAsync();
        var isEnabled = await SnapshotSqlDataReader.IsSnapshotIsolationEnabledAsync(connection);
        Assert.SkipUnless(isEnabled == enabled, $"The test needs ALLOW_SNAPSHOT_ISOLATION {(enabled ? "ON" : "OFF")} on the database.");
    }

    private sealed class CapturingLogger : ILogger
    {
        private readonly List<(LogLevel Level, string Message, Exception Exception)> _entries = new();

        public IReadOnlyList<(LogLevel Level, string Message, Exception Exception)> Entries => _entries.ToList();

        public IDisposable BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter)
        {
            _entries.Add((logLevel, formatter(state, exception), exception));
        }
    }
}
