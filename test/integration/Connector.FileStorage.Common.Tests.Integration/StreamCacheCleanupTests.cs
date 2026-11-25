using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Castle.Windsor;

using CluedIn.Connector.FileStorage.Common.Connector;
using CluedIn.Core;

using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

using Moq;

using Xunit;

namespace CluedIn.Connector.FileStorage.Common.Tests.Integration;

/// <summary>
/// Removes real tables of a stream. Only the tables of the test's own stream are cleaned up, the database is shared with other tests.
/// Needs INTEGRATIONTEST_STREAMCACHE (a base64 encoded SQL Server connection string).
/// </summary>
public class StreamCacheCleanupTests
{
    private readonly Guid _streamId = Guid.NewGuid();
    private readonly Mock<ILogger> _logger = new();
    private readonly string _connectionString = Encoding.UTF8.GetString(Convert.FromBase64String(Environment.GetEnvironmentVariable("INTEGRATIONTEST_STREAMCACHE") ?? string.Empty));

    private string Table => CacheTableHelper.GetCacheTableName(_streamId);

    private StreamCacheTableRemover Remover => new(_logger.Object, _connectionString);

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task CleanUpStream_RemovesTheArchivedTablesExceptTheMostRecent_AndWarnsWhenTheStreamDoesNotExist(bool streamExists)
    {
        await CreateTablesAsync();
        try
        {
            var streamTables = StreamCacheTables.FromTableNames(await Remover.GetTableNamesAsync()).Single(tables => tables.StreamId == _streamId);
            var cleaner = new StreamCacheCleaner(_logger.Object, new ApplicationContext(new WindsorContainer()));

            await cleaner.CleanUpStreamAsync(Remover, streamTables, streamExists);

            var expectedTables = new[]
            {
                Table,
                $"{Table}_20250103000000",
                $"{Table}_ExportHistory_ExportHistory",
                $"{Table}_History",
                $"{Table}_History_20250103000000",
            };
            Assert.Equal(expectedTables.OrderBy(name => name, StringComparer.Ordinal), await GetStreamTablesAsync());
            Assert.Empty(GetLogged(LogLevel.Error));
            Assert.NotEmpty(GetLogged(LogLevel.Information));
            if (streamExists)
            {
                Assert.Empty(GetLogged(LogLevel.Warning));
            }
            else
            {
                var warning = Assert.Single(GetLogged(LogLevel.Warning));
                Assert.Contains($"{Table}_20250103000000", warning);
                Assert.DoesNotContain($"{Table}_20250102000000", warning);
            }
        }
        finally
        {
            await Remover.RemoveAllTablesAsync(_streamId);
        }
    }

    [Fact]
    public async Task RemoveAllTables_RemovesEveryTableOfTheStream()
    {
        await CreateTablesAsync();

        await Remover.RemoveAllTablesAsync(_streamId);

        Assert.Empty(await GetStreamTablesAsync());
        Assert.Empty(GetLogged(LogLevel.Error));
    }

    private List<string> GetLogged(LogLevel level)
    {
        return _logger.Invocations
            .Where(invocation => invocation.Method.Name == nameof(ILogger.Log) && level.Equals(invocation.Arguments[0]))
            .Select(invocation => invocation.Arguments[2].ToString())
            .ToList();
    }

    private async Task<List<string>> GetStreamTablesAsync()
    {
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();
        await using var command = new SqlCommand($"SELECT name FROM sys.tables WHERE name LIKE '{Table}%'", connection);
        await using var reader = await command.ExecuteReaderAsync();
        var names = new List<string>();
        while (await reader.ReadAsync())
        {
            names.Add(reader.GetString(0));
        }

        return names.OrderBy(name => name, StringComparer.Ordinal).ToList();
    }

    private async Task CreateTablesAsync()
    {
        foreach (var suffix in new[] { string.Empty, "_20250101000000", "_20250102000000", "_20250103000000" })
        {
            await ExecuteAsync($"""
                CREATE TABLE [{Table}{suffix}] (
                    [Id] UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
                    [ValidFrom] DATETIME2 GENERATED ALWAYS AS ROW START HIDDEN,
                    [ValidTo] DATETIME2 GENERATED ALWAYS AS ROW END HIDDEN,
                    PERIOD FOR SYSTEM_TIME(ValidFrom, ValidTo)
                ) WITH (SYSTEM_VERSIONING = ON (HISTORY_TABLE = dbo.[{Table}_History{suffix}]));
                """);
        }

        await ExecuteAsync($"CREATE TABLE [{StorageExportEntitiesJobBase.GetExportHistoryTableName(_streamId)}] ([Id] INT)");
    }

    private async Task ExecuteAsync(string sql)
    {
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();
        await using var command = new SqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync();
    }
}
