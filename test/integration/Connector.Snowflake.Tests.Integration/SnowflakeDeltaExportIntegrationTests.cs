using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using System.Threading.Tasks;

using Castle.MicroKernel.Registration;
using Castle.Windsor;

using CluedIn.ComponentHealth.Services;
using CluedIn.ComponentHealth.Services.Models;
using CluedIn.ComponentHealth.Storage;
using CluedIn.Connector.FileStorage.Common;
using CluedIn.Connector.FileStorage.Common.Connector;
using CluedIn.Connector.Snowflake.Connector;
using CluedIn.Connector.Snowflake.Connector.Snowpipe;
using CluedIn.Core;
using CluedIn.Core.Accounts;
using CluedIn.Core.Caching;
using CluedIn.Core.Connectors;
using CluedIn.Core.Data;
using CluedIn.Core.Data.Parts;
using CluedIn.Core.Data.Relational;
using CluedIn.Core.DataStore;
using CluedIn.Core.Events;
using CluedIn.Core.Streams;
using CluedIn.Core.Streams.Models;
using CluedIn.Streams.StreamLog;

using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

using Moq;

using Xunit;

using ExecutionContext = CluedIn.Core.ExecutionContext;
using ProviderDefinition = CluedIn.Core.Data.Relational.ProviderDefinition;

namespace CluedIn.Connector.Snowflake.Tests.Integration;

/// <summary>
/// Verifies that successive exports only ship the delta (rows actually changed since the
/// last successful export) rather than a full re-send of every row in the stream cache -
/// covering new inserts, an update to an existing entity, and a deletion. Mirrors the delta
/// guarantee FabricOpenMirroringConnectorTests exercises for the OpenMirroring connector,
/// adapted to Snowflake's MERGE-into-target-table model: since a MERGE is idempotent, the
/// target table's final contents alone can't distinguish a delta-only run from a full
/// resend, so each run's actual row count is read directly off the SQL Server export
/// history table (TotalRows) as the proof, in addition to asserting the target table's
/// contents are correct after each run.
///
/// This does not extend StorageConnectorTestsBase: that base class's own inherited [Fact]s
/// (VerifyStoreData_Sync_WithStreamCacheCanHandleMultipleUpdates, etc.) assert exact CSV/
/// Parquet file contents against a fixed column set, which doesn't apply to Snowflake -
/// there is no output file, and its dynamic, upper-cased, one-column-per-property table
/// shape doesn't match those fixed expectations. So this test builds its own minimal
/// Castle Windsor/Moq harness (mirroring StorageConnectorTestsBase.SetupContainer) rather
/// than inheriting a base whose generic tests would fail for an unrelated reason.
///
/// Requires the same environment variables as SnowflakeApiClientIntegrationTests
/// (SNOWFLAKE_USER, SNOWFLAKE_PRIVATE_KEY, etc.; see SnowflakeTestCredentials) plus
/// INTEGRATIONTEST_STREAMCACHE (a base64-encoded SQL Server connection string), matching
/// every other FileStorage.Common connector's integration test convention. Skips instead of
/// failing when either is absent.
/// </summary>
public class SnowflakeDeltaExportIntegrationTests
{
    private readonly ITestOutputHelper _testOutputHelper;

    public SnowflakeDeltaExportIntegrationTests(ITestOutputHelper testOutputHelper)
    {
        _testOutputHelper = testOutputHelper ?? throw new ArgumentNullException(nameof(testOutputHelper));
    }

    [Fact]
    public async Task Export_OnlySendsDeltaRows_ForInsertsUpdatesAndDeletes()
    {
        if (!SnowflakeTestCredentials.IsAvailable)
        {
            DynamicSkip.Request(SnowflakeTestCredentials.SkipReason);
            return;
        }

        var streamCacheConnectionStringEncoded = Environment.GetEnvironmentVariable("INTEGRATIONTEST_STREAMCACHE");
        if (string.IsNullOrWhiteSpace(streamCacheConnectionStringEncoded))
        {
            DynamicSkip.Request("INTEGRATIONTEST_STREAMCACHE environment variable is not set.");
            return;
        }

        var streamCacheConnectionString = Encoding.UTF8.GetString(Convert.FromBase64String(streamCacheConnectionStringEncoded));
        var setup = await SetupAsync(streamCacheConnectionString);

        try
        {
            var entityA = Guid.NewGuid();
            var entityB = Guid.NewGuid();
            var entityC = Guid.NewGuid();
            var entityD = Guid.NewGuid();

            // Run 1 (initial export): three new entities - every row is "new", so the whole
            // set is expected to go out.
            await StoreAsync(setup, entityA, "delta-test-a", "Car A", persistVersion: 1, VersionChangeType.Added);
            await StoreAsync(setup, entityB, "delta-test-b", "Car B", persistVersion: 1, VersionChangeType.Added);
            await StoreAsync(setup, entityC, "delta-test-c", "Car C", persistVersion: 1, VersionChangeType.Added);

            var totalRun1 = await RunExportAsync(setup);
            Assert.Equal(3, totalRun1);
            await AssertTargetRowAsync(setup, entityA, "Car A");
            await AssertTargetRowAsync(setup, entityB, "Car B");
            await AssertTargetRowAsync(setup, entityC, "Car C");

            // Run 2 (new insert): only entity D is new since run 1 - the delta must be that
            // one row, not all four.
            await StoreAsync(setup, entityD, "delta-test-d", "Car D", persistVersion: 1, VersionChangeType.Added);

            var totalRun2 = await RunExportAsync(setup);
            Assert.Equal(1, totalRun2);
            await AssertTargetRowAsync(setup, entityD, "Car D");
            await AssertTargetRowAsync(setup, entityA, "Car A");
            await AssertTargetRowAsync(setup, entityB, "Car B");
            await AssertTargetRowAsync(setup, entityC, "Car C");

            // Run 3 (update): only entity A changed since run 2 - the delta must be that one
            // row, not all four, and the MERGE must update it in place rather than duplicate it.
            await StoreAsync(setup, entityA, "delta-test-a", "Car A Updated", persistVersion: 2, VersionChangeType.Changed);

            var totalRun3 = await RunExportAsync(setup);
            Assert.Equal(1, totalRun3);
            await AssertTargetRowAsync(setup, entityA, "Car A Updated");
            await AssertTargetRowAsync(setup, entityB, "Car B");
            await AssertTargetRowAsync(setup, entityC, "Car C");
            await AssertTargetRowAsync(setup, entityD, "Car D");

            // Run 4 (deletion): only entity B was removed since run 3 - the delta must be
            // that one row, not all four, and the MERGE must delete it from the target
            // rather than leave it or touch anything else.
            await StoreAsync(setup, entityB, "delta-test-b", "Car B", persistVersion: 2, VersionChangeType.Removed);

            var totalRun4 = await RunExportAsync(setup);
            Assert.Equal(1, totalRun4);
            await AssertRowAbsentAsync(setup, entityB);
            await AssertTargetRowAsync(setup, entityA, "Car A Updated");
            await AssertTargetRowAsync(setup, entityC, "Car C");
            await AssertTargetRowAsync(setup, entityD, "Car D");
        }
        finally
        {
            await CleanupAsync(setup);
        }
    }

    // Verifies TableName pattern support (SnowflakeConfigurationConstants.TableName /
    // SnowflakeExportEntitiesJob/SnowflakeConnector.ArchiveContainer): the target table
    // lands at the resolved name, and - since GetTransientTableName/GetPipeName derive from
    // the resolved name, not the raw pattern - so do the transient table and pipe.
    [Fact]
    public async Task TableNamePattern_ResolvesConsistentlyForTargetTransientAndPipe()
    {
        if (!SnowflakeTestCredentials.IsAvailable)
        {
            DynamicSkip.Request(SnowflakeTestCredentials.SkipReason);
            return;
        }

        var streamCacheConnectionStringEncoded = Environment.GetEnvironmentVariable("INTEGRATIONTEST_STREAMCACHE");
        if (string.IsNullOrWhiteSpace(streamCacheConnectionStringEncoded))
        {
            DynamicSkip.Request("INTEGRATIONTEST_STREAMCACHE environment variable is not set.");
            return;
        }

        var streamCacheConnectionString = Encoding.UTF8.GetString(Convert.FromBase64String(streamCacheConnectionStringEncoded));
        var uniqueSuffix = Guid.NewGuid().ToString("N").ToUpperInvariant();
        var tableNamePattern = $"XUNIT_PATTERN_{{ContainerName}}_{uniqueSuffix}";
        var setup = await SetupAsync(streamCacheConnectionString, tableNamePattern);

        // Mirrors exactly what SnowflakeExportEntitiesJob resolves (and upper-cases)
        // TableName to - SetupAsync always sets the stream's ContainerName to "test".
        var expectedResolvedTableName = $"XUNIT_PATTERN_{setup.StreamModel.ContainerName}_{uniqueSuffix}".ToUpperInvariant();
        var expectedTransientTableName = SnowflakeConnectorConfiguration.GetTransientTableName(expectedResolvedTableName);
        var expectedPipeName = SnowflakeConnectorConfiguration.GetPipeName(expectedResolvedTableName);

        try
        {
            var entityId = Guid.NewGuid();
            await StoreAsync(setup, entityId, "pattern-test-a", "Car A", persistVersion: 1, VersionChangeType.Added);

            var total = await RunExportAsync(setup);
            Assert.Equal(1, total);

            var qualifiedTarget = SnowflakeSqlBuilder.QualifiedName(setup.Configuration.Database, setup.Configuration.Schema, expectedResolvedTableName);
            var idColumn = SnowflakeSqlBuilder.SanitizeColumnName(StorageConfigurationConstants.IdKey);
            var nameColumn = SnowflakeSqlBuilder.SanitizeColumnName("name");
            var targetResult = await setup.ApiClient.ExecuteStatementAsync($"SELECT {nameColumn} FROM {qualifiedTarget} WHERE {idColumn} = '{entityId}'");
            var row = Assert.Single(targetResult.Rows);
            Assert.Equal("Car A", Assert.Single(row));

            var transientResult = await setup.ApiClient.ExecuteStatementAsync(
                SnowflakeSqlBuilder.ShowTablesLikeInSchema(setup.Configuration.Database, setup.Configuration.Schema, expectedTransientTableName));
            Assert.Single(transientResult.Rows);

            var pipeResult = await setup.ApiClient.ExecuteStatementAsync(
                SnowflakeSqlBuilder.ShowPipesLikeInSchema(setup.Configuration.Database, setup.Configuration.Schema, expectedPipeName));
            Assert.Single(pipeResult.Rows);
        }
        finally
        {
            try
            {
                await setup.ApiClient.ExecuteStatementAsync(
                    SnowflakeSqlBuilder.DropPipeIfExists(setup.Configuration.Database, setup.Configuration.Schema, expectedPipeName));
                await setup.ApiClient.ExecuteStatementAsync(
                    SnowflakeSqlBuilder.DropTableIfExists(setup.Configuration.Database, setup.Configuration.Schema, expectedTransientTableName));
                await setup.ApiClient.ExecuteStatementAsync(
                    SnowflakeSqlBuilder.DropTableIfExists(setup.Configuration.Database, setup.Configuration.Schema, expectedResolvedTableName));
            }
            catch (Exception ex)
            {
                _testOutputHelper.WriteLine($"Best-effort Snowflake cleanup failed: {ex.Message}");
            }
            finally
            {
                (setup.ApiClient as IDisposable)?.Dispose();
            }

            try
            {
                await DeleteCacheTablesAsync(setup.StreamModel.Id, setup.StreamCacheConnectionString);
            }
            catch (Exception ex)
            {
                _testOutputHelper.WriteLine($"Best-effort SQL Server cleanup failed: {ex.Message}");
            }
        }
    }

    private async Task<long?> RunExportAsync(TestSetup setup)
    {
        var jobArgs = new StorageJobArgs
        {
            OrganizationId = setup.Organization.Id.ToString(),
            Schedule = "0 0/1 * * *",
            Message = setup.StreamModel.Id.ToString(),
            IsTriggeredFromJobServer = false,
        };

        var result = await setup.ExportJob.DoRunInternalAsync(setup.Context, jobArgs);
        _testOutputHelper.WriteLine($"Export result: HasExported={result.HasExported}, Reason={result.Reason}, FilePath={result.FilePath}");
        Assert.True(result.HasExported, $"Export did not run. Reason: {result.Reason}");

        return await GetLastExportTotalRowsAsync(setup);
    }

    private static async Task<long?> GetLastExportTotalRowsAsync(TestSetup setup)
    {
        var tableName = CacheTableHelper.GetExportHistoryTableName(setup.StreamModel.Id) + "_ExportHistory";
        await using var connection = new SqlConnection(setup.StreamCacheConnectionString);
        await connection.OpenAsync();

        var sql = $"SELECT TOP (1) TotalRows, Status FROM [{tableName}] ORDER BY StartTime DESC";
        using var command = new SqlCommand(sql, connection) { CommandType = CommandType.Text };
        using var reader = await command.ExecuteReaderAsync();
        if (!await reader.ReadAsync())
        {
            return null;
        }

        var status = reader["Status"] as string;
        Assert.Equal("Complete", status);
        return reader["TotalRows"] is DBNull ? null : Convert.ToInt64(reader["TotalRows"]);
    }

    private static async Task StoreAsync(
        TestSetup setup,
        Guid entityId,
        string code,
        string name,
        int persistVersion,
        VersionChangeType changeType)
    {
        var entity = CreateEntity(entityId, code, name, persistVersion, changeType);
        await setup.Connector.StoreData(setup.Context, setup.StreamModel, entity);
    }

    private static ConnectorEntityData CreateEntity(
        Guid entityId,
        string code,
        string name,
        int persistVersion,
        VersionChangeType changeType)
    {
        var originCode = EntityCode.FromKey($"/TestEntity#Acceptance:{code}");
        return new ConnectorEntityData(
            changeType,
            StreamMode.Sync,
            entityId,
            new ConnectorEntityPersistInfo($"hash-{persistVersion}", persistVersion),
            null,
            originCode,
            "/TestEntity",
            [
                new ConnectorPropertyData("name", name, new EntityPropertyConnectorPropertyDataType(typeof(string))),
            ],
            [originCode],
            [],
            []);
    }

    private async Task AssertTargetRowAsync(TestSetup setup, Guid entityId, string expectedName)
    {
        var qualified = SnowflakeSqlBuilder.QualifiedName(setup.Configuration.Database, setup.Configuration.Schema, setup.Configuration.TableName);
        var idColumn = SnowflakeSqlBuilder.SanitizeColumnName(StorageConfigurationConstants.IdKey);
        var nameColumn = SnowflakeSqlBuilder.SanitizeColumnName("name");
        var sql = $"SELECT {nameColumn} FROM {qualified} WHERE {idColumn} = '{entityId}'";

        var result = await setup.ApiClient.ExecuteStatementAsync(sql);
        var row = Assert.Single(result.Rows);
        Assert.Equal(expectedName, Assert.Single(row));
    }

    private async Task AssertRowAbsentAsync(TestSetup setup, Guid entityId)
    {
        var qualified = SnowflakeSqlBuilder.QualifiedName(setup.Configuration.Database, setup.Configuration.Schema, setup.Configuration.TableName);
        var idColumn = SnowflakeSqlBuilder.SanitizeColumnName(StorageConfigurationConstants.IdKey);
        var sql = $"SELECT {idColumn} FROM {qualified} WHERE {idColumn} = '{entityId}'";

        var result = await setup.ApiClient.ExecuteStatementAsync(sql);
        Assert.Empty(result.Rows);
    }

    private async Task CleanupAsync(TestSetup setup)
    {
        try
        {
            await setup.ApiClient.ExecuteStatementAsync(
                SnowflakeSqlBuilder.DropPipeIfExists(setup.Configuration.Database, setup.Configuration.Schema, SnowflakeConnectorConfiguration.GetPipeName(setup.Configuration.TableName)));
            await setup.ApiClient.ExecuteStatementAsync(
                SnowflakeSqlBuilder.DropTableIfExists(setup.Configuration.Database, setup.Configuration.Schema, SnowflakeConnectorConfiguration.GetTransientTableName(setup.Configuration.TableName)));
            await setup.ApiClient.ExecuteStatementAsync(
                SnowflakeSqlBuilder.DropTableIfExists(setup.Configuration.Database, setup.Configuration.Schema, setup.Configuration.TableName));
        }
        catch (Exception ex)
        {
            _testOutputHelper.WriteLine($"Best-effort Snowflake cleanup failed: {ex.Message}");
        }
        finally
        {
            (setup.ApiClient as IDisposable)?.Dispose();
        }

        try
        {
            await DeleteCacheTablesAsync(setup.StreamModel.Id, setup.StreamCacheConnectionString);
        }
        catch (Exception ex)
        {
            _testOutputHelper.WriteLine($"Best-effort SQL Server cleanup failed: {ex.Message}");
        }
    }

    // Mirrors StorageConnectorTestsBase.DeleteTable exactly - system-versioned temporal
    // tables must have SYSTEM_VERSIONING turned off before they (and their _History
    // counterpart) can be dropped.
    private static async Task DeleteCacheTablesAsync(Guid streamId, string streamCacheConnectionString)
    {
        const int notTemporalTableErrorCode = 13591;
        var tableName = CacheTableHelper.GetCacheTableName(streamId);
        var deleteTableSql = $"""
            IF EXISTS (SELECT * FROM SYSOBJECTS WHERE NAME='{tableName}' AND XTYPE='U')
            ALTER TABLE dbo.[{tableName}]  SET ( SYSTEM_VERSIONING = Off )

            DROP TABLE IF EXISTS [{tableName}];
            DROP TABLE IF EXISTS [{tableName}_History];
            DROP TABLE IF EXISTS [{CacheTableHelper.GetExportHistoryTableName(streamId)}_ExportHistory];
            """;
        try
        {
            await ExecuteAsync(deleteTableSql, streamCacheConnectionString);
        }
        catch (SqlException ex) when (ex.Number == notTemporalTableErrorCode)
        {
            var deleteTableSeparatelySql = $"""
                IF EXISTS (SELECT * FROM SYSOBJECTS WHERE NAME='{tableName}' AND XTYPE='U')
                DROP TABLE IF EXISTS [{tableName}];
                DROP TABLE IF EXISTS [{tableName}_History];
                DROP TABLE IF EXISTS [{CacheTableHelper.GetExportHistoryTableName(streamId)}_ExportHistory];
                """;
            await ExecuteAsync(deleteTableSeparatelySql, streamCacheConnectionString);
        }

        static async Task ExecuteAsync(string sql, string connectionString)
        {
            await using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();
            var command = new SqlCommand(sql, connection) { CommandType = CommandType.Text };
            _ = await command.ExecuteNonQueryAsync();
        }
    }

    // A condensed, Snowflake-specific stand-in for StorageConnectorTestsBase.SetupContainer -
    // real SnowflakeConnector/SnowflakeExportEntitiesJob instances wired against a real
    // Windsor container, with only the handful of dependencies those types actually touch
    // mocked out (stream repository, component health, provider definition/organization
    // plumbing that ExecutionContext/Organization need to construct).
    private async Task<TestSetup> SetupAsync(string streamCacheConnectionString, string tableNamePattern = null)
    {
        var organizationId = Guid.NewGuid();
        var providerDefinitionId = Guid.NewGuid();

        var container = new WindsorContainer();
        var applicationContext = new ApplicationContext(container);

        container.Register(Component.For<ISystemEvents>().Instance(new Mock<ISystemEvents>().Object));

        var streamLogServiceMock = new Mock<IStreamLogService>();
        streamLogServiceMock
            .Setup(s => s.StoreHistoryLogEntryAsync(It.IsAny<CluedIn.Streams.StreamLog.History.StreamHistoryLogHistoryModel>()))
            .Returns(Task.CompletedTask);
        container.Register(Component.For<IStreamLogService>().Instance(streamLogServiceMock.Object));

        var componentHealthServiceMock = new Mock<IComponentHealthService>();
        componentHealthServiceMock
            .Setup(service => service.GetComponentHealth(It.IsAny<ExecutionContext>(), It.IsAny<ComponentArea>(), It.IsAny<Guid>()))
            .ReturnsAsync(new ComponentHealthModel { Status = ComponentHealthStatus.Healthy });
        container.Register(Component.For<IComponentHealthService>().Instance(componentHealthServiceMock.Object));

        // Real wall-clock time rather than a fixed mocked instant: StoreData's temporal
        // cache-table ValidFrom and the export's asOfTime (UseCurrentTimeForExport=true, see
        // CreateConfigurationDictionary) both come from GetUtcNow(), so as long as every
        // StoreAsync call happens-before its corresponding RunExportAsync call in real time -
        // guaranteed, since they're sequential awaited calls below - the delta window lines
        // up correctly without needing StorageConnectorTestsBase's ModifyHistoryTimeToBeCurrentTime
        // SQL rewrite.
        var timeProviderMock = new Mock<ITimeProvider>();
        timeProviderMock.Setup(x => x.GetUtcNow()).Returns(() => DateTimeOffset.UtcNow);

        var cache = new Mock<InMemoryApplicationCache>(MockBehavior.Loose, container) { CallBase = true };
        container.Register(Component.For<IApplicationCache>().Instance(cache.Object));

        var providerDefinitionDataStore = new Mock<IRelationalDataStore<ProviderDefinition>>();
        var providerDefinition = new ProviderDefinition
        {
            IsEnabled = true,
            ProviderId = SnowflakeConfigurationConstants.SnowflakeProviderId,
            Id = providerDefinitionId,
        };
        providerDefinitionDataStore.Setup(store => store.GetByIdAsync(It.IsAny<ExecutionContext>(), providerDefinitionId))
            .ReturnsAsync(providerDefinition);
        container.Register(Component.For<IRelationalDataStore<ProviderDefinition>>().Instance(providerDefinitionDataStore.Object));

        var systemConnectionStrings = new Mock<ISystemConnectionStrings>();
        container.Register(Component.For<ISystemConnectionStrings>().Instance(systemConnectionStrings.Object));
        container.Register(Component.For<SystemContext>().Instance(new SystemContext(container)));
        container.Register(Component.For<ILogger<OrganizationDataStores>>().Instance(new Mock<ILogger<OrganizationDataStores>>().Object));

        var organizationDataShard = new Mock<IOrganizationDataShard>();
        systemConnectionStrings.Setup(x => x.SystemOrganizationDataShard).Returns(organizationDataShard.Object);

        var organization = new Organization(applicationContext, organizationId);
        var organizationRepository = new Mock<IOrganizationRepository>();
        organizationRepository.Setup(x => x.GetOrganization(It.IsAny<ExecutionContext>(), It.IsAny<Guid>())).Returns(organization);
        container.Register(Component.For<IOrganizationRepository>().Instance(organizationRepository.Object));

        var executionContextLogger = new Mock<ILogger<ExecutionContext>>();
        container.Register(Component.For<ILogger<ExecutionContext>>().Instance(executionContextLogger.Object));
        var context = new ExecutionContext(applicationContext, organization, executionContextLogger.Object);

        var streamModel = new StreamModel
        {
            Id = Guid.NewGuid(),
            ConnectorProviderDefinitionId = providerDefinitionId,
            ContainerName = "test",
            Mode = StreamMode.Sync,
            ExportIncomingEdges = true,
            ExportOutgoingEdges = true,
            Status = StreamStatus.Started,
            OrganizationId = organization.Id,
        };
        var streamRepositoryMock = new Mock<IStreamRepository>();
        // IStreamRepository.GetStream gained an ExecutionContext parameter in CluedIn 4.7.0 -
        // see StorageConnectorTestsBase.v46.cs/.v47_to_Latest.cs for the same split.
#if CLUEDIN_V47_OR_GREATER
        streamRepositoryMock.Setup(x => x.GetStream(It.IsAny<ExecutionContext>(), streamModel.Id)).ReturnsAsync(streamModel);
#else
        streamRepositoryMock.Setup(x => x.GetStream(streamModel.Id)).ReturnsAsync(streamModel);
#endif
        container.Register(Component.For<IStreamRepository>().Instance(streamRepositoryMock.Object));

        var constantsMock = new Mock<ISnowflakeConfigurationConstants>();
        constantsMock.Setup(x => x.CacheRecordsThresholdKeyName).Returns("abc");
        constantsMock.Setup(x => x.CacheRecordsThresholdDefaultValue).Returns(50);
        constantsMock.Setup(x => x.CacheSyncIntervalKeyName).Returns("abc");
        constantsMock.Setup(x => x.CacheSyncIntervalDefaultValue).Returns(2000);
        constantsMock.Setup(x => x.CacheBufferStrategyKeyName).Returns("CacheBufferStrategyKeyName");
        constantsMock.Setup(x => x.CacheBufferStrategyDefaultValue).Returns(nameof(BufferStrategy.Safe));
        constantsMock.Setup(x => x.HealthCheckErrorLogIntervalKeyName).Returns("HealthCheckErrorLogInterval");
        constantsMock.Setup(x => x.HealthCheckErrorLogIntervalDefaultValue).Returns(0);
        constantsMock.Setup(x => x.ProviderId).Returns(SnowflakeConfigurationConstants.SnowflakeProviderId);

        var tableName = tableNamePattern ?? $"CLUEDIN_DELTA_TEST_{Guid.NewGuid():N}".ToUpperInvariant();
        var configurationDictionary = CreateConfigurationDictionary(tableName, streamCacheConnectionString);
        var configuration = new SnowflakeConnectorConfiguration(configurationDictionary, "test");
        var apiClient = new SnowflakeApiClient(SnowflakeConnectionSettings.FromConfiguration(configuration));

        var storageFactoryMock = new Mock<SnowflakeStorageFactory>();
        storageFactoryMock
            .Setup(x => x.CreateStorageClient(It.IsAny<ExecutionContext>(), It.IsAny<IStorageConfiguration>()))
            .Returns<ExecutionContext, IStorageConfiguration>((_, data) =>
            {
                var snowflakeConfiguration = (SnowflakeConnectorConfiguration)data;
                var clientApiClient = new SnowflakeApiClient(SnowflakeConnectionSettings.FromConfiguration(snowflakeConfiguration));
                return Task.FromResult<IStorageClient>(new SnowflakeStorageClient(
                    NullLogger<SnowflakeStorageClient>.Instance,
                    NullLoggerFactory.Instance,
                    snowflakeConfiguration,
                    clientApiClient));
            });
        storageFactoryMock.Setup(x => x.CreateStorageConfiguration(It.IsAny<ExecutionContext>(), It.IsAny<IReadOnlyStreamModel>()))
            .ReturnsAsync(configuration);
        storageFactoryMock.Setup(x => x.CreateStorageConfiguration(It.IsAny<ExecutionContext>(), It.IsAny<Guid>()))
            .ReturnsAsync(configuration);

        var connector = new SnowflakeConnector(
            NullLogger<SnowflakeConnector>.Instance,
            applicationContext,
            constantsMock.Object,
            storageFactoryMock.Object,
            timeProviderMock.Object);

        var exportJob = new SnowflakeExportEntitiesJob(
            applicationContext,
            streamRepositoryMock.Object,
            constantsMock.Object,
            storageFactoryMock.Object,
            timeProviderMock.Object);

        return new TestSetup(context, connector, streamModel, organization, configuration, exportJob, apiClient, streamCacheConnectionString);
    }

    private static Dictionary<string, object> CreateConfigurationDictionary(string tableName, string streamCacheConnectionString)
    {
        var credentials = SnowflakeTestCredentials.Load();
        return new Dictionary<string, object>
        {
            { SnowflakeConfigurationConstants.Account, credentials.Account },
            { SnowflakeConfigurationConstants.User, credentials.User },
            { SnowflakeConfigurationConstants.PrivateKey, credentials.PrivateKeyPem },
            { SnowflakeConfigurationConstants.PrivateKeyPassphrase, credentials.PrivateKeyPassphrase },
            { SnowflakeConfigurationConstants.Database, credentials.Database },
            { SnowflakeConfigurationConstants.Schema, credentials.Schema },
            { SnowflakeConfigurationConstants.Warehouse, credentials.Warehouse },
            { SnowflakeConfigurationConstants.Role, credentials.Role },
            { SnowflakeConfigurationConstants.TableName, tableName },
            { nameof(StorageConfigurationConstants.IsStreamCacheEnabled), true },
            { nameof(StorageConfigurationConstants.StreamCacheConnectionString), streamCacheConnectionString },
            { nameof(StorageConfigurationConstants.OutputFormat), "csv" },
            { nameof(StorageConfigurationConstants.UseCurrentTimeForExport), true },
            { nameof(StorageConfigurationConstants.Schedule), "0 0/1 * * *" },
            { nameof(StorageConfigurationConstants.ContainerName), "test" },
        };
    }

    private sealed record TestSetup(
        ExecutionContext Context,
        SnowflakeConnector Connector,
        StreamModel StreamModel,
        Organization Organization,
        SnowflakeConnectorConfiguration Configuration,
        SnowflakeExportEntitiesJob ExportJob,
        SnowflakeApiClient ApiClient,
        string StreamCacheConnectionString);
}
