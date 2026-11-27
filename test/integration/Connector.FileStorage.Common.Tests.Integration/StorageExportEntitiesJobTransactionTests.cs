using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Transactions;

using Castle.MicroKernel.Registration;
using Castle.Windsor;

using CluedIn.ComponentHealth.Services;
using CluedIn.ComponentHealth.Services.Models;
using CluedIn.ComponentHealth.Storage;
using CluedIn.Connector.FileStorage.Common.Connector;
using CluedIn.Connector.FileStorage.Common.Connector.SqlDataWriter;
using CluedIn.Core;
using CluedIn.Core.Accounts;
using CluedIn.Core.Caching;
using CluedIn.Core.Data.Relational;
using CluedIn.Core.DataStore;
using CluedIn.Core.Events;
using CluedIn.Core.Streams;
using CluedIn.Core.Streams.Models;
using CluedIn.Streams.StreamLog;

using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

using Moq;

using Xunit;

using ExecutionContext = CluedIn.Core.ExecutionContext;
using ProviderDefinition = CluedIn.Core.Data.Relational.ProviderDefinition;

namespace CluedIn.Connector.FileStorage.Common.Tests.Integration;

/// <summary>
/// Runs the real <see cref="StorageExportEntitiesJobBase"/> against a real SQL Server stream cache, with the storage
/// target replaced by an in-memory fake, to verify how the export job uses transactions and its history table.
///
/// The export used to run inside one ambient TransactionScope (TimeSpan.MaxValue, silently capped to
/// <see cref="TransactionManager.MaximumTimeout"/>). On a long export the transaction was ended part-way through, the
/// final history update then failed with "The transaction associated with the current connection has completed but has
/// not been disposed", the history row was rolled back and the whole export was repeated.
///
/// Requires INTEGRATIONTEST_STREAMCACHE (a base64-encoded SQL Server connection string), like every other integration
/// test in this repository.
/// </summary>
[Collection(CollectionName)]
public class StorageExportEntitiesJobTransactionTests
{
    // These tests change the process-wide TransactionManager.MaximumTimeout, so they must not run in parallel with each other.
    private const string CollectionName = "StorageExportEntitiesJobTransactionTests";
    private const string Schedule = "0 0/1 * * *";
    private const int RowCount = 3;

    [Fact]
    public async Task Export_RunsWithoutAmbientTransaction_AndRecordsCompleteHistory()
    {
        await using var harness = await ExportHarness.CreateAsync();

        var result = await harness.RunExportAsync();

        Assert.True(result.HasExported, $"Export did not run. Reason: {result.Reason}");
        Assert.Null(harness.Job.Writer.AmbientTransactionWhileWriting);
        Assert.True(harness.Job.Writer.WasCalled);
        Assert.True(
            harness.Job.OutputTargetWasInitializedInAmbientTransaction,
            "Initializing the output target should run in its own short transaction (overrides may take transaction-owned application locks).");

        var history = Assert.Single(await harness.ReadHistoryAsync());
        Assert.Equal("Complete", history.Status);
        Assert.Equal(RowCount, history.TotalRows);
        Assert.NotNull(history.EndTime);
        Assert.Single(harness.Storage.Files.Keys, key => key.EndsWith(".csv", StringComparison.OrdinalIgnoreCase));
        Assert.DoesNotContain(harness.Storage.Files.Keys, key => key.EndsWith(".tmp", StringComparison.OrdinalIgnoreCase));
    }

    // TransactionManager.MaximumTimeout and TransactionManager.DefaultTimeout are read-only on net6.0 (the CluedIn 4.x
    // lines), so the two tests that shrink them only exist where they can be set. The export code under test is the same
    // on every line, and the other tests in this class run on all of them.
#if !NET6_0
    [Fact]
    public async Task Export_RunningLongerThanTheTransactionTimeout_StillRecordsCompleteHistory()
    {
        // The production failure only happened after System.Transactions' maximum timeout (10 minutes by default) had
        // elapsed. Shrink it so the same thing can be reproduced in seconds.
        var originalMaximumTimeout = TransactionManager.MaximumTimeout;
        TransactionManager.MaximumTimeout = TimeSpan.FromSeconds(2);
        try
        {
            await using var harness = await ExportHarness.CreateAsync();
            harness.Job.Writer.DelayWhileWriting = TimeSpan.FromSeconds(5);

            var result = await harness.RunExportAsync();

            Assert.True(result.HasExported, $"Export did not run. Reason: {result.Reason}");
            var history = Assert.Single(await harness.ReadHistoryAsync());
            Assert.Equal("Complete", history.Status);
            Assert.Equal(RowCount, history.TotalRows);
        }
        finally
        {
            TransactionManager.MaximumTimeout = originalMaximumTimeout;
        }
    }
#endif

    [Fact]
    public async Task Export_HoldsThePerStreamLockForTheWholeExport()
    {
        await using var harness = await ExportHarness.CreateAsync();
        StorageExportEntitiesJobBase.ExportResult concurrentResult = null;
        harness.Job.Writer.WhileWriting = async () => concurrentResult = await harness.RunExportAsync();

        var result = await harness.RunExportAsync();

        Assert.True(result.HasExported, $"Export did not run. Reason: {result.Reason}");
        Assert.NotNull(concurrentResult);
        Assert.False(concurrentResult.HasExported);
        Assert.Equal("Failed to acquire lock", concurrentResult.Reason);
        var history = Assert.Single(await harness.ReadHistoryAsync());
        Assert.Equal("Complete", history.Status);
    }

    [Fact]
    public async Task Export_WhenWritingFails_MarksHistoryFailed_AndRethrows()
    {
        await using var harness = await ExportHarness.CreateAsync();
        harness.Job.Writer.ExceptionToThrow = new InvalidOperationException("Simulated failure while writing the export file.");

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => harness.RunExportAsync());

        Assert.Equal("Simulated failure while writing the export file.", exception.Message);
        var history = Assert.Single(await harness.ReadHistoryAsync());
        Assert.Equal("Failed", history.Status);
        Assert.NotNull(history.EndTime);
        Assert.Null(history.TotalRows);
    }

    [Fact]
    public async Task Export_RetryAfterFailure_ReusesTheHistoryRowOfTheSameSlot()
    {
        await using var harness = await ExportHarness.CreateAsync();
        harness.Job.Writer.ExceptionToThrow = new InvalidOperationException("Simulated failure while writing the export file.");
        await Assert.ThrowsAsync<InvalidOperationException>(() => harness.RunExportAsync());
        Assert.Equal("Failed", Assert.Single(await harness.ReadHistoryAsync()).Status);

        harness.Job.Writer.ExceptionToThrow = null;
        var result = await harness.RunExportAsync();

        Assert.True(result.HasExported, $"Retry did not run. Reason: {result.Reason}");
        var history = Assert.Single(await harness.ReadHistoryAsync());
        Assert.Equal("Complete", history.Status);
        Assert.Equal(RowCount, history.TotalRows);
    }

    [Fact]
    public async Task Export_RetryAfterRunWasKilledMidExport_ReusesTheStartingRow()
    {
        await using var harness = await ExportHarness.CreateAsync();
        harness.Job.Writer.ExceptionToThrow = new InvalidOperationException("Simulated failure while writing the export file.");
        await Assert.ThrowsAsync<InvalidOperationException>(() => harness.RunExportAsync());

        // A pod that is stopped part-way through an export (for example by a deployment) never gets to mark its row as
        // Failed, so the row is left in Starting.
        await harness.ExecuteAsync($"UPDATE [{harness.HistoryTableName}] SET Status = 'Starting', EndTime = NULL");
        Assert.Equal("Starting", Assert.Single(await harness.ReadHistoryAsync()).Status);

        harness.Job.Writer.ExceptionToThrow = null;
        var result = await harness.RunExportAsync();

        Assert.True(result.HasExported, $"Retry did not run. Reason: {result.Reason}");
        var history = Assert.Single(await harness.ReadHistoryAsync());
        Assert.Equal("Complete", history.Status);
        Assert.Equal(RowCount, history.TotalRows);
    }

    [Fact]
    public async Task Export_StartingHistoryRow_IsVisibleToOtherConnectionsWhileTheExportRuns()
    {
        await using var harness = await ExportHarness.CreateAsync();
        IReadOnlyList<HistoryRow> historyWhileWriting = null;
        harness.Job.Writer.WhileWriting = async () => historyWhileWriting = await harness.ReadHistoryAsync();

        var result = await harness.RunExportAsync();

        Assert.True(result.HasExported, $"Export did not run. Reason: {result.Reason}");
        Assert.NotNull(historyWhileWriting);
        Assert.Equal("Starting", Assert.Single(historyWhileWriting).Status);
        Assert.Equal("Complete", Assert.Single(await harness.ReadHistoryAsync()).Status);
    }

    [Fact]
    public async Task Export_NeedsOnlyOnePooledConnection()
    {
        // An export that needs a second connection while it holds its first can deadlock itself: with enough concurrent
        // exports (or a small pool) every export holds one connection and waits for another that nobody can release. With
        // a pool of 1 the export can only succeed if the lock, the setup queries, the read and every history update share
        // one connection.
        await using var harness = await ExportHarness.CreateAsync(jobConnectionStringOptions: "Max Pool Size=1;Connect Timeout=3");

        var result = await harness.RunExportAsync();

        Assert.True(result.HasExported, $"Export did not run. Reason: {result.Reason}");
        var history = Assert.Single(await harness.ReadHistoryAsync());
        Assert.Equal("Complete", history.Status);
        Assert.Equal(RowCount, history.TotalRows);
    }

    [Fact]
    public async Task Export_ReleasesThePerStreamLockWhenItEnds()
    {
        // The lock is owned by the export connection's session, which goes back to the pool when the export ends, so it
        // has to be released explicitly.
        await using var harness = await ExportHarness.CreateAsync(jobConnectionStringOptions: "Max Pool Size=1");

        var result = await harness.RunExportAsync();

        Assert.True(result.HasExported, $"Export did not run. Reason: {result.Reason}");
        Assert.True(await harness.IsExportLockFreeAsync());
    }

    [Fact]
    public async Task Export_WhenWritingFails_ReleasesThePerStreamLock()
    {
        await using var harness = await ExportHarness.CreateAsync(jobConnectionStringOptions: "Max Pool Size=1");
        harness.Job.Writer.ExceptionToThrow = new InvalidOperationException("Simulated write failure");

        await Assert.ThrowsAsync<InvalidOperationException>(harness.RunExportAsync);

        Assert.True(await harness.IsExportLockFreeAsync());
    }

#if !NET6_0
    [Fact]
    public async Task Export_OutputTargetInitializationLongerThanTheDefaultTransactionTimeout_StillSucceeds()
    {
        // Initializing the output target runs in its own transaction and may call slow remote storage (Open Mirroring
        // creates directories and writes metadata files while holding a transaction-owned application lock). It needs an
        // explicit timeout rather than System.Transactions' default (1 minute), which would abort it mid-initialization.
        var originalDefaultTimeout = TransactionManager.DefaultTimeout;
        TransactionManager.DefaultTimeout = TimeSpan.FromSeconds(1);
        try
        {
            await using var harness = await ExportHarness.CreateAsync();
            harness.Job.OutputTargetInitializationDelay = TimeSpan.FromSeconds(3);

            var result = await harness.RunExportAsync();

            Assert.True(result.HasExported, $"Export did not run. Reason: {result.Reason}");
            Assert.True(harness.Job.OutputTargetWasInitializedInAmbientTransaction);
            var history = Assert.Single(await harness.ReadHistoryAsync());
            Assert.Equal("Complete", history.Status);
            Assert.Equal(RowCount, history.TotalRows);
        }
        finally
        {
            TransactionManager.DefaultTimeout = originalDefaultTimeout;
        }
    }
#endif

    private sealed record HistoryRow(string Status, int? TotalRows, DateTimeOffset? EndTime);

    private sealed class TestExportJob : StorageExportEntitiesJobBase
    {
        public TestExportJob(
            ApplicationContext appContext,
            IStreamRepository streamRepository,
            IStorageConfigurationConstants configurationConstants,
            IStorageFactory storageFactory,
            ITimeProvider timeProvider)
            : base(appContext, streamRepository, configurationConstants, storageFactory, timeProvider)
        {
        }

        public RecordingSqlDataWriter Writer { get; } = new();

        public bool OutputTargetWasInitializedInAmbientTransaction { get; private set; }

        public TimeSpan OutputTargetInitializationDelay { get; set; }

        protected override ISqlDataWriter GetSqlDataWriter(string outputFormat) => Writer;

        protected override async Task InitializeOutputTargetAsync(
            ExecutionContext context,
            SqlConnection connection,
            ExportJobData exportJobData,
            IStorageClient storageClient)
        {
            OutputTargetWasInitializedInAmbientTransaction = Transaction.Current != null;
            await base.InitializeOutputTargetAsync(context, connection, exportJobData, storageClient);
            if (OutputTargetInitializationDelay > TimeSpan.Zero)
            {
                await Task.Delay(OutputTargetInitializationDelay);
            }
        }
    }

    /// <summary>Reads every row of the export and writes one line per row, recording the state it is called in.</summary>
    private sealed class RecordingSqlDataWriter : ISqlDataWriter
    {
        public bool WasCalled { get; private set; }

        public Transaction AmbientTransactionWhileWriting { get; private set; }

        public TimeSpan DelayWhileWriting { get; set; }

        public Func<Task> WhileWriting { get; set; }

        public Exception ExceptionToThrow { get; set; }

        public async Task<long> WriteAsync(
            ExecutionContext context,
            IStorageConfiguration configuration,
            Stream outputStream,
            ICollection<string> fieldNames,
            bool isInitialExport,
            SqlDataReader reader)
        {
            WasCalled = true;
            AmbientTransactionWhileWriting = Transaction.Current;

            long rows = 0;
            while (await reader.ReadAsync())
            {
                rows++;
                var line = Encoding.UTF8.GetBytes($"{reader["Id"]},{reader["Name"]}{Environment.NewLine}");
                await outputStream.WriteAsync(line);
            }

            if (WhileWriting != null)
            {
                await WhileWriting();
            }

            if (DelayWhileWriting > TimeSpan.Zero)
            {
                await Task.Delay(DelayWhileWriting);
            }

            if (ExceptionToThrow != null)
            {
                throw ExceptionToThrow;
            }

            return rows;
        }
    }

    private sealed class ExportHarness : IAsyncDisposable
    {
        private readonly string _connectionString;
        private readonly Guid _streamId;
        private readonly ExecutionContext _context;
        private readonly string _cacheTableName;

        private ExportHarness(
            string connectionString,
            Guid streamId,
            ExecutionContext context,
            TestExportJob job,
            InMemoryStorage storage)
        {
            _connectionString = connectionString;
            _streamId = streamId;
            _context = context;
            Job = job;
            Storage = storage;
            _cacheTableName = CacheTableHelper.GetCacheTableName(streamId);
            HistoryTableName = StorageExportEntitiesJobBase.GetExportHistoryTableName(streamId);
        }

        public TestExportJob Job { get; }

        public InMemoryStorage Storage { get; }

        public string HistoryTableName { get; }

        /// <param name="jobConnectionStringOptions">
        /// Extra connection string options for the connection string the export job itself uses (for example a small
        /// "Max Pool Size"). Its connection pool is separate from the one the harness uses to set up and inspect the database.
        /// </param>
        public static async Task<ExportHarness> CreateAsync(string jobConnectionStringOptions = null)
        {
            var encodedConnectionString = Environment.GetEnvironmentVariable("INTEGRATIONTEST_STREAMCACHE");
            Assert.False(
                string.IsNullOrWhiteSpace(encodedConnectionString),
                "INTEGRATIONTEST_STREAMCACHE (a base64-encoded SQL Server connection string) is required.");
            var connectionString = Encoding.UTF8.GetString(Convert.FromBase64String(encodedConnectionString));

            var streamId = Guid.NewGuid();
            var cacheTableName = CacheTableHelper.GetCacheTableName(streamId);
            await ExecuteAsync(
                connectionString,
                $"""
                CREATE TABLE [{cacheTableName}] (
                    [Id] UNIQUEIDENTIFIER NOT NULL,
                    [Name] NVARCHAR(100) NULL,
                    [ValidFrom] DATETIME2(7) GENERATED ALWAYS AS ROW START NOT NULL,
                    [ValidTo] DATETIME2(7) GENERATED ALWAYS AS ROW END NOT NULL,
                    PERIOD FOR SYSTEM_TIME ([ValidFrom], [ValidTo]),
                    CONSTRAINT [PK_{cacheTableName}] PRIMARY KEY CLUSTERED ([Id])
                ) WITH (SYSTEM_VERSIONING = ON (HISTORY_TABLE = dbo.[{cacheTableName}_History]));
                """);
            for (var i = 0; i < RowCount; i++)
            {
                await ExecuteAsync(connectionString, $"INSERT INTO [{cacheTableName}] ([Id], [Name]) VALUES (NEWID(), 'row {i}')");
            }

            // Export "as of" a moment that is guaranteed to be after the rows were written, taken from the database's own
            // clock so that a container/host clock difference cannot hide them. It is fixed for the lifetime of the
            // harness, so every run is the same export slot (stream, data time, trigger source and schedule).
            var asOfTime = await ScalarAsync<DateTime>(connectionString, "SELECT DATEADD(SECOND, 1, SYSUTCDATETIME())");
            await Task.Delay(TimeSpan.FromSeconds(1.5));
            var fixedNow = new DateTimeOffset(DateTime.SpecifyKind(asOfTime, DateTimeKind.Utc));

            var providerId = Guid.NewGuid();
            var providerDefinitionId = Guid.NewGuid();
            var organizationId = Guid.NewGuid();

            var container = new WindsorContainer();
            var applicationContext = new ApplicationContext(container);
            container.Register(Component.For<ISystemEvents>().Instance(new Mock<ISystemEvents>().Object));

            var streamLogService = new Mock<IStreamLogService>();
            streamLogService
                .Setup(s => s.StoreHistoryLogEntryAsync(It.IsAny<CluedIn.Streams.StreamLog.History.StreamHistoryLogHistoryModel>()))
                .Returns(Task.CompletedTask);
            container.Register(Component.For<IStreamLogService>().Instance(streamLogService.Object));

            var componentHealthService = new Mock<IComponentHealthService>();
            componentHealthService
                .Setup(service => service.GetComponentHealth(It.IsAny<ExecutionContext>(), It.IsAny<ComponentArea>(), It.IsAny<Guid>()))
                .ReturnsAsync(new ComponentHealthModel { Status = ComponentHealthStatus.Healthy });
            container.Register(Component.For<IComponentHealthService>().Instance(componentHealthService.Object));

            var timeProvider = new Mock<ITimeProvider>();
            timeProvider.Setup(x => x.GetUtcNow()).Returns(fixedNow);

            var cache = new Mock<InMemoryApplicationCache>(MockBehavior.Loose, container) { CallBase = true };
            container.Register(Component.For<IApplicationCache>().Instance(cache.Object));

            var providerDefinitionDataStore = new Mock<IRelationalDataStore<ProviderDefinition>>();
            providerDefinitionDataStore
                .Setup(store => store.GetByIdAsync(It.IsAny<ExecutionContext>(), providerDefinitionId))
                .ReturnsAsync(new ProviderDefinition { IsEnabled = true, ProviderId = providerId, Id = providerDefinitionId });
            container.Register(Component.For<IRelationalDataStore<ProviderDefinition>>().Instance(providerDefinitionDataStore.Object));

            var systemConnectionStrings = new Mock<ISystemConnectionStrings>();
            container.Register(Component.For<ISystemConnectionStrings>().Instance(systemConnectionStrings.Object));
            container.Register(Component.For<SystemContext>().Instance(new SystemContext(container)));
            container.Register(Component.For<ILogger<OrganizationDataStores>>().Instance(new Mock<ILogger<OrganizationDataStores>>().Object));
            systemConnectionStrings.Setup(x => x.SystemOrganizationDataShard).Returns(new Mock<IOrganizationDataShard>().Object);

            var organization = new Organization(applicationContext, organizationId);
            var organizationRepository = new Mock<IOrganizationRepository>();
            organizationRepository.Setup(x => x.GetOrganization(It.IsAny<ExecutionContext>(), It.IsAny<Guid>())).Returns(organization);
            container.Register(Component.For<IOrganizationRepository>().Instance(organizationRepository.Object));

            var executionContextLogger = new Mock<ILogger<ExecutionContext>>();
            container.Register(Component.For<ILogger<ExecutionContext>>().Instance(executionContextLogger.Object));
            var context = new ExecutionContext(applicationContext, organization, executionContextLogger.Object);

            var streamModel = new StreamModel
            {
                Id = streamId,
                ConnectorProviderDefinitionId = providerDefinitionId,
                ContainerName = "test",
                Mode = StreamMode.Sync,
                Status = StreamStatus.Started,
                OrganizationId = organization.Id,
            };
            var streamRepository = new Mock<IStreamRepository>();
            // IStreamRepository.GetStream gained an ExecutionContext parameter in CluedIn 4.7.0 - see StorageConnectorTestsBase.v46.cs/.v47_to_Latest.cs.
#if CLUEDIN_V47_OR_GREATER
            streamRepository.Setup(x => x.GetStream(It.IsAny<ExecutionContext>(), streamId)).ReturnsAsync(streamModel);
#else
            streamRepository.Setup(x => x.GetStream(streamId)).ReturnsAsync(streamModel);
#endif
            container.Register(Component.For<IStreamRepository>().Instance(streamRepository.Object));

            var constants = new Mock<IStorageConfigurationConstants>();
            constants.Setup(x => x.ProviderId).Returns(providerId);
            constants.Setup(x => x.ExportCommandTimeoutKeyName).Returns("ExportCommandTimeout");
            constants.Setup(x => x.ExportCommandTimeoutDefaultValue).Returns(600);

            var jobConnectionString = string.IsNullOrEmpty(jobConnectionStringOptions)
                ? connectionString
                : $"{connectionString.TrimEnd(';')};{jobConnectionStringOptions}";
            var configuration = new TestStorageConfiguration(jobConnectionString);
            var storage = new InMemoryStorage();
            var storageFactory = new Mock<IStorageFactory>();
            storageFactory
                .Setup(x => x.CreateStorageConfiguration(It.IsAny<ExecutionContext>(), It.IsAny<IReadOnlyStreamModel>()))
                .ReturnsAsync(configuration);
            storageFactory
                .Setup(x => x.CreateStorageConfiguration(It.IsAny<ExecutionContext>(), It.IsAny<Guid>()))
                .ReturnsAsync(configuration);
            storageFactory
                .Setup(x => x.CreateStorageClient(It.IsAny<ExecutionContext>(), It.IsAny<IStorageConfiguration>()))
                .ReturnsAsync(() => new InMemoryStorageClient(storage));

            var job = new TestExportJob(applicationContext, streamRepository.Object, constants.Object, storageFactory.Object, timeProvider.Object);
            return new ExportHarness(connectionString, streamId, context, job, storage);
        }

        public async Task<StorageExportEntitiesJobBase.ExportResult> RunExportAsync()
        {
            var args = new StorageJobArgs
            {
                Schedule = Schedule,
                Message = _streamId.ToString(),
                IsTriggeredFromJobServer = false,
            };
            return await Job.DoRunInternalAsync(_context, args);
        }

        public async Task<IReadOnlyList<HistoryRow>> ReadHistoryAsync()
        {
            await using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();
            await using var command = new SqlCommand($"SELECT Status, TotalRows, EndTime FROM [{HistoryTableName}] ORDER BY StartTime", connection);
            await using var reader = await command.ExecuteReaderAsync();
            var rows = new List<HistoryRow>();
            while (await reader.ReadAsync())
            {
                rows.Add(new HistoryRow(
                    reader.GetString(0),
                    reader.IsDBNull(1) ? null : reader.GetInt32(1),
                    reader.IsDBNull(2) ? null : reader.GetFieldValue<DateTimeOffset>(2)));
            }

            return rows;
        }

        public async Task<bool> IsExportLockFreeAsync()
        {
            await using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();
            await using var command = new SqlCommand(
                "DECLARE @result INT; EXEC @result = sp_getapplock @Resource = @Resource, @LockMode = 'Exclusive', @LockOwner = 'Session', @LockTimeout = 0; SELECT @result;",
                connection);
            command.Parameters.Add(new SqlParameter("@Resource", $"{Job.GetType().Name}_{_streamId}"));
            return (int)await command.ExecuteScalarAsync() >= 0;
        }

        public Task ExecuteAsync(string sql) => ExecuteAsync(_connectionString, sql);

        public async ValueTask DisposeAsync()
        {
            // System-versioned tables must have versioning turned off before they (and their _History table) can be dropped.
            await ExecuteAsync(
                _connectionString,
                $"""
                IF OBJECT_ID(N'dbo.[{_cacheTableName}]', N'U') IS NOT NULL
                    ALTER TABLE dbo.[{_cacheTableName}] SET (SYSTEM_VERSIONING = OFF);
                DROP TABLE IF EXISTS [{_cacheTableName}];
                DROP TABLE IF EXISTS [{_cacheTableName}_History];
                DROP TABLE IF EXISTS [{HistoryTableName}];
                """);
        }

        private static async Task ExecuteAsync(string connectionString, string sql)
        {
            await using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();
            await using var command = new SqlCommand(sql, connection) { CommandType = CommandType.Text };
            _ = await command.ExecuteNonQueryAsync();
        }

        private static async Task<T> ScalarAsync<T>(string connectionString, string sql)
        {
            await using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();
            await using var command = new SqlCommand(sql, connection) { CommandType = CommandType.Text };
            return (T)await command.ExecuteScalarAsync();
        }
    }

    private sealed class TestStorageConfiguration : IStorageConfiguration
    {
        public TestStorageConfiguration(string streamCacheConnectionString)
        {
            StreamCacheConnectionString = streamCacheConnectionString;
        }

        public string ContainerName => "test";

        public bool IsStreamCacheEnabled => true;

        public bool UseCurrentTimeForExport => true;

        public string FileNamePattern => null;

        public string StreamCacheConnectionString { get; }

        public string OutputFormat => "csv";

        public string Schedule => StorageExportEntitiesJobTransactionTests.Schedule;

        public bool ShouldWriteGuidAsString => false;

        public bool ShouldEscapeVocabularyKeys => false;

        public string CustomCron => null;

        public bool IsDeltaMode => false;

        public bool IsOverwriteEnabled => true;

        public bool IsArrayColumnsEnabled => false;

        public string RootDirectoryPath => null;

        public bool IsSoftDelete => false;
    }

    private sealed class InMemoryStorage
    {
        public ConcurrentDictionary<string, byte[]> Files { get; } = new();

        public ConcurrentDictionary<string, IDictionary<string, string>> Metadata { get; } = new();
    }

    private sealed class InMemoryStorageClient : IStorageClient
    {
        private readonly InMemoryStorage _storage;

        public InMemoryStorageClient(InMemoryStorage storage)
        {
            _storage = storage;
        }

        public Task<DirectoryPath> GetBaseDirectoryPathAsync() => Task.FromResult(new DirectoryPath("root"));

        public Task SaveDataAsync(FilePath filePath, string content, string contentType)
        {
            _storage.Files[filePath.FullPath] = Encoding.UTF8.GetBytes(content);
            return Task.CompletedTask;
        }

        public Task DeleteDirectoryAsync(DirectoryPath directoryPath)
        {
            foreach (var key in _storage.Files.Keys.Where(key => key.StartsWith(directoryPath.Path + "/", StringComparison.Ordinal)).ToList())
            {
                _storage.Files.TryRemove(key, out _);
                _storage.Metadata.TryRemove(key, out _);
            }

            return Task.CompletedTask;
        }

        public Task DeleteFileAsync(FilePath filePath)
        {
            _storage.Files.TryRemove(filePath.FullPath, out _);
            _storage.Metadata.TryRemove(filePath.FullPath, out _);
            return Task.CompletedTask;
        }

        public Task<bool> FileExistsAsync(FilePath filePath) => Task.FromResult(_storage.Files.ContainsKey(filePath.FullPath));

        public Task<FileMetadata> GetFileMetadataAsync(FilePath filePath)
        {
            if (!_storage.Files.ContainsKey(filePath.FullPath))
            {
                return Task.FromResult<FileMetadata>(null);
            }

            var metadata = _storage.Metadata.TryGetValue(filePath.FullPath, out var existing) ? existing : new Dictionary<string, string>();
            return Task.FromResult(new FileMetadata(metadata));
        }

        public Task<bool> DirectoryExistsAsync(DirectoryPath directory) => Task.FromResult(true);

        public Task<IEnumerable<FullyQualifiedFilePath>> GetFilesInDirectoryAsync(DirectoryPath directoryPath)
        {
            return Task.FromResult<IEnumerable<FullyQualifiedFilePath>>(Array.Empty<FullyQualifiedFilePath>());
        }

        public Task<IStorageFileClient> GetFileClientAsync(FilePath filePath)
        {
            return Task.FromResult<IStorageFileClient>(new InMemoryStorageFileClient(_storage, filePath));
        }

        public Task VerifyConnectionAsync() => Task.CompletedTask;

        public Task CreateDirectoryIfNotExistsAsync(DirectoryPath directoryPath) => Task.CompletedTask;

        public void Dispose()
        {
        }
    }

    private sealed class InMemoryStorageFileClient : IStorageFileClient
    {
        private readonly InMemoryStorage _storage;
        private readonly FilePath _filePath;

        public InMemoryStorageFileClient(InMemoryStorage storage, FilePath filePath)
        {
            _storage = storage;
            _filePath = filePath;
        }

        public Uri Uri => new($"memory://{_filePath.FullPath}");

        public Task DeleteAsync() => DeleteIfExistsAsync();

        public Task DeleteIfExistsAsync()
        {
            _storage.Files.TryRemove(_filePath.FullPath, out _);
            _storage.Metadata.TryRemove(_filePath.FullPath, out _);
            return Task.CompletedTask;
        }

        public Task<bool> ExistsAsync() => Task.FromResult(_storage.Files.ContainsKey(_filePath.FullPath));

        public Task<Stream> OpenWriteAsync(bool overwrite)
        {
            return Task.FromResult<Stream>(new CommitOnDisposeStream(bytes => _storage.Files[_filePath.FullPath] = bytes));
        }

        public Task RenameAsync(FilePath targetPath)
        {
            if (_storage.Files.TryRemove(_filePath.FullPath, out var bytes))
            {
                _storage.Files[targetPath.FullPath] = bytes;
            }

            if (_storage.Metadata.TryRemove(_filePath.FullPath, out var metadata))
            {
                _storage.Metadata[targetPath.FullPath] = metadata;
            }

            return Task.CompletedTask;
        }

        public Task SetMetadataAsync(FileMetadata fileMetadata)
        {
            _storage.Metadata[_filePath.FullPath] = fileMetadata.Metadata;
            return Task.CompletedTask;
        }
    }

    private sealed class CommitOnDisposeStream : MemoryStream
    {
        private readonly Action<byte[]> _commit;
        private bool _committed;

        public CommitOnDisposeStream(Action<byte[]> commit)
        {
            _commit = commit;
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing && !_committed)
            {
                _committed = true;
                _commit(ToArray());
            }

            base.Dispose(disposing);
        }
    }
}
