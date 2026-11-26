using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Castle.MicroKernel.Registration;
using Castle.Windsor;

using CluedIn.Connector.FileStorage.Common.Connector;
using CluedIn.Core;
using CluedIn.Core.Connectors;
using CluedIn.Core.Data;
using CluedIn.Core.Data.Parts;
using CluedIn.Core.Data.Vocabularies;
using CluedIn.Core.Processing;
using CluedIn.Core.Events;
using CluedIn.Core.Streams;
using CluedIn.Core.Streams.Models;

using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

using Moq;

using ExecutionContext = CluedIn.Core.ExecutionContext;

namespace CluedIn.Connector.FileStorage.Common.Tests.Integration;

/// <summary>
/// A real <see cref="StorageConnectorBase"/> storing entities into a real stream cache table (SQL Server).
/// The storage target is not involved, so it is only the cache writes that are exercised.
/// </summary>
/// <remarks>
/// Needs INTEGRATIONTEST_STREAMCACHE, a base64 encoded connection string, like the other integration tests.
/// </remarks>
internal sealed class CacheWriteHarness : IAsyncDisposable
{
    private static readonly Guid _providerDefinitionId = Guid.Parse("c444cda8-d9b5-45cc-a82d-fef28e08d55c");
    private static readonly DateTimeOffset _now = new(2026, 11, 20, 10, 0, 0, TimeSpan.Zero);

    private readonly TestStorageConnector _connector;
    private readonly StreamModel _streamModel;
    private readonly bool _isConnectionStringOverridden;

    private CacheWriteHarness(TestStorageConnector connector, StreamModel streamModel, CapturingLogger logger, string connectionString, bool softDelete, bool isConnectionStringOverridden)
    {
        _isConnectionStringOverridden = isConnectionStringOverridden;
        _connector = connector;
        _streamModel = streamModel;
        Logger = logger;
        ConnectionString = connectionString;
        IsSoftDelete = softDelete;
    }

    public static string StreamCacheConnectionString
    {
        get
        {
            var encoded = Environment.GetEnvironmentVariable("INTEGRATIONTEST_STREAMCACHE");
            return string.IsNullOrWhiteSpace(encoded)
                ? null
                : Encoding.UTF8.GetString(Convert.FromBase64String(encoded));
        }
    }

    public CapturingLogger Logger { get; }

    public string ConnectionString { get; }

    public bool IsSoftDelete { get; }

    public Guid StreamId => _streamModel.Id;

    public string TableName => CacheTableHelper.GetCacheTableName(StreamId);

    public static CacheWriteHarness Create(bool softDelete, string connectionString = null, int storeRetryCount = 0, int storeRetryDelayMilliseconds = 0)
    {
        var isConnectionStringOverridden = connectionString != null;
        connectionString ??= StreamCacheConnectionString
            ?? throw new InvalidOperationException("INTEGRATIONTEST_STREAMCACHE environment variable is not set.");

        var container = new WindsorContainer();
        container.Register(Component.For<ISystemEvents>().Instance(new Mock<ISystemEvents>().Object));
        container.Register(Component.For<SystemContext>().Instance(new SystemContext(container)));
        var applicationContext = new ApplicationContext(container);

        var timeProvider = new Mock<ITimeProvider>();
        timeProvider.Setup(x => x.GetUtcNow()).Returns(_now);

        var constants = new Mock<IStorageConfigurationConstants>();
        constants.Setup(x => x.ProviderId).Returns(_providerDefinitionId);
        constants.Setup(x => x.CacheRecordsThresholdKeyName).Returns("CacheRecordsThreshold");
        constants.Setup(x => x.CacheRecordsThresholdDefaultValue).Returns(50);
        constants.Setup(x => x.CacheSyncIntervalKeyName).Returns("CacheSyncInterval");
        constants.Setup(x => x.CacheSyncIntervalDefaultValue).Returns(2000);
        constants.Setup(x => x.CacheBufferStrategyKeyName).Returns("CacheBufferStrategy");
        constants.Setup(x => x.CacheBufferStrategyDefaultValue).Returns(nameof(BufferStrategy.Safe));
        constants.Setup(x => x.HealthCheckErrorLogIntervalKeyName).Returns("HealthCheckErrorLogInterval");
        constants.Setup(x => x.HealthCheckErrorLogIntervalDefaultValue).Returns(0);
        constants.Setup(x => x.StoreRetryCountKeyName).Returns("StoreRetryCount");
        constants.Setup(x => x.StoreRetryCountDefaultValue).Returns(storeRetryCount);
        constants.Setup(x => x.StoreRetryDelayKeyName).Returns("StoreRetryDelay");
        constants.Setup(x => x.StoreRetryDelayDefaultValue).Returns(storeRetryDelayMilliseconds);

        var configuration = new TestStorageConfiguration(connectionString, softDelete);
        var storageFactory = new Mock<IStorageFactory>();
        storageFactory
            .Setup(x => x.CreateStorageConfiguration(It.IsAny<ExecutionContext>(), It.IsAny<IReadOnlyStreamModel>()))
            .ReturnsAsync(configuration);

        var logger = new CapturingLogger();
        var connector = new TestStorageConnector(logger, applicationContext, constants.Object, storageFactory.Object, timeProvider.Object);

        var streamModel = new StreamModel
        {
            Id = Guid.NewGuid(),
            ConnectorProviderDefinitionId = _providerDefinitionId,
            ContainerName = "test",
            Mode = StreamMode.Sync,
            ExportIncomingEdges = false,
            ExportOutgoingEdges = false,
            Status = StreamStatus.Started,
            OrganizationId = Guid.NewGuid(),
        };

        return new CacheWriteHarness(connector, streamModel, logger, connectionString, softDelete, isConnectionStringOverridden);
    }

    public Task<SaveResult> StoreAsync(ConnectorEntityData entity) => _connector.StoreData(null, _streamModel, entity);

    /// <summary>
    /// An entity with a column of each of the types the cache table has to cope with.
    /// </summary>
    public static ConnectorEntityData CreateEntity(
        Guid entityId,
        VersionChangeType changeType = VersionChangeType.Added,
        int persistVersion = 1,
        string name = "name",
        int age = 30,
        DateTime? dateTime = null,
        decimal number = 12.5m,
        string extraColumn = null)
    {
        var date = dateTime ?? new DateTime(2020, 1, 2, 3, 4, 5, DateTimeKind.Utc);
        return new ConnectorEntityData(
            changeType,
            StreamMode.Sync,
            entityId,
            new ConnectorEntityPersistInfo("hash" + persistVersion, persistVersion),
            null,
            EntityCode.FromKey($"/Person#Acceptance:{entityId}"),
            "/Person",
            [
                new ConnectorPropertyData("user.age", age.ToString(),
                    new VocabularyKeyConnectorPropertyDataType(new VocabularyKey("user.age", dataType: VocabularyKeyDataType.Integer) { Storage = VocabularyKeyStorage.Typed })),
                new ConnectorPropertyData("user.dateTime", date,
                    new VocabularyKeyConnectorPropertyDataType(new VocabularyKey("user.dateTime", dataType: VocabularyKeyDataType.DateTime) { Storage = VocabularyKeyStorage.Typed })),
                new ConnectorPropertyData("user.dateTimeOffset", new DateTimeOffset(date, TimeSpan.Zero),
                    new VocabularyKeyConnectorPropertyDataType(new VocabularyKey("user.dateTimeOffset", dataType: VocabularyKeyDataType.DateTime) { Storage = VocabularyKeyStorage.Typed })),
                new ConnectorPropertyData("user.number", number,
                    new VocabularyKeyConnectorPropertyDataType(new VocabularyKey("user.number", dataType: VocabularyKeyDataType.Number) { Storage = VocabularyKeyStorage.Typed })),
                new ConnectorPropertyData("Name", name, new EntityPropertyConnectorPropertyDataType(typeof(string))),
                .. (extraColumn is null ? Array.Empty<ConnectorPropertyData>() : new[] { new ConnectorPropertyData(extraColumn, "value", new EntityPropertyConnectorPropertyDataType(typeof(string))) }),
            ],
            [EntityCode.FromKey($"/Person#Acceptance:{entityId}")],
            [],
            []);
    }

    /// <summary>
    /// Reads the rows of the cache table, ordered by id, with the temporal columns left out.
    /// </summary>
    public async Task<List<Dictionary<string, object>>> ReadRowsAsync()
    {
        await using var connection = new SqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = new SqlCommand($"SELECT * FROM [{TableName}] ORDER BY [Id]", connection);
        await using var reader = await command.ExecuteReaderAsync();

        var rows = new List<Dictionary<string, object>>();
        while (await reader.ReadAsync())
        {
            var row = new Dictionary<string, object>();
            for (var i = 0; i < reader.FieldCount; i++)
            {
                row[reader.GetName(i)] = reader.IsDBNull(i) ? null : reader.GetValue(i);
            }

            rows.Add(row);
        }

        return rows;
    }

    public async Task<int> CountRowsAsync()
    {
        await using var connection = new SqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = new SqlCommand($"SELECT COUNT(*) FROM [{TableName}]", connection);
        return (int)await command.ExecuteScalarAsync();
    }

    public async Task DropTableAsync()
    {
        await using var connection = new SqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = new SqlCommand($"""
            IF EXISTS (SELECT * FROM sys.tables WHERE name = '{TableName}' AND temporal_type = 2)
                ALTER TABLE [dbo].[{TableName}] SET (SYSTEM_VERSIONING = OFF);
            DROP TABLE IF EXISTS [{TableName}];
            DROP TABLE IF EXISTS [{TableName}_History];
            """, connection);
        await command.ExecuteNonQueryAsync();
    }

    public async ValueTask DisposeAsync()
    {
        _connector.Dispose();
        try
        {
            await DropTableAsync();
        }
        catch (SqlException) when (_isConnectionStringOverridden)
        {
            // A test that wrote to a SQL Server or a database that cannot be reached on purpose: there is no table to drop
        }
    }

    private sealed class TestStorageConnector : StorageConnectorBase
    {
        public TestStorageConnector(
            ILogger<StorageConnectorBase> logger,
            ApplicationContext applicationContext,
            IStorageConfigurationConstants constants,
            IStorageFactory storageFactory,
            ITimeProvider timeProvider)
            : base(logger, applicationContext, constants, storageFactory, timeProvider)
        {
        }

        protected override Type ExportJobType => typeof(object);
    }

    private sealed class TestStorageConfiguration : IStorageConfiguration
    {
        public TestStorageConfiguration(string connectionString, bool softDelete)
        {
            StreamCacheConnectionString = connectionString;
            IsSoftDelete = softDelete;
        }

        public string ContainerName => "test";

        public bool IsStreamCacheEnabled => true;

        public bool UseCurrentTimeForExport => false;

        public string FileNamePattern => "{StreamId}.{OutputFormat}";

        public string StreamCacheConnectionString { get; }

        public string OutputFormat => "csv";

        public string Schedule => "Hourly";

        public bool ShouldWriteGuidAsString => false;

        public bool ShouldEscapeVocabularyKeys => false;

        public string CustomCron => null;

        public bool IsDeltaMode => false;

        public bool IsOverwriteEnabled => false;

        public bool IsArrayColumnsEnabled => false;

        public string RootDirectoryPath => null;

        public bool IsSoftDelete { get; }
    }

    internal sealed class CapturingLogger : ILogger<StorageConnectorBase>
    {
        private readonly List<(LogLevel Level, string Message, Exception Exception)> _entries = new();

        public IReadOnlyList<(LogLevel Level, string Message, Exception Exception)> Entries
        {
            get
            {
                lock (_entries)
                {
                    return _entries.ToList();
                }
            }
        }

        public IDisposable BeginScope<TState>(TState state) => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter)
        {
            lock (_entries)
            {
                _entries.Add((logLevel, formatter(state, exception), exception));
            }
        }
    }
}
