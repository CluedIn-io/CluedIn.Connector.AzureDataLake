using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Castle.MicroKernel.Registration;
using Castle.Windsor;

using CluedIn.Connector.FileStorage.Common;
using CluedIn.Connector.FileStorage.Common.Connector;
using CluedIn.Core;
using CluedIn.Core.Accounts;
using CluedIn.Core.Caching;
using CluedIn.Core.Connectors;
using CluedIn.Core.Data;
using CluedIn.Core.Data.Parts;
using CluedIn.Core.Data.Relational;
using CluedIn.Core.Data.Vocabularies;
using CluedIn.Core.DataStore;
using CluedIn.Core.Streams;
using CluedIn.Core.Streams.Models;

using CsvHelper;
using CsvHelper.Configuration;

using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

using Moq;

using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

using Parquet;

using Xunit;
using Xunit.Abstractions;

using ExecutionContext = CluedIn.Core.ExecutionContext;

namespace CluedIn.Connector.DataLake.Common.Tests.Integration;

public abstract partial class StorageConnectorTestsBase<TConnector, TClientFactory, TConfigurationConstants>
    where TConnector : StorageConnectorBase
    where TClientFactory : class, IStorageFactory
    where TConfigurationConstants : class, IStorageConfigurationConstants
{
    protected const int NotTemporalTableErrorCode = 13591;
    protected readonly ITestOutputHelper _testOutputHelper;
    protected static readonly DateTimeOffset _defaultCurrentTime = new(2024, 8, 21, 3, 16, 0, TimeSpan.FromHours(5));

    public StorageConnectorTestsBase(ITestOutputHelper testOutputHelper)
    {
        _testOutputHelper = testOutputHelper ?? throw new ArgumentNullException(nameof(testOutputHelper));
    }

    protected ITestOutputHelper TestOutputHelper => _testOutputHelper;
    protected static DateTimeOffset DefaultCurrentTime => _defaultCurrentTime;

    protected abstract Guid StorageProviderId { get; }

    protected static async Task DeleteTable(Guid streamId, string streamCacheConnectionString)
    {
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
            await DeleteTableInternal(deleteTableSql, streamCacheConnectionString);
        }
        catch (SqlException ex) when (ex.Number == NotTemporalTableErrorCode)
        {
            var deleteTableSeparatelySql = $"""
                IF EXISTS (SELECT * FROM SYSOBJECTS WHERE NAME='{tableName}' AND XTYPE='U')
                DROP TABLE IF EXISTS [{tableName}];
                DROP TABLE IF EXISTS [{tableName}_History];
                DROP TABLE IF EXISTS [{CacheTableHelper.GetExportHistoryTableName(streamId)}_ExportHistory];
                """;
            await DeleteTableInternal(deleteTableSeparatelySql, streamCacheConnectionString);
        }

        static async Task DeleteTableInternal(string deleteTableSql, string streamCacheConnectionString)
        {
            await using var connection = new SqlConnection(streamCacheConnectionString);
            await connection.OpenAsync();

            var command = new SqlCommand(deleteTableSql, connection)
            {
                CommandType = CommandType.Text
            };
            _ = await command.ExecuteNonQueryAsync();
        }
    }

    private protected Task<SetupContainerResult> SetupContainer<TJobData>(
        TJobData jobData,
        StreamMode streamMode,
        Action<Mock<IDateTimeOffsetProvider>> configureTimeProvider = null)
        where TJobData : StorageConfigurationBase
    {
        var organizationId = Guid.NewGuid();
        var providerDefinitionId = Guid.Parse("c444cda8-d9b5-45cc-a82d-fef28e08d55c");

        var container = new WindsorContainer();
        var applicationContext = new ApplicationContext(container);

        var mockDateTimeOffsetProvider = SetupDateTimeOffsetProvider(configureTimeProvider);
        _ = SetupApplicationCache(container);
        var providerDefinition = SetupProviderDefinition(providerDefinitionId, container);
        var organization = SetupOrganization(organizationId, container, applicationContext);
        var context = SetupExecutionContext(applicationContext, organization);

        SetupConfiguration(jobData);

        var constantsMock = CreateConstantsMock();
        var jobDataFactoryMock = CreateStorageFactoryMock(container, mockDateTimeOffsetProvider);
        var connectorMock = GetConnectorMock(applicationContext, mockDateTimeOffsetProvider, constantsMock, jobDataFactoryMock);
        jobDataFactoryMock.Setup(x => x.CreateStorageConfiguration(It.IsAny<ExecutionContext>(), providerDefinitionId, It.IsAny<string>()))
            .ReturnsAsync(jobData);
        connectorMock.CallBase = true;

        var (streamModel, streamRepositoryMock) = SetupStreamModel(organization, streamMode, providerDefinitionId, container);
        return Task.FromResult(
            new SetupContainerResult(
                context,
                connectorMock,
                streamModel,
                mockDateTimeOffsetProvider,
                applicationContext,
                organization,
                jobDataFactoryMock,
                jobData,
                constantsMock,
                streamRepositoryMock,
                providerDefinition));
    }



    protected abstract Mock<TConnector> GetConnectorMock(
        ApplicationContext applicationContext,
        Mock<IDateTimeOffsetProvider> mockDateTimeOffsetProvider,
        Mock<TConfigurationConstants> constantsMock,
        Mock<TClientFactory> jobDataFactory);

    protected abstract Mock<TClientFactory> CreateStorageFactoryMock(WindsorContainer container, Mock<IDateTimeOffsetProvider> mockDateTimeOffsetProvider);

    protected virtual StreamModel CreateStreamModel(
        Organization organization,
        Guid providerDefinitionId,
        StreamMode streamMode)
    {
        var streamModel = new StreamModel
        {
            Id = Guid.NewGuid(),
            ConnectorProviderDefinitionId = providerDefinitionId,
            ContainerName = "test",
            Mode = streamMode,
            ExportIncomingEdges = true,
            ExportOutgoingEdges = true,
            Status = StreamStatus.Started,
            OrganizationId = organization.Id,
        };
        return streamModel;
    }

    protected virtual Mock<TConfigurationConstants> CreateConstantsMock()
    {
        var constants = new Mock<TConfigurationConstants>();
        constants.Setup(x => x.CacheRecordsThresholdKeyName).Returns("abc");
        constants.Setup(x => x.CacheRecordsThresholdDefaultValue).Returns(50);
        constants.Setup(x => x.CacheSyncIntervalKeyName).Returns("abc");
        constants.Setup(x => x.CacheSyncIntervalDefaultValue).Returns(2000);
        constants.Setup(x => x.CacheBufferStrategyKeyName).Returns("CacheBufferStrategyKeyName");
        constants.Setup(x => x.CacheBufferStrategyDefaultValue).Returns(nameof(BufferStrategy.Safe));
        constants.Setup(x => x.ProviderId).Returns(StorageProviderId);
        return constants;
    }

    private protected async Task ModifyHistoryTimeToBeCurrentTime(SetupContainerResult setupContainerResult, ConnectorEntityData connectorEntityData)
    {
        var jobData = setupContainerResult.StorageConfiguration;
        var connectionString = jobData.StreamCacheConnectionString;
        var streamModel = setupContainerResult.StreamModel;
        var mockDateTimeOffsetProvider = setupContainerResult.DateTimeOffsetProviderMock;

        try
        {
            await using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();
            var tableName = CacheTableHelper.GetCacheTableName(streamModel.Id);

            await DisableHistory(connection, tableName);
            await AlterHistory(mockDateTimeOffsetProvider, connection, tableName);
            await EnableHistory(connection, tableName);
            await AssertRowCount(connection, tableName);
        }
        catch (Exception ex)
        {
            _testOutputHelper.WriteLine(ex.Message + Environment.NewLine + ex.StackTrace);
            await DeleteTable(streamModel.Id, jobData.StreamCacheConnectionString);
            throw;
        }

        static async Task AssertRowCount(SqlConnection connection, string tableName)
        {
            var getCountSql = $"SELECT COUNT(*) FROM [{tableName}]";
            var sqlCommand = new SqlCommand(getCountSql, connection)
            {
                CommandType = CommandType.Text,
            };
            var total = (int)await sqlCommand.ExecuteScalarAsync();

            Assert.Equal(1, total);
        }


        static async Task EnableHistory(SqlConnection connection, string tableName)
        {
            var enableHistorySql = $"""
                    ALTER TABLE [dbo].[{tableName}] ADD PERIOD FOR SYSTEM_TIME ([ValidFrom], [ValidTo]);
                    ALTER TABLE [dbo].[{tableName}] ALTER COLUMN [ValidFrom] ADD HIDDEN;
                    ALTER TABLE [dbo].[{tableName}] ALTER COLUMN [ValidTo] ADD HIDDEN;
                    ALTER TABLE [dbo].[{tableName}] SET (SYSTEM_VERSIONING = ON (HISTORY_TABLE = [dbo].[{tableName}_History], DATA_CONSISTENCY_CHECK = ON));
                    """;

            var enableHistoryCommand = new SqlCommand(enableHistorySql, connection)
            {
                CommandType = CommandType.Text,
            };
            await enableHistoryCommand.ExecuteNonQueryAsync();
        }

        static async Task DisableHistory(SqlConnection connection, string tableName)
        {
            var disableHistorySql = $"""
                ALTER TABLE [dbo].[{tableName}] SET (SYSTEM_VERSIONING = OFF);
                ALTER TABLE [dbo].[{tableName}] DROP PERIOD FOR SYSTEM_TIME;
                """;

            var disableHistoryCommand = new SqlCommand(disableHistorySql, connection)
            {
                CommandType = CommandType.Text,
            };
            await disableHistoryCommand.ExecuteNonQueryAsync();
        }

        async Task AlterHistory(Mock<IDateTimeOffsetProvider> mockDateTimeOffsetProvider, SqlConnection connection, string tableName)
        {

            var targetTime = mockDateTimeOffsetProvider.Object.GetCurrentUtcTime();
            var getCurrentValidFromSql = $"""
                        SELECT
                            [ValidFrom]
                        FROM
                            [dbo].[{tableName}]
                        WHERE
                            [{StorageConfigurationConstants.IdKey}] = @{StorageConfigurationConstants.IdKey};
                        """;
            using var getCurrentValidFromCommand = new SqlCommand(getCurrentValidFromSql, connection);
            getCurrentValidFromCommand.Parameters.Add(new SqlParameter($"@{StorageConfigurationConstants.IdKey}", connectorEntityData.EntityId));
            var validFrom = await getCurrentValidFromCommand.ExecuteScalarAsync() as DateTime?;


            var updateCurrentValidFromToTimeSql = $"""
                    UPDATE
                        [dbo].[{tableName}]
                    SET
                        [ValidFrom] = @ValidFrom
                    WHERE
                        [{StorageConfigurationConstants.IdKey}] = @{StorageConfigurationConstants.IdKey};
                    """;
            using var updateCurrentValidFromToTimeCommand = new SqlCommand(updateCurrentValidFromToTimeSql, connection)
            {
                CommandType = CommandType.Text,
            };
            updateCurrentValidFromToTimeCommand.Parameters.Add(new SqlParameter("@ValidFrom", targetTime));
            updateCurrentValidFromToTimeCommand.Parameters.Add(new SqlParameter($"@{StorageConfigurationConstants.IdKey}", connectorEntityData.EntityId));
            await updateCurrentValidFromToTimeCommand.ExecuteNonQueryAsync();


            var updateHistoryValidFromToTimeSql = $"""
                    UPDATE
                        [dbo].[{tableName}_History]
                    SET
                        [ValidTo] = @TargetValidTo
                    WHERE
                        [{StorageConfigurationConstants.IdKey}] = @{StorageConfigurationConstants.IdKey} AND
                        [ValidTo] = @OriginalValidTo;
                    """;
            using var updateHistoryValidFromToTimeCommand = new SqlCommand(updateHistoryValidFromToTimeSql, connection)
            {
                CommandType = CommandType.Text,
            };
            updateHistoryValidFromToTimeCommand.Parameters.Add(new SqlParameter("@TargetValidTo", targetTime));
            updateHistoryValidFromToTimeCommand.Parameters.Add(new SqlParameter("@OriginalValidTo", validFrom?.ToString("o")));
            updateHistoryValidFromToTimeCommand.Parameters.Add(new SqlParameter($"@{StorageConfigurationConstants.IdKey}", connectorEntityData.EntityId));
            await updateHistoryValidFromToTimeCommand.ExecuteNonQueryAsync();
        }
    }

    protected void AssertResult(List<DataRow> actualRows, List<DataRow> expectedRows)
    {
        TestOutputHelper.WriteLine("Actual" + Environment.NewLine + JsonConvert.SerializeObject(actualRows, Formatting.Indented));
        TestOutputHelper.WriteLine("Expected" + Environment.NewLine + JsonConvert.SerializeObject(expectedRows, Formatting.Indented));
        Assert.Equal(actualRows.Count, expectedRows.Count);
        for (var i = 0; i < expectedRows.Count; i++)
        {
            var expectedRow = expectedRows[i];
            var actualRow = actualRows[i];

            var expectedOrderedKeys = expectedRow.Columns.Keys.OrderBy(key => key).ToList();
            var actualOrderedKeys = actualRow.Columns.Keys.OrderBy(key => key).ToList();

            Assert.Equal(expectedOrderedKeys, actualOrderedKeys);

            foreach (var key in expectedOrderedKeys)
            {
                Assert.Equal(expectedRow.Columns[key], actualRow.Columns[key]);
            }
        }
    }
    private (StreamModel StreamModel, Mock<IStreamRepository> StreamRepositoryMock) SetupStreamModel(
        Organization organization,
        StreamMode streamMode,
        Guid providerDefinitionId,
        WindsorContainer container)
    {
        var streamRepository = new Mock<IStreamRepository>();
        var streamModel = CreateStreamModel(organization, providerDefinitionId, streamMode);
        streamRepository.Setup(x => x.GetStream(It.IsAny<ExecutionContext>(), streamModel.Id)).ReturnsAsync(streamModel);
        container.Register(Component.For<IStreamRepository>().Instance(streamRepository.Object));
        return (streamModel, streamRepository);
    }

    private ExecutionContext SetupExecutionContext(ApplicationContext applicationContext, Organization organization)
    {
        var executionContextLogger = new Mock<ILogger<ExecutionContext>>();

        applicationContext.Container
            .Register(Component.For<ILogger<ExecutionContext>>()
            .Instance(executionContextLogger.Object));

        var context = new ExecutionContext(applicationContext, organization, executionContextLogger.Object);
        return context;
    }

    private static void SetupConfiguration<TJobData>(TJobData jobData) where TJobData : StorageConfigurationBase
    {
        var configurationDictionary = jobData.Configurations.ToDictionary(config => config.Key, config => config.Value);
        var connectorConnectionMock = new Mock<IConnectorConnectionV2>();
        connectorConnectionMock.Setup(x => x.Authentication).Returns(configurationDictionary);
        var configurationRepository = new Mock<IConfigurationRepository>();
        configurationRepository.Setup(x => x.GetConfigurationById(It.IsAny<ExecutionContext>(), It.IsAny<Guid>()))
            .Returns(configurationDictionary);
    }

    private static Organization SetupOrganization(Guid organizationId, WindsorContainer container, ApplicationContext applicationContext)
    {
        var systemConnectionStrings = new Mock<ISystemConnectionStrings>();
        container.Register(Component.For<ISystemConnectionStrings>().Instance(systemConnectionStrings.Object));
        container.Register(Component.For<SystemContext>().Instance(new SystemContext(container)));

        container.Register(Component.For<ILogger<OrganizationDataStores>>()
            .Instance(new Mock<ILogger<OrganizationDataStores>>().Object));

        var organizationDataShard = new Mock<IOrganizationDataShard>();
        systemConnectionStrings.Setup(x => x.SystemOrganizationDataShard).Returns(organizationDataShard.Object);

        var organization = new Organization(applicationContext, organizationId);
        var organizationRepository = new Mock<IOrganizationRepository>();
        organizationRepository.Setup(x => x.GetOrganization(It.IsAny<ExecutionContext>(), It.IsAny<Guid>()))
            .Returns(organization);
        container.Register(Component.For<IOrganizationRepository>().Instance(organizationRepository.Object));

        return organization;
    }

    private ProviderDefinition SetupProviderDefinition(Guid providerDefinitionId, WindsorContainer container)
    {
        var providerDefinitionDataStore = new Mock<IRelationalDataStore<ProviderDefinition>>();
        var providerDefinition = new ProviderDefinition
        {
            IsEnabled = true,
            ProviderId = StorageProviderId,
            Id = providerDefinitionId,
        };
        providerDefinitionDataStore.Setup(store => store.GetByIdAsync(It.IsAny<ExecutionContext>(), providerDefinitionId))
            .ReturnsAsync(providerDefinition);
        container.Register(Component.For<IRelationalDataStore<ProviderDefinition>>()
            .Instance(providerDefinitionDataStore.Object));
        return providerDefinition;
    }

    private static IApplicationCache SetupApplicationCache(WindsorContainer container)
    {
        var cache = new Mock<InMemoryApplicationCache>(MockBehavior.Loose, container)
        {
            CallBase = true
        };
        container.Register(Component.For<IApplicationCache>().Instance(cache.Object));
        return cache.Object;
    }

    private static Mock<IDateTimeOffsetProvider> SetupDateTimeOffsetProvider(Action<Mock<IDateTimeOffsetProvider>> configureTimeProvider)
    {
        var mockDateTimeOffsetProvider = new Mock<IDateTimeOffsetProvider>();
        if (configureTimeProvider != null)
        {
            configureTimeProvider(mockDateTimeOffsetProvider);
        }
        else
        {
            mockDateTimeOffsetProvider.Setup(x => x.GetCurrentUtcTime()).Returns(DefaultCurrentTime);
        }

        return mockDateTimeOffsetProvider;
    }

    protected enum ArrayType
    {
        SerializedStringArray,
        StringArray,
        ObjectArray
    }

    private protected abstract StorageExportEntitiesJobBase CreateExportJob(SetupContainerResult setupResult);

    private protected virtual async Task AssertCsvResultUnescaped(SetupContainerResult setupContainerResult, ExportedFilePath path)
    {
        await AssertCsvResult(setupContainerResult, path, ".");
    }

    private protected virtual async Task AssertCsvResultEscaped(SetupContainerResult setupContainerResult, ExportedFilePath path)
    {
        await AssertCsvResult(setupContainerResult, path, "_");
    }

    private protected virtual async Task AssertJsonResult(SetupContainerResult setupContainerResult, ExportedFilePath path, StreamMode streamMode, VersionChangeType changeType, bool isSingleObject = false)
    {
        using var reader = new StreamReader(await GetContents(setupContainerResult, path));
        var contents = await reader.ReadToEndAsync();

        var expectedResult = GetExpectedResult(".", streamMode, changeType, ArrayType.ObjectArray, isStringIntegers: false);
        object expectedObject = isSingleObject ? expectedResult.First().Columns : expectedResult.Select(item => item.Columns).ToArray();
        var expectedJson = JsonConvert.SerializeObject(expectedObject);
        Assert.Equal(expectedJson.ToAlphabeticJsonString(), contents.ToAlphabeticJsonString());
    }

    private protected virtual async Task AssertCsvResult(
        SetupContainerResult setupContainerResult,
        ExportedFilePath path,
        string separator,
        Func<IEnumerable<DataRow>, IEnumerable<DataRow>> formatResult = null)
    {
        using var streamReader = new StreamReader(await GetContents(setupContainerResult, path));
        using var csv = new CsvReader(streamReader, new CsvConfiguration(CultureInfo.InvariantCulture)
        {
            BadDataFound = null,
        });

        var actualRows = csv.GetRecords<dynamic>()
            .Select(row => new DataRow()
            {
                Columns = (row as IDictionary<string, object>).ToDictionary(col => col.Key, col => (object)col.Value),
            }).ToList();

        var unformattedExpectedResult = GetExpectedResult(separator);
        var formattedExpectedResult = formatResult?.Invoke(unformattedExpectedResult) ?? unformattedExpectedResult;

        AssertResult(actualRows, formattedExpectedResult.ToList());
    }

    private protected virtual Task AssertParquetResultUnescaped(SetupContainerResult setupContainerResult, ExportedFilePath path)
    {
        return AssertParquetResult(setupContainerResult, path, ".", false);
    }

    private protected virtual Task AssertParquetResultEscaped(SetupContainerResult setupContainerResult, ExportedFilePath path)
    {
        return AssertParquetResult(setupContainerResult, path, "_", false);
    }

    private protected virtual Task AssertParquetResultArrayColumnEnabled(SetupContainerResult setupContainerResult, ExportedFilePath path)
    {
        return AssertParquetResult(setupContainerResult, path, ".", true);
    }

    private protected virtual async Task AssertParquetResult(
        SetupContainerResult setupContainerResult,
        ExportedFilePath path,
        string separator,
        bool isArrayColumnEnabled,
        Func<IEnumerable<DataRow>, IEnumerable<DataRow>> formatResult = null)
    {
        using var parquetReader = await ParquetReader.CreateAsync(await GetContents(setupContainerResult, path));
        var actualRows = new List<DataRow>();
        for (var rowGroup = 0; rowGroup < parquetReader.RowGroupCount; rowGroup++)
        {
            using var rowGroupReader = parquetReader.OpenRowGroupReader(rowGroup);
            var columns = new Dictionary<string, object>();
            foreach (var dataField in parquetReader.Schema!.GetDataFields())
            {
                var dataColumn = await rowGroupReader.ReadColumnAsync(dataField);
                var columnType = dataColumn.Field.SchemaType;

                var value = GetValue(dataColumn);
                columns.Add(dataField.Name, value);
            }
            actualRows.Add(new DataRow() { Columns = columns });
        }

        TestOutputHelper.WriteLine(JsonConvert.SerializeObject(actualRows, Formatting.Indented));

        var unformattedExpectedResult = GetExpectedResult(
            separator,
            arrayType: isArrayColumnEnabled ? ArrayType.StringArray : ArrayType.SerializedStringArray,
            isSimplifiedEdges: isArrayColumnEnabled,
            isStringIntegers: false);

        var formattedExpectedResult = formatResult?.Invoke(unformattedExpectedResult) ?? unformattedExpectedResult;

        AssertResult(actualRows, formattedExpectedResult.ToList());

        object GetValue(Parquet.Data.DataColumn dataColumn)
        {
            var type = dataColumn.Field.ClrType;

            if (type == typeof(Guid))
            {
                return getValueDirectlyOrFromNullable<Guid>(dataColumn).ToString();
            }
            else if (type == typeof(int))
            {
                return getValueDirectlyOrFromNullable<int>(dataColumn);
            }
            else if (type == typeof(long))
            {
                return getValueDirectlyOrFromNullable<long>(dataColumn);
            }
            else if (type == typeof(string))
            {
                var value = ((string[])dataColumn.Data);

                if (dataColumn.Field.IsArray)
                    return value;

                return value[0];
            }

            throw new NotSupportedException($"Type {dataColumn.Field.ClrType} not supported.");
        }

        object getValueDirectlyOrFromNullable<TValue>(Parquet.Data.DataColumn dataColumn)
            where TValue : struct
        {
            if (dataColumn.Field.IsNullable)
                return ((TValue?[])dataColumn.Data)[0];

            return ((TValue[])dataColumn.Data)[0];
        }
    }

    protected static List<DataRow> GetExpectedResult(
        string separator,
        StreamMode streamMode = StreamMode.Sync,
        VersionChangeType versionChangeType = VersionChangeType.Added,
        ArrayType arrayType = ArrayType.SerializedStringArray,
        bool isStringIntegers = true,
        bool isSimplifiedEdges = false)
    {
        var codesString = """
                    ["/Person#Acceptance:7c5591cf-861a-4642-861d-3b02485854a0"]
                    """;
        object codes = arrayType == ArrayType.SerializedStringArray ? codesString : JArray.Parse(codesString);

        var incomingEdgesFullString = $$$"""
            [{"FromReference":{"Code":{"Origin":{"Code":"Acceptance","Id":null},"Value":"7c5591cf-861a-4642-861d-3b02485854a0","Key":"/Person#Acceptance:7c5591cf-861a-4642-861d-3b02485854a0","Type":{"IsEntityContainer":false,"Root":null,"Code":"/Person"}},"Type":{"IsEntityContainer":false,"Root":null,"Code":"/Person"},"Name":null,"Properties":null,"PropertyCount":null,"EntityId":null,"IsEmpty":false},"ToReference":{"Code":{"Origin":{"Code":"Somewhere","Id":null},"Value":"1234","Key":"/EntityA#Somewhere:1234","Type":{"IsEntityContainer":false,"Root":null,"Code":"/EntityA"}},"Type":{"IsEntityContainer":false,"Root":null,"Code":"/EntityA"},"Name":null,"Properties":null,"PropertyCount":null,"EntityId":null,"IsEmpty":false},"EdgeType":{"Root":null,"Code":"/EntityA"},"HasProperties":false,"Properties":{},"CreationOptions":0,"Weight":null,"Version":0}]
            """;
        var incomingEdgesShortString = $$$"""
            ["EdgeType: /EntityA; From: §C:/Person#Acceptance:7c5591cf-861a-4642-861d-3b02485854a0; To: §C:/EntityA#Somewhere:1234; Properties: 0"]
            """;

        object incomingEdges = arrayType == ArrayType.SerializedStringArray
            ? incomingEdgesFullString
            : isSimplifiedEdges ? JArray.Parse(incomingEdgesShortString) : JArray.Parse(incomingEdgesFullString);


        var outgoingEdgesFullString = $$$"""
            [{"FromReference":{"Code":{"Origin":{"Code":"Somewhere","Id":null},"Value":"5678","Key":"/EntityB#Somewhere:5678","Type":{"IsEntityContainer":false,"Root":null,"Code":"/EntityB"}},"Type":{"IsEntityContainer":false,"Root":null,"Code":"/EntityB"},"Name":null,"Properties":null,"PropertyCount":null,"EntityId":null,"IsEmpty":false},"ToReference":{"Code":{"Origin":{"Code":"Acceptance","Id":null},"Value":"7c5591cf-861a-4642-861d-3b02485854a0","Key":"/Person#Acceptance:7c5591cf-861a-4642-861d-3b02485854a0","Type":{"IsEntityContainer":false,"Root":null,"Code":"/Person"}},"Type":{"IsEntityContainer":false,"Root":null,"Code":"/Person"},"Name":null,"Properties":null,"PropertyCount":null,"EntityId":null,"IsEmpty":false},"EdgeType":{"Root":null,"Code":"/EntityB"},"HasProperties":false,"Properties":{},"CreationOptions":0,"Weight":null,"Version":0}]
            """;
        var outgoingEdgesShortString = $$$"""
            ["EdgeType: /EntityB; From: §C:/EntityB#Somewhere:5678; To: §C:/Person#Acceptance:7c5591cf-861a-4642-861d-3b02485854a0; Properties: 0"]
            """;

        object outgoingEdges = arrayType == ArrayType.SerializedStringArray
            ? outgoingEdgesFullString
            : isSimplifiedEdges ? JArray.Parse(outgoingEdgesShortString) : JArray.Parse(outgoingEdgesFullString);

        var columns = new Dictionary<string, object>
        {
            { "Id", "f55c66dc-7881-55c9-889f-344992e71cb8" },
            { "Codes", codes },
            { "ContainerName", "test" },
            { "EntityType", "/Person" },
            { "Epoch", isStringIntegers ? 1724192160000.ToString() : 1724192160000 },
            { "IncomingEdges", incomingEdges },
            { "Name", "Jean Luc Picard" },
            { "OriginEntityCode", "/Person#Acceptance:7c5591cf-861a-4642-861d-3b02485854a0" },
            { "OutgoingEdges", outgoingEdges },
            { "PersistHash", "etypzcezkiehwq8vw4oqog==" },
            { "PersistVersion", isStringIntegers ? 1.ToString() : 1 },
            { "ProviderDefinitionId", "c444cda8-d9b5-45cc-a82d-fef28e08d55c" },
            { "Timestamp", "2024-08-21T03:16:00.0000000+05:00" },
            { $"user{separator}age", "123" },
            { $"user{separator}dobInDateTime", "2000-01-02T03:04:05" },
            { $"user{separator}dobInDateTimeOffset", "2000-01-02T03:04:05+12:34" },
            { $"user{separator}lastName", "Picard" },
        };

        if (streamMode == StreamMode.EventStream)
        {
            columns["ChangeType"] = versionChangeType.ToString();
        }

        var converted = ConvertJsonTokensToDictionary(columns, arrayType);
        return [new DataRow() { Columns = converted }];
    }

    protected static Dictionary<string, object> ConvertJsonTokensToDictionary(Dictionary<string, object> toConvert, ArrayType arrayType)
    {
        var result = new Dictionary<string, object>();
        foreach (var kvp in toConvert)
        {
            if (kvp.Value is JArray jArray)
            {
                if (jArray.First is JObject jObject)
                {
                    if (arrayType == ArrayType.StringArray)
                    {
                        var converted = jArray.ToObject<List<JObject>>();
                        result.Add(kvp.Key, converted.Select(JsonConvert.SerializeObject).ToList());
                    }
                    else
                    {
                        var converted = jArray.ToObject<List<Dictionary<string, object>>>();
                        result.Add(kvp.Key, converted.Select(item => ConvertJsonTokensToDictionary(item, arrayType)).ToList());
                    }
                }
                else if (jArray.First is JToken jToken)
                {
                    result.Add(kvp.Key, jArray.ToObject<List<object>>());
                }
            }
            else if (kvp.Value is JObject jObject)
            {
                var converted = jObject.ToObject<Dictionary<string, object>>();

                result.Add(kvp.Key, ConvertJsonTokensToDictionary(converted, arrayType));
            }
            else
            {
                result.Add(kvp.Key, kvp.Value);
            }
        }

        return result;
    }

    private protected ConnectorEntityData CreateBaseConnectorEntityData(
        StreamMode streamMode,
        VersionChangeType versionChangeType,
        int persistVersion = 1,
        UserData? userData = null)
    {
        var userDataToUse = userData ?? UserData.Default;
        var dobInDateTimeOffset = userDataToUse.DobInDateTimeOffset;
        var dobInDateTime = dobInDateTimeOffset.DateTime;

        var data = new ConnectorEntityData(versionChangeType, streamMode,
            Guid.Parse("f55c66dc-7881-55c9-889f-344992e71cb8"),
            new ConnectorEntityPersistInfo("etypzcezkiehwq8vw4oqog==", persistVersion), null,
            EntityCode.FromKey("/Person#Acceptance:7c5591cf-861a-4642-861d-3b02485854a0"),
            "/Person",
            [
                new ConnectorPropertyData("user.lastName", userDataToUse.LastName,
                    new VocabularyKeyConnectorPropertyDataType(new VocabularyKey("user.lastName"))),
                new ConnectorPropertyData("user.age", userDataToUse.Age.ToString(),
                    new VocabularyKeyConnectorPropertyDataType(
                        new VocabularyKey("user.age", dataType: VocabularyKeyDataType.Integer)
                        {
                            Storage = VocabularyKeyStorage.Typed,
                        })),
                new ConnectorPropertyData("user.dobInDateTime", dobInDateTime,
                    new VocabularyKeyConnectorPropertyDataType(
                        new VocabularyKey("user.dobInDateTime", dataType: VocabularyKeyDataType.DateTime)
                        {
                            Storage = VocabularyKeyStorage.Typed,
                        })),
                new ConnectorPropertyData("user.dobInDateTimeOffset", dobInDateTimeOffset,
                    new VocabularyKeyConnectorPropertyDataType(
                        new VocabularyKey("user.dobInDateTimeOffset", dataType: VocabularyKeyDataType.DateTime)
                        {
                            Storage = VocabularyKeyStorage.Typed,
                        })),
                new ConnectorPropertyData("Name", userDataToUse.Name,
                    new EntityPropertyConnectorPropertyDataType(typeof(string))),
            ],
            [EntityCode.FromKey("/Person#Acceptance:7c5591cf-861a-4642-861d-3b02485854a0")],
            [
                new EntityEdge(
                    new EntityReference(
                        EntityCode.FromKey("/Person#Acceptance:7c5591cf-861a-4642-861d-3b02485854a0")),
                    new EntityReference(EntityCode.FromKey("/EntityA#Somewhere:1234")), "/EntityA")
            ],
            [
                new EntityEdge(new EntityReference(EntityCode.FromKey("/EntityB#Somewhere:5678")),
                    new EntityReference(
                        EntityCode.FromKey("/Person#Acceptance:7c5591cf-861a-4642-861d-3b02485854a0")),
                    "/EntityB")
            ]);
        return data;
    }

    private protected record SetupContainerResult(
        ExecutionContext Context,
        Mock<TConnector> ConnectorMock,
        StreamModel StreamModel,
        Mock<IDateTimeOffsetProvider> DateTimeOffsetProviderMock,
        ApplicationContext ApplicationContext,
        Organization Organization,
        Mock<TClientFactory> StorageFactoryMock,
        StorageConfigurationBase StorageConfiguration,
        Mock<TConfigurationConstants> ConstantsMock,
        Mock<IStreamRepository> StreamRepositoryMock,
        ProviderDefinition ProviderDefinition);

    private protected record UserData(
        string Name,
        string LastName,
        int Age,
        DateTimeOffset DobInDateTimeOffset)
    {
        public static UserData Default => new UserData(
            "Jean Luc Picard",
            "Picard",
            123,
            new DateTimeOffset(2000, 01, 02, 03, 04, 05, TimeSpan.FromMinutes(12 * 60 + 34)));
    };

    public class DataRow
    {
        public Dictionary<string, object> Columns { get; set; }
    }


    private protected abstract Task<ExportedFilePath> WaitForFileToBeCreated(
        SetupContainerResult setupContainerResult,
        Func<IList<ExportedFilePath>, IList<ExportedFilePath>> filterPaths = null,
        Func<SetupContainerResult, string> getDirectoryName = null);

    protected record ExportedFilePath(string Name, string DirectoryPath, long? ContentLength);
    private protected abstract Task<Stream> GetContents(
        SetupContainerResult setupContainerResult,
        ExportedFilePath path);

    private protected async Task AssertImmediateOutputResult(
        SetupContainerResult setupContainerResult,
        Func<SetupContainerResult, ExportedFilePath, Task> assertMethod)
    {
        try
        {
            var path = await WaitForFileToBeCreated(setupContainerResult);

            await assertMethod(setupContainerResult, path);

            await CleanUpExportedFile(setupContainerResult, path);
        }
        finally
        {

            await CleanUpAfterImmediateOutputTest(setupContainerResult);
        }
    }


    private protected async Task AssertExportJobOutputFileContents(
        SetupContainerResult setupContainerResult,
        StorageExportEntitiesJobBase exportJob,
        Func<SetupContainerResult, ExportedFilePath, Task> assertMethod,
        Func<ExecuteExportArg, Task<ExportedFilePath>> executeExport = null)
    {
        var context = setupContainerResult.Context;
        var streamModel = setupContainerResult.StreamModel;
        var organization = setupContainerResult.Organization;

        try
        {
            var executeExportArg = new ExecuteExportArg(
                context,
                streamModel.Id,
                organization,
                exportJob,
                setupContainerResult);

            var path = executeExport == null
                ? await DefaultExecuteExport(executeExportArg)
                : await executeExport(executeExportArg);

            await assertMethod(setupContainerResult, path);
            await CleanUpExportedFile(setupContainerResult, path);
        }
        finally
        {
            await CleanUpAfterFileOutputTest(setupContainerResult);
        }
    }


    private protected async Task<ExportedFilePath> DefaultExecuteExport(ExecuteExportArg executeExportArg)
    {
        executeExportArg.ExportJob.Run(new Core.Jobs.JobArgs
        {
            OrganizationId = executeExportArg.Organization.Id.ToString(),
            Schedule = "* * * * *",
            Message = executeExportArg.StreamId.ToString(),
        });

        var path = await WaitForFileToBeCreated(executeExportArg.SetupContainerResult);
        return path;
    }

    private protected record ExecuteExportArg(
            ExecutionContext ExecutionContext,
            Guid StreamId,
            Organization Organization,
            StorageExportEntitiesJobBase ExportJob,
            SetupContainerResult SetupContainerResult);

    private protected abstract Task CleanUpExportedFile(SetupContainerResult setupContainerResult, ExportedFilePath filePath);

    private protected abstract Task CleanUpAfterImmediateOutputTest(SetupContainerResult setupContainerResult);

    private protected abstract Task CleanUpAfterFileOutputTest(SetupContainerResult setupContainerResult);

    private protected abstract Task WaitForFileToBeDeleted(SetupContainerResult setupContainerResult, ExportedFilePath path);
    private protected abstract Task<DateTimeOffset> GetFileDataTime(ExecuteExportArg executeExportArg, ExportedFilePath path);

    private protected virtual async Task VerifyStoreData_Sync_WithStreamCache(
       string format,
       Func<SetupContainerResult, ExportedFilePath, Task> assertMethod,
       Func<ExecuteExportArg, Task<ExportedFilePath>> executeExport = null,
       Action<Mock<IDateTimeOffsetProvider>> configureTimeProvider = null,
       Action<Dictionary<string, object>> configureAuthentication = null,
       Func<IEnumerable<ConnectorEntityData>> getConnectorEntityData = null,
       Func<SetupContainerResult, ConnectorEntityData, Task> storeData = null)
    {
        var configuration = CreateConfigurationWithStreamCache(format);
        configureAuthentication?.Invoke(configuration);
        var jobData = CreateStorageConfiguration(configuration);

        var setupResult = await SetupContainer(jobData, StreamMode.Sync, configureTimeProvider);
        var connector = setupResult.ConnectorMock.Object;

        var connectorEntityData = getConnectorEntityData == null
            ? [CreateBaseConnectorEntityData(StreamMode.Sync, VersionChangeType.Added)]
            : getConnectorEntityData();
        foreach (var data in connectorEntityData)
        {
            if (storeData != null)
            {
                await storeData(setupResult, data);
                continue;
            }

            await connector.StoreData(setupResult.Context, setupResult.StreamModel, data);
            await ModifyHistoryTimeToBeCurrentTime(setupResult, data);
        }
        var exportJob = CreateExportJob(setupResult);

        //await AssertExportJobOutputFileContents(
        //    setupResult,
        //    exportJob,
        //    assertMethod,
        //    executeExport);

        _testOutputHelper.WriteLine(nameof(VerifyStoreData_Sync_WithStreamCache) + "LALALA");
    }

    private protected abstract StorageConfigurationBase CreateStorageConfiguration(Dictionary<string, object> configuration);

    private protected abstract Dictionary<string, object> CreateConfigurationWithoutStreamCache();

    private protected virtual Dictionary<string, object> CreateConfigurationWithStreamCache(string format)
    {
        var baseConfiguration = CreateConfigurationWithoutStreamCache();
        var streamCacheConnectionStringEncoded = Environment.GetEnvironmentVariable("INTEGRATIONTEST_STREAMCACHE");
        var streamCacheConnectionString = Encoding.UTF8.GetString(Convert.FromBase64String(streamCacheConnectionStringEncoded));
        Console.WriteLine(streamCacheConnectionString);
        Assert.NotNull(streamCacheConnectionString);

        var updatedConfiguration = new Dictionary<string, object>(baseConfiguration)
        {
            { nameof(StorageConfigurationConstants.IsStreamCacheEnabled), true },
            { nameof(StorageConfigurationConstants.StreamCacheConnectionString), streamCacheConnectionString },
            { nameof(StorageConfigurationConstants.OutputFormat), format },
            { nameof(StorageConfigurationConstants.UseCurrentTimeForExport), true },
            { nameof(StorageConfigurationConstants.Schedule), CronSchedules.JobScheduleNames.Hourly },
            { nameof(StorageConfigurationConstants.ContainerName), "test" },
        };
        return updatedConfiguration;
    }

    //[Fact]
    //public async Task VerifyStoreData_Sync_WithStreamCacheCanHandleMultipleUpdates()
    //{
    //    var initialUserData = UserData.Default;
    //    var firstChangeUserData = initialUserData with { Age = initialUserData.Age + 1 };
    //    var secondChangeUserData = initialUserData with { Age = initialUserData.Age + 2 };
    //    await VerifyStoreData_Sync_WithStreamCache("csv",
    //        async (setupContainerResult, path) => await AssertCsvResult(setupContainerResult, path, "_", (rows) =>
    //        {
    //            var updated = rows.ToList();
    //            updated[0].Columns[StorageConfigurationConstants.PersistVersionKey] = 3.ToString();
    //            updated[0].Columns["user_age"] = secondChangeUserData.Age.ToString();
    //            return updated;
    //        }),
    //        getConnectorEntityData: () =>
    //        {
    //            var initialEntityData = CreateBaseConnectorEntityData(StreamMode.Sync, VersionChangeType.Added, persistVersion: 1, userData: initialUserData);
    //            var firstChangeEntityData = CreateBaseConnectorEntityData(StreamMode.Sync, VersionChangeType.Changed, persistVersion: 2, userData: firstChangeUserData);
    //            var secondChangeEntityData = CreateBaseConnectorEntityData(StreamMode.Sync, VersionChangeType.Changed, persistVersion: 3, userData: secondChangeUserData);

    //            return new[] { initialEntityData, firstChangeEntityData, secondChangeEntityData };
    //        },
    //        configureAuthentication: (values) =>
    //        {
    //            values.Add(nameof(StorageConfigurationConstants.ShouldEscapeVocabularyKeys), true);
    //            values.Add(nameof(StorageConfigurationConstants.ShouldWriteGuidAsString), true);
    //        });
    //}

    //[Fact]
    //public async Task VerifyStoreData_Sync_WithStreamCacheCanIgnoreOutdatedValues()
    //{
    //    var initialUserData = UserData.Default;
    //    var firstChangeUserData = initialUserData with { Age = initialUserData.Age + 1 };
    //    var secondChangeUserData = initialUserData with { Age = initialUserData.Age + 2 };
    //    await VerifyStoreData_Sync_WithStreamCache("csv",
    //        async (setupContainerResult, path) => await AssertCsvResult(setupContainerResult, path, "_", (rows) =>
    //        {
    //            var updated = rows.ToList();
    //            updated[0].Columns[StorageConfigurationConstants.PersistVersionKey] = 3.ToString();
    //            updated[0].Columns["user_age"] = secondChangeUserData.Age.ToString();
    //            return updated;
    //        }),
    //        getConnectorEntityData: () =>
    //        {
    //            var initialEntityData = CreateBaseConnectorEntityData(StreamMode.Sync, VersionChangeType.Added, persistVersion: 1, userData: initialUserData);
    //            var firstChangeEntityData = CreateBaseConnectorEntityData(StreamMode.Sync, VersionChangeType.Changed, persistVersion: 2, userData: firstChangeUserData);
    //            var secondChangeEntityData = CreateBaseConnectorEntityData(StreamMode.Sync, VersionChangeType.Changed, persistVersion: 3, userData: secondChangeUserData);

    //            // Second change is the most recent version, so first change should be ignored and not cause the export to fail
    //            return new[] { initialEntityData, secondChangeEntityData, firstChangeEntityData };
    //        },
    //        configureAuthentication: (values) =>
    //        {
    //            values.Add(nameof(StorageConfigurationConstants.ShouldEscapeVocabularyKeys), true);
    //            values.Add(nameof(StorageConfigurationConstants.ShouldWriteGuidAsString), true);
    //        });
    //}

    //[Fact]
    //public async Task VerifyStoreData_Sync_WithStreamCacheCanReAddAfterDeletion()
    //{
    //    var initialUserData = UserData.Default;
    //    var removedUserData = initialUserData with { Age = initialUserData.Age + 1 };
    //    var readdUserData = initialUserData with { Age = initialUserData.Age + 2 };
    //    await VerifyStoreData_Sync_WithStreamCache("csv",
    //        async (setupContainerResult, path) => await AssertCsvResult(setupContainerResult, path, "_", (rows) =>
    //        {
    //            var updated = rows.ToList();
    //            updated[0].Columns[StorageConfigurationConstants.PersistVersionKey] = 3.ToString();
    //            updated[0].Columns["user_age"] = readdUserData.Age.ToString();
    //            return updated;
    //        }),
    //        getConnectorEntityData: () =>
    //        {

    //            var initialEntityData = CreateBaseConnectorEntityData(StreamMode.Sync, VersionChangeType.Added, persistVersion: 1, userData: initialUserData);
    //            var removedEntityData = CreateBaseConnectorEntityData(StreamMode.Sync, VersionChangeType.Removed, persistVersion: 2, userData: removedUserData);
    //            var readdEntityData = CreateBaseConnectorEntityData(StreamMode.Sync, VersionChangeType.Added, persistVersion: 3, userData: readdUserData);

    //            // Intermediate version is outdated when final version is stored, so it should be ignored and not cause the export to fail
    //            return new[] { initialEntityData, removedEntityData, readdEntityData };
    //        },
    //        configureAuthentication: (values) =>
    //        {
    //            values.Add(nameof(StorageConfigurationConstants.ShouldEscapeVocabularyKeys), true);
    //            values.Add(nameof(StorageConfigurationConstants.ShouldWriteGuidAsString), true);
    //        });
    //}
}
