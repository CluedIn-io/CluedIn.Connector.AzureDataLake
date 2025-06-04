using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Models;

using Castle.MicroKernel.Registration;
using Castle.Windsor;

using CluedIn.Connector.DataLake.Common.Connector;
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

public abstract partial class DataLakeConnectorTestsBase<TConnector, TJobDataFactory, TConstants>
    where TConnector : DataLakeConnector
    where TJobDataFactory : class, IDataLakeJobDataFactory
    where TConstants : class, IDataLakeConstants
{
    private readonly ITestOutputHelper _testOutputHelper;
    private static readonly DateTimeOffset _defaultCurrentTime = new(2024, 8, 21, 3, 16, 0, TimeSpan.FromHours(5));


    public DataLakeConnectorTestsBase(ITestOutputHelper testOutputHelper)
    {
        _testOutputHelper = testOutputHelper ?? throw new ArgumentNullException(nameof(testOutputHelper));
    }

    private protected Task<SetupContainerResult> SetupContainer<TJobData>(
        TJobData jobData,
        StreamMode streamMode,
        Action<Mock<IDateTimeOffsetProvider>> configureTimeProvider = null)
        where TJobData : DataLakeJobData
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
        var jobDataFactoryMock = CreateJobDataFactoryMock(container);
        var connectorMock = GetConnectorMock(mockDateTimeOffsetProvider, constantsMock, jobDataFactoryMock);
        jobDataFactoryMock.Setup(x => x.GetConfiguration(It.IsAny<ExecutionContext>(), providerDefinitionId, It.IsAny<string>()))
            .ReturnsAsync(jobData);
        connectorMock.CallBase = true;

        var (streamModel, streamRepositoryMock) = SetupStreamModel(streamMode, providerDefinitionId, container);
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

    private (StreamModel StreamModel, Mock<IStreamRepository> StreamRepositoryMock) SetupStreamModel(StreamMode streamMode, Guid providerDefinitionId, WindsorContainer container)
    {
        var streamRepository = new Mock<IStreamRepository>();
        var streamModel = CreateStreamModel(providerDefinitionId, streamMode);
        streamRepository.Setup(x => x.GetStream(streamModel.Id)).ReturnsAsync(streamModel);
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

    private static void SetupConfiguration<TJobData>(TJobData jobData) where TJobData : DataLakeJobData
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
            ProviderId = DataLakeProviderId,
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

    protected abstract Mock<TConnector> GetConnectorMock(
        Mock<IDateTimeOffsetProvider> mockDateTimeOffsetProvider,
        Mock<TConstants> constantsMock,
        Mock<TJobDataFactory> jobDataFactory);

    protected virtual Mock<TJobDataFactory> CreateJobDataFactoryMock(WindsorContainer container)
    {
        var dataFactoryMock = new Mock<TJobDataFactory>();

        return dataFactoryMock;
    }

    protected virtual StreamModel CreateStreamModel(Guid providerDefinitionId, StreamMode streamMode)
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
        };
        return streamModel;
    }

    protected virtual Mock<TConstants> CreateConstantsMock()
    {
        var constants = new Mock<TConstants>();
        constants.Setup(x => x.CacheRecordsThresholdKeyName).Returns("abc");
        constants.Setup(x => x.CacheRecordsThresholdDefaultValue).Returns(50);
        constants.Setup(x => x.CacheSyncIntervalKeyName).Returns("abc");
        constants.Setup(x => x.CacheSyncIntervalDefaultValue).Returns(2000);
        constants.Setup(x => x.ProviderId).Returns(DataLakeProviderId);
        return constants;
    }

    protected async Task AssertImmediateOutputResult(
        string fileSystemName,
        string directoryName,
        DataLakeServiceClient client,
        Func<DataLakeFileClient, Task> assertMethod)
    {
        try
        {
            var path = await WaitForFileToBeCreated(fileSystemName, directoryName, client);
            var fsClient = client.GetFileSystemClient(fileSystemName);
            var fileClient = fsClient.GetFileClient(path.Name);

            await assertMethod(fileClient);

            await fsClient.GetDirectoryClient(directoryName).DeleteAsync();
            await WaitForFileToBeDeleted(fileSystemName, directoryName, client, path);
        }
        finally
        {
            if (!IsFixedFileSystem)
            {
                await DeleteFileSystem(client, fileSystemName);
            }
            else
            {
                await DeleteDirectory(client, fileSystemName, directoryName);
            }
        }
    }

    protected async Task WaitForFileToBeDeleted(string fileSystemName, string directoryName, DataLakeServiceClient client, PathItem path)
    {
        var d = DateTime.Now;
        while (true)
        {
            if (DateTime.Now > d.AddSeconds(30))
            {
                throw new TimeoutException("Timeout waiting for file to be deleted");
            }

            if (!IsFixedFileSystem)
            {
                if (client.GetFileSystems().All(fs => fs.Name != fileSystemName))
                {
                    continue;
                }
            }

            var fileSystemClient = client.GetFileSystemClient(fileSystemName);

            var directoryClient = fileSystemClient.GetDirectoryClient(directoryName);
            if (!await directoryClient.ExistsAsync())
            {
                break;
            }

            var paths = directoryClient.GetPaths(recursive: true)
                .Where(p => p.IsDirectory == false)
                .Where(p => p.Name.Contains(directoryName))
                .Select(p => p.Name)
                .ToArray();

            if (paths.Contains(path.Name))
            {
                await Task.Delay(1000);
                continue;
            }

            break;
        }
    }

    private protected async Task AssertExportJobOutputFileContents(
        string fileSystemName,
        string directoryName,
        SetupContainerResult setupContainerResult,
        DataLakeServiceClient client,
        DataLakeExportEntitiesJobBase exportJob,
        Func<DataLakeFileClient, DataLakeFileSystemClient, SetupContainerResult, Task> assertMethod,
        Func<ExecuteExportArg, Task<PathItem>> executeExport = null)
    {
        var context = setupContainerResult.Context;
        var streamModel = setupContainerResult.StreamModel;
        var organization = setupContainerResult.Organization;
        var jobData = setupContainerResult.DataLakeJobData;

        try
        {
            var fsClient = client.GetFileSystemClient(fileSystemName);
            await ModifyHistoryTimeToBeCurrentTime(setupContainerResult);

            var executeExportArg = new ExecuteExportArg(
                context,
                streamModel.Id,
                organization,
                FileSystemName: fileSystemName,
                DirectoryName: directoryName,
                client,
                exportJob);

            var path = executeExport == null
                ? await DefaultExecuteExport(executeExportArg)
                : await executeExport(executeExportArg);

            var fileClient = fsClient.GetFileClient(path.Name);

            await assertMethod(fileClient, fsClient, setupContainerResult);

            await fsClient.GetDirectoryClient(directoryName).DeleteAsync();
        }
        finally
        {
            if (!IsFixedFileSystem)
            {
                await DeleteFileSystem(client, fileSystemName);
            }
            else
            {
                await DeleteDirectory(client, fileSystemName, directoryName);
            }

            await DeleteTable(streamModel.Id, jobData.StreamCacheConnectionString);
        }
    }

    private protected abstract DataLakeExportEntitiesJobBase CreateExportJob(SetupContainerResult setupResult);


    private protected async Task<PathItem> DefaultExecuteExport(ExecuteExportArg executeExportArg)
    {
        executeExportArg.ExportJob.Run(new Core.Jobs.JobArgs
        {
            OrganizationId = executeExportArg.Organization.Id.ToString(),
            Schedule = "* * * * *",
            Message = executeExportArg.StreamId.ToString(),
        });

        var path = await WaitForFileToBeCreated(
            executeExportArg.FileSystemName,
            executeExportArg.DirectoryName,
            executeExportArg.Client);
        return path;
    }

    protected static async Task DeleteTable(Guid streamId, string streamCacheConnectionString)
    {
        await using var connection = new SqlConnection(streamCacheConnectionString);
        await connection.OpenAsync();
        var tableName = CacheTableHelper.GetCacheTableName(streamId);
        var deleteTableSql = $"""
            IF EXISTS (SELECT * FROM SYSOBJECTS WHERE NAME='{tableName}' AND XTYPE='U')
            ALTER TABLE dbo.[{tableName}]  SET ( SYSTEM_VERSIONING = Off )

            DROP TABLE IF EXISTS [{tableName}];
            DROP TABLE IF EXISTS [{tableName}_History];
            """;
        var command = new SqlCommand(deleteTableSql, connection)
        {
            CommandType = CommandType.Text
        };
        _ = await command.ExecuteNonQueryAsync();
    }

    private protected static async Task ModifyHistoryTimeToBeCurrentTime(SetupContainerResult setupContainerResult)
    {
        var jobData = setupContainerResult.DataLakeJobData;
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
        catch
        {
            await DeleteTable(streamModel.Id, jobData.StreamCacheConnectionString);
        }

        static async Task EnableHistory(SqlConnection connection, string tableName)
        {
            var enableHistorySql = $"ALTER TABLE [dbo].[{tableName}] SET (SYSTEM_VERSIONING = ON (HISTORY_TABLE = [dbo].[{tableName}_History], DATA_CONSISTENCY_CHECK = ON));";

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

        static async Task AlterHistory(Mock<IDateTimeOffsetProvider> mockDateTimeOffsetProvider, SqlConnection connection, string tableName)
        {
            var alterHistorySql = $"""
                    UPDATE [dbo].[{tableName}] SET [ValidFrom] = @ValidFrom;
                    ALTER TABLE [dbo].[{tableName}] ADD PERIOD FOR SYSTEM_TIME ([ValidFrom], [ValidTo]);
                    ALTER TABLE [dbo].[{tableName}] ALTER COLUMN [ValidFrom] ADD HIDDEN;
                    ALTER TABLE [dbo].[{tableName}] ALTER COLUMN [ValidTo] ADD HIDDEN;
                    """;
            var alterHistoryCommand = new SqlCommand(alterHistorySql, connection)
            {
                CommandType = CommandType.Text,
            };
            alterHistoryCommand.Parameters.Add(new SqlParameter("@ValidFrom", mockDateTimeOffsetProvider.Object.GetCurrentUtcTime()));
            await alterHistoryCommand.ExecuteNonQueryAsync();
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
    }

    private protected virtual async Task AssertCsvResultUnescaped(DataLakeFileClient fileClient, DataLakeFileSystemClient dataLakeFileSystemClient, SetupContainerResult setupContainerResult)
    {
        await AssertCsvResult(fileClient, ".");
    }

    private protected virtual async Task AssertCsvResultEscaped(DataLakeFileClient fileClient, DataLakeFileSystemClient dataLakeFileSystemClient, SetupContainerResult setupContainerResult)
    {
        await AssertCsvResult(fileClient, "_");
    }

    protected virtual async Task AssertJsonResult(DataLakeFileClient fileClient, StreamMode streamMode, VersionChangeType changeType, bool isSingleObject = false)
    {
        using var reader = new StreamReader(fileClient.Read().Value.Content);
        var contents = await reader.ReadToEndAsync();

        var expectedResult = GetExpectedResult(".", streamMode, changeType, ArrayType.ObjectArray, isStringIntegers: false);
        object expectedObject = isSingleObject ? expectedResult.First() : expectedResult.Select(item => item.Columns).ToArray();
        var expectedJson = JsonConvert.SerializeObject(expectedObject);
        Assert.Equal(expectedJson.ToAlphabeticJsonString(), contents.ToAlphabeticJsonString());
    }

    protected virtual async Task AssertCsvResult(
        DataLakeFileClient fileClient,
        string separator,
        Func<IEnumerable<DataRow>, IEnumerable<DataRow>> formatResult = null)
    {
        using var streamReader = new StreamReader(fileClient.Read().Value.Content);
        using var csv = new CsvReader(streamReader, new CsvConfiguration(CultureInfo.InvariantCulture)
        {
            BadDataFound = null,
        });
        csv.Context.RegisterClassMap<MyClassWithDictionaryMapper>();

        var actualRows = csv.GetRecords<CsvDataRow>()
            .Select(row => new DataRow()
            {
                Columns = row.Columns.ToDictionary(col => col.Key, col => (object)col.Value),
            }).ToList();

        var unformattedExpectedResult = GetExpectedResult(separator);
        var formattedExpectedResult = formatResult?.Invoke(unformattedExpectedResult) ?? unformattedExpectedResult;

        AssertResult(actualRows, formattedExpectedResult.ToList());
    }

    private void AssertResult(List<DataRow> actualRows, List<DataRow> expectedRows)
    {
        TestOutputHelper.WriteLine(JsonConvert.SerializeObject(actualRows, Formatting.Indented));
        Assert.Equal(actualRows.Count, actualRows.Count);
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

    protected enum ArrayType
    {
        SerializedStringArray,
        StringArray,
        ObjectArray
    }
    private static List<DataRow> GetExpectedResult(
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

    private protected virtual Task AssertParquetResultUnescaped(DataLakeFileClient fileClient, DataLakeFileSystemClient dataLakeFileSystemClient, SetupContainerResult setupContainerResult)
    {
        return AssertParquetResult(fileClient, ".", false);
    }

    private protected virtual Task AssertParquetResultEscaped(DataLakeFileClient fileClient, DataLakeFileSystemClient dataLakeFileSystemClient, SetupContainerResult setupContainerResult)
    {
        return AssertParquetResult(fileClient, "_", false);
    }

    private protected virtual Task AssertParquetResultArrayColumnEnabled(DataLakeFileClient fileClient, DataLakeFileSystemClient dataLakeFileSystemClient, SetupContainerResult setupContainerResult)
    {
        return AssertParquetResult(fileClient, ".", true);
    }

    protected virtual async Task AssertParquetResult(
        DataLakeFileClient fileClient,
        string separator,
        bool isArrayColumnEnabled,
        Func<IEnumerable<DataRow>, IEnumerable<DataRow>> formatResult = null)
    {
        using var memoryStream = new MemoryStream();
        await fileClient.ReadToAsync(memoryStream);
        using var parquetReader = await ParquetReader.CreateAsync(memoryStream);
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

    protected static async Task DeleteFileSystem(DataLakeServiceClient client, string fileSystemName)
    {
        if (client.GetFileSystems().All(fs => fs.Name != fileSystemName))
        {
            return;
        }
        var fsClient = client.GetFileSystemClient(fileSystemName);
        await fsClient.DeleteIfExistsAsync();
    }

    protected static async Task DeleteDirectory(DataLakeServiceClient client, string fileSystemName, string directoryName)
    {
        var fsClient = client.GetFileSystemClient(fileSystemName);
        var directoryClient = fsClient.GetDirectoryClient(directoryName);
        await directoryClient.DeleteIfExistsAsync();
    }

    protected virtual async Task<PathItem> WaitForFileToBeCreated(
        string fileSystemName,
        string directoryName,
        DataLakeServiceClient client,
        Func<IList<PathItem>, IList<PathItem>> filterPaths = null)
    {
        PathItem path;
        var d = DateTime.Now;
        while (true)
        {
            if (DateTime.Now > d.AddSeconds(30))
            {
                throw new TimeoutException();
            }

            if (!IsFixedFileSystem)
            {
                if (client.GetFileSystems().All(fs => fs.Name != fileSystemName))
                {
                    continue;
                }
            }

            var fileSystemClient = client.GetFileSystemClient(fileSystemName);

            if (!await fileSystemClient.ExistsAsync())
            {
                continue;
            }
            var directoryClient = fileSystemClient.GetDirectoryClient(directoryName);
            if (!await directoryClient.ExistsAsync())
            {
                continue;
            }

            IList<PathItem> paths = [..directoryClient.GetPaths(recursive: true)
                .Where(p => p.IsDirectory == false)
                .Where(p => p.Name.Contains(directoryName))];

            paths = filterPaths == null ? paths : filterPaths(paths);

            if (paths.Count == 0)
            {
                await Task.Delay(1000);
                continue;
            }

            if (paths.Count > 1)
            {
                TestOutputHelper.WriteLine("Found multiple paths: {0}.", JsonConvert.SerializeObject(paths, Formatting.Indented));
            }
            path = paths.Single();

            if (path.ContentLength > 0)
            {
                break;
            }
        }
        return path;
    }

    private protected static async Task<DateTimeOffset> GetFileDataTime(ExecuteExportArg executeExportArg, PathItem path)
    {
        var fsClient = executeExportArg.Client.GetFileSystemClient(executeExportArg.FileSystemName);
        var dirClient = fsClient.GetDirectoryClient(executeExportArg.DirectoryName);
        var fileClient = dirClient.GetFileClient(path.Name[(executeExportArg.DirectoryName.Length + 1)..]);
        var fileProperties = await fileClient.GetPropertiesAsync();
        var fileMetadata = fileProperties.Value.Metadata;
        var fileDataTime = fileMetadata["DataTime"];

        return DateTimeOffset.Parse(fileDataTime);
    }

    protected virtual bool IsFixedFileSystem => false;

    protected abstract Guid DataLakeProviderId { get; }

    protected ITestOutputHelper TestOutputHelper => _testOutputHelper;
    protected static DateTimeOffset DefaultCurrentTime => _defaultCurrentTime;

    public ConnectorEntityData CreateBaseConnectorEntityData(StreamMode streamMode, VersionChangeType versionChangeType)
    {
        var dobInDateTime = new DateTime(2000, 01, 02, 03, 04, 05);
        var dobInDateTimeOffset = new DateTimeOffset(2000, 01, 02, 03, 04, 05, TimeSpan.FromMinutes(12 * 60 + 34));
        var data = new ConnectorEntityData(versionChangeType, streamMode,
            Guid.Parse("f55c66dc-7881-55c9-889f-344992e71cb8"),
            new ConnectorEntityPersistInfo("etypzcezkiehwq8vw4oqog==", 1), null,
            EntityCode.FromKey("/Person#Acceptance:7c5591cf-861a-4642-861d-3b02485854a0"),
            "/Person",
            [
                new ConnectorPropertyData("user.lastName", "Picard",
                    new VocabularyKeyConnectorPropertyDataType(new VocabularyKey("user.lastName"))),
                new ConnectorPropertyData("user.age", "123",
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
                new ConnectorPropertyData("Name", "Jean Luc Picard",
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
        Mock<TJobDataFactory> JobDataFactoryMock,
        DataLakeJobData DataLakeJobData,
        Mock<TConstants> ConstantsMock,
        Mock<IStreamRepository> StreamRepositoryMock,
        ProviderDefinition ProviderDefinition);

    private protected record ExecuteExportArg(
            ExecutionContext ExecutionContext,
            Guid StreamId,
            Organization Organization,
            string FileSystemName,
            string DirectoryName,
            DataLakeServiceClient Client,
            DataLakeExportEntitiesJobBase ExportJob);

    public class MyClassWithDictionaryMapper : ClassMap<CsvDataRow>
    {
        public MyClassWithDictionaryMapper()
        {

            Map(m => m.Columns).Convert
               (row => row.Row.HeaderRecord.Select
                (column => new { column, value = row.Row.GetField(column) })
                .ToDictionary(d => d.column, d => d.value)
                );
        }
    }

    public class DataRow
    {
        public Dictionary<string, object> Columns { get; set; }
    }
    public class CsvDataRow
    {
        public Dictionary<string, string> Columns { get; set; }
    }
}
