using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

using Azure.Identity;
using Azure.Storage.Files.DataLake;

using Castle.Windsor;

using CluedIn.Connector.DataLake.Common.Tests.Integration;
using CluedIn.Connector.FileStorage.Common;
using CluedIn.Connector.FileStorage.Common.Connector;
using CluedIn.Connector.OneLake.Connector;
using CluedIn.Core;
using CluedIn.Core.Data.Parts;
using CluedIn.Core.Streams.Models;

using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

using Moq;

using Xunit;
using Xunit.Abstractions;

using Encoding = System.Text.Encoding;

namespace CluedIn.Connector.OneLake.Tests.Integration;

public class OneLakeConnectorTests : DataLakeConnectorTestsBase<OneLakeConnector, OneLakeFactory, IOneLakeConfigurationConstants>
{
    protected override Guid StorageProviderId => OneLakeConfigurationConstants.DataLakeProviderId;
    protected override bool IsFixedFileSystem => true;

    public OneLakeConnectorTests(ITestOutputHelper testOutputHelper)
        : base(testOutputHelper)
    {
    }

    [Fact]
    public async Task VerifyConnection_WhenValidCredentials_ReturnSuccess()
    {
        var configuration = CreateConfigurationWithoutStreamCache();
        var storageConfiguration = new OneLakeConnectorConfiguration(configuration);

        var setupResult = await SetupContainer(storageConfiguration, StreamMode.EventStream);
        var connector = setupResult.ConnectorMock.Object;
        setupResult.StorageFactoryMock.Setup(factory => factory.CreateStorageConfiguration(setupResult.Context, configuration, It.IsAny<string>()))
            .Returns(Task.FromResult<IStorageConfiguration>(storageConfiguration));
        var result = await connector.VerifyConnection(setupResult.Context, configuration);
        Assert.NotNull(result);
        Assert.True(result.Success);
    }

    [Fact]
    public async Task VerifyConnection_WhenInvalidTenantId_ReturnInvalidCredentialsErrorMessage()
    {
        var configuration = CreateConfigurationWithoutStreamCache();
        configuration[nameof(OneLakeConfigurationConstants.TenantId)] = "1";
        var storageConfiguration = new OneLakeConnectorConfiguration(configuration);

        var setupResult = await SetupContainer(storageConfiguration, StreamMode.EventStream);
        var connector = setupResult.ConnectorMock.Object;
        setupResult.StorageFactoryMock.Setup(factory => factory.CreateStorageConfiguration(setupResult.Context, configuration, It.IsAny<string>()))
            .Returns(Task.FromResult<IStorageConfiguration>(storageConfiguration));
        var result = await connector.VerifyConnection(setupResult.Context, configuration);
        Assert.NotNull(result);
        Assert.False(result.Success);
        Assert.Equal(OneLakeConnector.InvalidCredentialsErrorMessage, result.ErrorMessage);
    }

    [Fact]
    public async Task VerifyConnection_WhenInvalidClientId_ReturnInvalidCredentialsErrorMessage()
    {
        var configuration = CreateConfigurationWithoutStreamCache();
        configuration[nameof(OneLakeConfigurationConstants.ClientId)] = "1";
        var storageConfiguration = new OneLakeConnectorConfiguration(configuration);

        var setupResult = await SetupContainer(storageConfiguration, StreamMode.EventStream);
        var connector = setupResult.ConnectorMock.Object;
        setupResult.StorageFactoryMock.Setup(factory => factory.CreateStorageConfiguration(setupResult.Context, configuration, It.IsAny<string>()))
            .Returns(Task.FromResult<IStorageConfiguration>(storageConfiguration));
        var result = await connector.VerifyConnection(setupResult.Context, configuration);
        Assert.NotNull(result);
        Assert.False(result.Success);
        Assert.Equal(OneLakeConnector.InvalidCredentialsErrorMessage, result.ErrorMessage);
    }

    [Fact]
    public async Task VerifyConnection_WhenInvalidClientSecret_ReturnInvalidCredentialsErrorMessage()
    {
        var configuration = CreateConfigurationWithoutStreamCache();
        configuration[nameof(OneLakeConfigurationConstants.ClientSecret)] = "1";
        var storageConfiguration = new OneLakeConnectorConfiguration(configuration);

        var setupResult = await SetupContainer(storageConfiguration, StreamMode.EventStream);
        var connector = setupResult.ConnectorMock.Object;
        setupResult.StorageFactoryMock.Setup(factory => factory.CreateStorageConfiguration(setupResult.Context, configuration, It.IsAny<string>()))
            .Returns(Task.FromResult<IStorageConfiguration>(storageConfiguration));
        var result = await connector.VerifyConnection(setupResult.Context, configuration);
        Assert.NotNull(result);
        Assert.False(result.Success);
        Assert.Equal(OneLakeConnector.InvalidCredentialsErrorMessage, result.ErrorMessage);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData("  ")]
    public async Task VerifyConnection_WhenWorkspaceNameInvalid_ReturnWorkspaceNameInvalidErrorMessage(string workspaceName)
    {
        var configuration = CreateConfigurationWithoutStreamCache();
        configuration[nameof(OneLakeConfigurationConstants.WorkspaceName)] = workspaceName;
        var storageConfiguration = new OneLakeConnectorConfiguration(configuration);

        var setupResult = await SetupContainer(storageConfiguration, StreamMode.EventStream);
        var connector = setupResult.ConnectorMock.Object;
        setupResult.StorageFactoryMock.Setup(factory => factory.CreateStorageConfiguration(setupResult.Context, configuration, It.IsAny<string>()))
            .Returns(Task.FromResult<IStorageConfiguration>(storageConfiguration));
        var result = await connector.VerifyConnection(setupResult.Context, configuration);
        Assert.NotNull(result);
        Assert.False(result.Success);
        Assert.Equal(OneLakeConnector.InvalidWorkspaceErrorMessage, result.ErrorMessage);
    }

    [Fact]
    public async Task VerifyConnection_WhenWorkspaceNotFound_ReturnWorkspaceNotFoundErrorMessage()
    {
        var configuration = CreateConfigurationWithoutStreamCache();
        configuration[nameof(OneLakeConfigurationConstants.WorkspaceName)] = "1";
        var storageConfiguration = new OneLakeConnectorConfiguration(configuration);

        var setupResult = await SetupContainer(storageConfiguration, StreamMode.EventStream);
        var connector = setupResult.ConnectorMock.Object;
        setupResult.StorageFactoryMock.Setup(factory => factory.CreateStorageConfiguration(setupResult.Context, configuration, It.IsAny<string>()))
            .Returns(Task.FromResult<IStorageConfiguration>(storageConfiguration));
        var result = await connector.VerifyConnection(setupResult.Context, configuration);
        Assert.NotNull(result);
        Assert.False(result.Success);
        Assert.Equal(OneLakeConnector.WorkspaceNotFoundErrorMessageFormat.FormatWith("1"), result.ErrorMessage);
    }

    [Theory]
    [InlineData("NotFiles/")]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData("File")]
    [InlineData("File/")]
    [InlineData(null)]
    public async Task VerifyConnection_WhenItemFolderInvalid_ReturnInvalidFolderErrorMessage(string itemFolder)
    {
        var configuration = CreateConfigurationWithoutStreamCache();
        configuration[nameof(OneLakeConfigurationConstants.ItemFolder)] = itemFolder;
        var storageConfiguration = new OneLakeConnectorConfiguration(configuration);

        var setupResult = await SetupContainer(storageConfiguration, StreamMode.EventStream);
        var connector = setupResult.ConnectorMock.Object;
        setupResult.StorageFactoryMock.Setup(factory => factory.CreateStorageConfiguration(setupResult.Context, configuration, It.IsAny<string>()))
            .Returns(Task.FromResult<IStorageConfiguration>(storageConfiguration));
        var result = await connector.VerifyConnection(setupResult.Context, configuration);
        Assert.NotNull(result);
        Assert.False(result.Success);
        Assert.Equal(OneLakeConnector.InvalidFolderErrorMessage, result.ErrorMessage);
    }

    [Theory]
    [InlineData("Files/")]
    [InlineData("Files")]
    [InlineData("Files/path")]
    [InlineData("Files/path with space")]
    [InlineData("Files/クルード・イン")]
    public async Task VerifyConnection_WhenItemFolderValid_ReturnInvalidFolderErrorMessage(string itemFolder)
    {
        var configuration = CreateConfigurationWithoutStreamCache();
        configuration[nameof(OneLakeConfigurationConstants.ItemFolder)] = itemFolder;
        var storageConfiguration = new OneLakeConnectorConfiguration(configuration);

        var setupResult = await SetupContainer(storageConfiguration, StreamMode.EventStream);
        var connector = setupResult.ConnectorMock.Object;
        setupResult.StorageFactoryMock.Setup(factory => factory.CreateStorageConfiguration(setupResult.Context, configuration, It.IsAny<string>()))
            .Returns(Task.FromResult<IStorageConfiguration>(storageConfiguration));
        var result = await connector.VerifyConnection(setupResult.Context, configuration);
        Assert.NotNull(result);
        Assert.True(result.Success);
    }

    [Fact]
    public async Task VerifyStoreData_EventStream()
    {
        var configuration = CreateConfigurationWithoutStreamCache();
        var storageConfiguration = new OneLakeConnectorConfiguration(configuration);

        var setupResult = await SetupContainer(storageConfiguration, StreamMode.EventStream);
        var connector = setupResult.ConnectorMock.Object;

        var data = CreateBaseConnectorEntityData(StreamMode.EventStream, VersionChangeType.Added);
        await connector.StoreData(setupResult.Context, setupResult.StreamModel, data);
        await AssertImmediateOutputResult(
            setupResult,
            assertMethod: async (setupResult, filePath) =>
            {
                await AssertJsonResult(setupResult, filePath, StreamMode.EventStream, VersionChangeType.Added);
            });
    }

    [Fact]
    public async void VerifyStoreData_Sync_WithoutStreamCache()
    {
        var configuration = CreateConfigurationWithoutStreamCache();
        var storageConfiguration = new OneLakeConnectorConfiguration(configuration);

        var setupResult = await SetupContainer(storageConfiguration, StreamMode.Sync);
        var connector = setupResult.ConnectorMock.Object;

        var data = CreateBaseConnectorEntityData(StreamMode.Sync, VersionChangeType.Added);
        await connector.StoreData(setupResult.Context, setupResult.StreamModel, data);
        await AssertImmediateOutputResult(
            setupResult,
            assertMethod: async (setupResult, filePath) =>
            {
                await AssertJsonResult(setupResult, filePath, StreamMode.Sync, VersionChangeType.Added, isSingleObject: true);
            });
    }

    [Fact]
    public async Task VerifyStoreData_Sync_WithStreamCacheAndJsonFormat()
    {
        await VerifyStoreData_Sync_WithStreamCache(
            "JSON",
            assertMethod: async (setupResult, filePath) =>
            {
                await AssertJsonResult(setupResult, filePath, StreamMode.Sync, VersionChangeType.Added);
            });
    }

    [Fact]
    public async Task VerifyStoreData_Sync_WithStreamCacheAndCsvFormatUnescaped()
    {
        await VerifyStoreData_Sync_WithStreamCache(
            "csv",
            AssertCsvResultUnescaped,
            configureAuthentication: (values) =>
            {
                values.Add(nameof(StorageConfigurationConstants.ShouldEscapeVocabularyKeys), false);
                values.Add(nameof(StorageConfigurationConstants.ShouldWriteGuidAsString), false);
            });
    }

    [Fact]
    public async Task VerifyStoreData_Sync_WithStreamCacheAndCsvFormatEscaped()
    {
        await VerifyStoreData_Sync_WithStreamCache(
            "csv",
            AssertCsvResultEscaped,
            configureAuthentication: (values) =>
            {
                values.Add(nameof(StorageConfigurationConstants.ShouldEscapeVocabularyKeys), true);
                values.Add(nameof(StorageConfigurationConstants.ShouldWriteGuidAsString), true);
            });
    }

    [Fact]
    public async Task VerifyStoreData_Sync_WithStreamCacheAndParquetFormatUnescaped()
    {
        await VerifyStoreData_Sync_WithStreamCache(
            "pArQuet",
            AssertParquetResultUnescaped,
            configureAuthentication: (values) =>
            {
                values.Add(nameof(StorageConfigurationConstants.ShouldEscapeVocabularyKeys), false);
                values.Add(nameof(StorageConfigurationConstants.ShouldWriteGuidAsString), false);
            });
    }

    [Fact]
    public async Task VerifyStoreData_Sync_WithStreamCacheAndParquetFormatWithEscaped()
    {
        await VerifyStoreData_Sync_WithStreamCache(
            "pArQuet",
            AssertParquetResultEscaped,
            configureAuthentication: (values) =>
            {
                values.Add(nameof(StorageConfigurationConstants.ShouldEscapeVocabularyKeys), true);
                values.Add(nameof(StorageConfigurationConstants.ShouldWriteGuidAsString), true);
            });
    }

    [Fact]
    public async Task VerifyStoreData_Sync_WithStreamCacheAndParquetFormatWithArrayColumnsEnabled()
    {
        await VerifyStoreData_Sync_WithStreamCache(
            "pArQuet",
            AssertParquetResultArrayColumnEnabled,
            configureAuthentication: (values) =>
            {
                values.Add(nameof(StorageConfigurationConstants.ShouldEscapeVocabularyKeys), false);
                values.Add(nameof(StorageConfigurationConstants.ShouldWriteGuidAsString), false);
                values.Add(nameof(StorageConfigurationConstants.IsArrayColumnsEnabled), true);
            });
    }

    [Fact]
    public async Task VerifyStoreData_Sync_WithStreamCacheAndParquetCanLoadToTable()
    {
        var tableName = Guid.NewGuid().ToString("N");
        await VerifyStoreData_Sync_WithStreamCache(
            "pArQuet",
            async (setupResult, filePath) =>
            {
                await AssertParquetResultEscaped(setupResult, filePath);
                var storageConfiguration = setupResult.StorageConfiguration as OneLakeConnectorConfiguration;
                var dataLakeClient = GetDataLakeClient(storageConfiguration);
                var directoryName = $"{storageConfiguration.ItemName}.Lakehouse/Tables/{tableName}";

                var tableFile = await WaitForFileToBeCreated(
                    setupResult,
                    paths => paths.Where(path => path.Name.EndsWith("parquet", StringComparison.OrdinalIgnoreCase)).ToList(),
                    (_, _) => directoryName);

                Assert.NotNull(tableFile);
                var fileSystemClient = dataLakeClient.GetFileSystemClient(storageConfiguration.FileSystemName);
                await fileSystemClient.DeleteDirectoryAsync(directoryName);
            },
            configureAuthentication: (values) =>
            {
                values.Add(nameof(StorageConfigurationConstants.ShouldEscapeVocabularyKeys), true);
                values.Add(nameof(StorageConfigurationConstants.ShouldWriteGuidAsString), true);
                values.Add(nameof(OneLakeConfigurationConstants.ShouldLoadToTable), true);
                values.Add(nameof(OneLakeConfigurationConstants.TableName), tableName);
            });
    }

    [Fact]
    public async Task VerifyStoreData_Sync_WhenRepeatRunAndFileExistsUsingInternalSchedulerAndSameDataTime_CanSkip()
    {
        await VerifyStoreData_Sync_WithStreamCache(
            "csv",
            AssertCsvResultEscaped,
            async executeExportArg =>
            {
                var jobArgs = new StorageJobArgs
                {
                    OrganizationId = executeExportArg.Organization.Id.ToString(),
                    Schedule = "0 0/1 * * *",
                    Message = executeExportArg.StreamId.ToString(),
                    IsTriggeredFromJobServer = false,
                };
                await executeExportArg.ExportJob.DoRunAsync(
                    executeExportArg.ExecutionContext,
                    jobArgs);

                var firstPath = await WaitForFileToBeCreated(executeExportArg.SetupContainerResult);

                var firstDataTime = await GetFileDataTime(executeExportArg, firstPath);
                await executeExportArg.ExportJob.DoRunAsync(
                    executeExportArg.ExecutionContext,
                    jobArgs);

                var secondPath = await WaitForFileToBeCreated(executeExportArg.SetupContainerResult);
                var secondDataTime = await GetFileDataTime(executeExportArg, secondPath);

                Assert.Equal(firstDataTime, secondDataTime);
                return secondPath;
            });
    }

    [Fact]
    public async Task VerifyStoreData_Sync_WhenRepeatRunAndFileExistsUsingInternalSchedulerAndDifferentDataTime_CanOverwrite()
    {
        var executionCount = 0;
        var dateTimeList = new List<DateTimeOffset>
        {
            DefaultCurrentTime,
            new DateTimeOffset(2024, 8, 21, 4, 16, 0, TimeSpan.FromHours(5)),
        };
        await VerifyStoreData_Sync_WithStreamCache(
            "csv",
            AssertCsvResultEscaped,
            async executeExportArg =>
            {
                var jobArgs = new StorageJobArgs
                {
                    OrganizationId = executeExportArg.Organization.Id.ToString(),
                    Schedule = "0 0 1-31 * *",
                    Message = executeExportArg.StreamId.ToString(),
                    IsTriggeredFromJobServer = false,
                };
                await executeExportArg.ExportJob.DoRunAsync(
                    executeExportArg.ExecutionContext,
                    jobArgs);

                executionCount++;

                var firstPath = await WaitForFileToBeCreated(executeExportArg.SetupContainerResult);

                var firstDataTime = await GetFileDataTime(executeExportArg, firstPath);
                await executeExportArg.ExportJob.DoRunAsync(
                    executeExportArg.ExecutionContext,
                    jobArgs);

                var secondPath = await WaitForFileToBeCreated(
                    executeExportArg.SetupContainerResult,
                    filterPaths: paths =>
                    {
                        return paths.Where(path => path.Name != firstPath.Name).ToList();
                    });
                var secondDataTime = await GetFileDataTime(executeExportArg, secondPath);

                Assert.NotEqual(firstDataTime, secondDataTime);
                return secondPath;
            },
            mockDateTimeOffsetProvider =>
            {
                mockDateTimeOffsetProvider.Setup(x => x.GetCurrentUtcTime())
                    .Returns(() =>
                    {
                        return dateTimeList[executionCount];
                    });
            });
    }

    [Fact]
    public async Task VerifyStoreData_Sync_WhenRepeatRunAndFileExistsUsingJobServer_CanCreateNewFile()
    {
        var executionCount = 0;
        var dateTimeList = new List<DateTimeOffset>
        {
            DefaultCurrentTime,
            new DateTimeOffset(2024, 8, 21, 4, 16, 0, TimeSpan.FromHours(5)),
        };
        await VerifyStoreData_Sync_WithStreamCache(
            "csv",
            AssertCsvResultEscaped,
            async executeExportArg =>
            {
                var jobArgs = new StorageJobArgs
                {
                    OrganizationId = executeExportArg.Organization.Id.ToString(),
                    Schedule = "0 0/1 * * *",
                    Message = executeExportArg.StreamId.ToString(),
                    IsTriggeredFromJobServer = false,
                };
                await executeExportArg.ExportJob.DoRunAsync(
                    executeExportArg.ExecutionContext,
                    jobArgs);
                executionCount++;

                var firstPath = await WaitForFileToBeCreated(executeExportArg.SetupContainerResult);

                var firstDataTime = await GetFileDataTime(executeExportArg, firstPath);
                await executeExportArg.ExportJob.DoRunAsync(
                    executeExportArg.ExecutionContext,
                    jobArgs);

                var secondPath = await WaitForFileToBeCreated(
                    executeExportArg.SetupContainerResult,
                    filterPaths: paths =>
                    {
                        return paths.Where(path => path.Name != firstPath.Name).ToList();
                    });
                var secondDataTime = await GetFileDataTime(executeExportArg, secondPath);

                Assert.NotEqual(firstDataTime, secondDataTime);
                return secondPath;
            },
            mockDateTimeOffsetProvider =>
            {
                mockDateTimeOffsetProvider.Setup(x => x.GetCurrentUtcTime())
                    .Returns(() =>
                    {
                        return dateTimeList[executionCount];
                    });
            });
    }

    [Fact]
    public async Task VerifyStoreData_Sync_WithStreamCacheCanIgnoreWhenChangedAfterDeletion()
    {
        await VerifyStoreData_Sync_WithStreamCache("csv",
            async (setupResult, filePath) => await AssertCsvResult(setupResult, filePath, "_", (rows) =>
            {
                var removed = rows.ToList();
                removed.Clear();
                return removed;
            }),
            getConnectorEntityData: () =>
            {
                var initialUserData = UserData.Default;
                var removedUserData = initialUserData with { Age = initialUserData.Age + 1 };
                var readdAfterDeletionUserData = initialUserData with { Age = initialUserData.Age + 2 };

                var initialEntityData = CreateBaseConnectorEntityData(StreamMode.Sync, VersionChangeType.Added, persistVersion: 1, userData: initialUserData);
                var removedEntityData = CreateBaseConnectorEntityData(StreamMode.Sync, VersionChangeType.Removed, persistVersion: 2, userData: removedUserData);
                var readdAfterDeletionEntityData = CreateBaseConnectorEntityData(StreamMode.Sync, VersionChangeType.Changed, persistVersion: 3, userData: readdAfterDeletionUserData);

                // Intermediate version is outdated when final version is stored, so it should be ignored and not cause the export to fail
                return new[] { initialEntityData, removedEntityData, readdAfterDeletionEntityData };
            },
            configureAuthentication: (values) =>
            {
                values.Add(nameof(StorageConfigurationConstants.ShouldEscapeVocabularyKeys), true);
                values.Add(nameof(StorageConfigurationConstants.ShouldWriteGuidAsString), true);
            });
    }

    private protected override StorageExportEntitiesJobBase CreateExportJob(SetupContainerResult setupResult)
    {
        var logger = new Mock<ILogger<OneLakeClient>>();
        var exportJob = new OneLakeExportEntitiesJob(
            setupResult.ApplicationContext,
            setupResult.StreamRepositoryMock.Object,
            setupResult.ConstantsMock.Object,
            setupResult.StorageFactoryMock.Object,
            setupResult.DateTimeOffsetProviderMock.Object);
        return exportJob;
    }

    private static DataLakeServiceClient GetDataLakeClient(OneLakeConnectorConfiguration storageConfiguration)
    {
        var sharedKeyCredential = GetCredential(storageConfiguration);
        return new DataLakeServiceClient(
            new Uri("https://onelake.dfs.fabric.microsoft.com"),
            sharedKeyCredential);
    }

    private static ClientSecretCredential GetCredential(OneLakeConnectorConfiguration storageConfiguration)
    {
        return new ClientSecretCredential(storageConfiguration.TenantId, storageConfiguration.ClientId, storageConfiguration.ClientSecret);
    }

    private protected override Dictionary<string, object> CreateConfigurationWithoutStreamCache()
    {
        var tenantId = Environment.GetEnvironmentVariable("ONELAKE_TENANTID");
        var clientId = Environment.GetEnvironmentVariable("ONELAKE_CLIENTID");
        var clientSecretEncoded = Environment.GetEnvironmentVariable("ONELAKE_CLIENTSECRET");
        var workspaceName = Environment.GetEnvironmentVariable("ONELAKE_WORKSPACENAME");
        var itemName = Environment.GetEnvironmentVariable("ONELAKE_ITEMNAME");

        var clientSecretString = Encoding.UTF8.GetString(Convert.FromBase64String(clientSecretEncoded));
        var maskedSecret = string.IsNullOrWhiteSpace(clientSecretEncoded) ? string.Empty
            : $"{clientSecretEncoded[0..3]}{new string('*', Math.Max(clientSecretEncoded.Length - 3, 0))}";
        TestOutputHelper.WriteLine(
            "Using TenantId: '{0}', ClientId: '{1}', ClientSecret: '{2}', WorkspaceName: '{3}', ItemName: '{4}'.",
            tenantId,
            clientId,
            maskedSecret,
            workspaceName,
            itemName);
        Assert.NotNull(tenantId);
        Assert.NotNull(clientId);
        Assert.NotNull(clientSecretString);
        Assert.NotNull(workspaceName);
        Assert.NotNull(itemName);

        var directoryName = $"xunit-{DateTime.Now.Ticks}";
        return new Dictionary<string, object>()
        {
            { nameof(OneLakeConfigurationConstants.TenantId), tenantId },
            { nameof(OneLakeConfigurationConstants.ClientId), clientId },
            { nameof(OneLakeConfigurationConstants.ClientSecret), clientSecretString },
            { nameof(OneLakeConfigurationConstants.WorkspaceName), workspaceName },
            { nameof(OneLakeConfigurationConstants.ItemName), itemName },
            { nameof(OneLakeConfigurationConstants.ItemFolder), $"Files/{directoryName}" },
            { nameof(OneLakeConfigurationConstants.ItemType), "Lakehouse" },
        };
    }

    private protected override Dictionary<string, object> CreateConfigurationWithStreamCache(string format)
    {
        var baseConfiguration = CreateConfigurationWithoutStreamCache();
        var streamCacheConnectionStringEncoded = Environment.GetEnvironmentVariable("ONELAKE_STREAMCACHE");
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

    protected override Mock<OneLakeConnector> GetConnectorMock(
        ApplicationContext applicationContext,
        Mock<IDateTimeOffsetProvider> mockDateTimeOffsetProvider,
        Mock<IOneLakeConfigurationConstants> constantsMock,
        Mock<OneLakeFactory> storageConfigurationFactory)
    {
        var mockConnector = new Mock<OneLakeConnector>(
            new Mock<ILogger<OneLakeConnector>>().Object,
            applicationContext,
            constantsMock.Object,
            storageConfigurationFactory.Object,
            mockDateTimeOffsetProvider.Object);
        return mockConnector;
    }

    protected override Mock<OneLakeFactory> CreateStorageFactoryMock(
        WindsorContainer container,
        Mock<IDateTimeOffsetProvider> mockDateTimeOffsetProvider)
    {
        //container.Register(Component.For<OneLakeFactory>().ImplementedBy<OneLakeFactory>().LifestyleSingleton());
        var storageFactory = new Mock<OneLakeFactory>();
        storageFactory.Setup(x => x.CreateStorageClient(It.IsAny<ExecutionContext>(), It.IsAny<IStorageConfiguration>()))
            .Returns<ExecutionContext, IStorageConfiguration>((_, data) => Task.FromResult<IStorageClient>(new OneLakeClient(NullLogger<OneLakeClient>.Instance, data as OneLakeConnectorConfiguration)));
        return storageFactory;
    }

    private protected override DataLakeServiceClient GetDataLakeClient(SetupContainerResult setupContainerResult)
    {
        return GetDataLakeClient(setupContainerResult.StorageConfiguration as OneLakeConnectorConfiguration);
    }

    private protected override string GetDirectoryName(SetupContainerResult setupContainerResult)
    {
        var config = setupContainerResult.StorageConfiguration as OneLakeConnectorConfiguration;
        return $"{config.ItemName}.Lakehouse/{config.ItemFolder}";
    }

    private protected override StorageConfigurationBase CreateStorageConfiguration(Dictionary<string, object> configuration)
    {
        return new OneLakeConnectorConfiguration(configuration);
    }
}

