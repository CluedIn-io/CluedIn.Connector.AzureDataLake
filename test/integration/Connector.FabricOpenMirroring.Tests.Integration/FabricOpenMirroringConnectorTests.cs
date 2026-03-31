using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

using Azure.Identity;
using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Models;

using Castle.Windsor;

using CluedIn.Connector.DataLake.Common;
using CluedIn.Connector.DataLake.Common.Connector;
using CluedIn.Connector.DataLake.Common.Tests.Integration;
using CluedIn.Connector.FabricOpenMirroring.Connector;
using CluedIn.Connector.FileStorage.Common;
using CluedIn.Connector.FileStorage.Common.Connector;
using CluedIn.Core;
using CluedIn.Core.Connectors;
using CluedIn.Core.Data.Parts;
using CluedIn.Core.Streams.Models;

using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

using Moq;

using Xunit;
using Xunit.Abstractions;

using Encoding = System.Text.Encoding;

namespace CluedIn.Connector.FabricOpenMirroring.Tests.Integration;

public class OpenMirroringConnectorTests : DataLakeConnectorTestsBase<OpenMirroringConnector, OpenMirroringFactory, IOpenMirroringConfigurationConstants>
{
    protected override Guid StorageProviderId => OpenMirroringConfigurationConstants.DataLakeProviderId;
    protected override bool IsFixedFileSystem => true;

    public OpenMirroringConnectorTests(ITestOutputHelper testOutputHelper)
        : base(testOutputHelper)
    {
    }

    [Fact]
    public async Task VerifyConnection_WhenValidCredentials_ReturnSuccess()
    {
        var configuration = CreateConfigurationWithStreamCache(StorageConfigurationConstants.OutputFormats.Csv);
        var jobData = new OpenMirroringConnectorConfiguration(configuration);

        var setupResult = await SetupContainer(jobData, StreamMode.Sync);
        var connector = setupResult.ConnectorMock.Object;
        setupResult.StorageFactoryMock.Setup(factory => factory.CreateStorageConfiguration(setupResult.Context, configuration, It.IsAny<string>()))
            .Returns(Task.FromResult<IStorageConfiguration>(jobData));
        var result = await connector.VerifyConnection(setupResult.Context, configuration);
        Assert.NotNull(result);
        Assert.True(result.Success);
    }

    [Fact]
    public async Task VerifyConnection_WhenInvalidTenantId_ReturnInvalidCredentialsErrorMessage()
    {
        var configuration = CreateConfigurationWithStreamCache(StorageConfigurationConstants.OutputFormats.Csv);
        configuration[nameof(OpenMirroringConfigurationConstants.TenantId)] = "1";
        var jobData = new OpenMirroringConnectorConfiguration(configuration);

        var setupResult = await SetupContainer(jobData, StreamMode.Sync);
        var connector = setupResult.ConnectorMock.Object;
        setupResult.StorageFactoryMock.Setup(factory => factory.CreateStorageConfiguration(setupResult.Context, configuration, It.IsAny<string>()))
            .Returns(Task.FromResult<IStorageConfiguration>(jobData));
        var result = await connector.VerifyConnection(setupResult.Context, configuration);
        Assert.NotNull(result);
        Assert.False(result.Success);
        Assert.Equal(OpenMirroringConnector.InvalidCredentialsErrorMessage, result.ErrorMessage);
    }

    [Fact]
    public async Task VerifyConnection_WhenInvalidClientId_ReturnInvalidCredentialsErrorMessage()
    {
        var configuration = CreateConfigurationWithStreamCache(StorageConfigurationConstants.OutputFormats.Csv);
        configuration[nameof(OpenMirroringConfigurationConstants.ClientId)] = "1";
        var jobData = new OpenMirroringConnectorConfiguration(configuration);

        var setupResult = await SetupContainer(jobData, StreamMode.Sync);
        var connector = setupResult.ConnectorMock.Object;
        setupResult.StorageFactoryMock.Setup(factory => factory.CreateStorageConfiguration(setupResult.Context, configuration, It.IsAny<string>()))
            .Returns(Task.FromResult<IStorageConfiguration>(jobData));
        var result = await connector.VerifyConnection(setupResult.Context, configuration);
        Assert.NotNull(result);
        Assert.False(result.Success);
        Assert.Equal(OpenMirroringConnector.InvalidCredentialsErrorMessage, result.ErrorMessage);
    }

    [Fact]
    public async Task VerifyConnection_WhenInvalidClientSecret_ReturnInvalidCredentialsErrorMessage()
    {
        var configuration = CreateConfigurationWithStreamCache(StorageConfigurationConstants.OutputFormats.Csv);
        configuration[nameof(OpenMirroringConfigurationConstants.ClientSecret)] = "1";
        var jobData = new OpenMirroringConnectorConfiguration(configuration);

        var setupResult = await SetupContainer(jobData, StreamMode.Sync);
        var connector = setupResult.ConnectorMock.Object;
        setupResult.StorageFactoryMock.Setup(factory => factory.CreateStorageConfiguration(setupResult.Context, configuration, It.IsAny<string>()))
            .Returns(Task.FromResult<IStorageConfiguration>(jobData));
        var result = await connector.VerifyConnection(setupResult.Context, configuration);
        Assert.NotNull(result);
        Assert.False(result.Success);
        Assert.Equal(OpenMirroringConnector.InvalidCredentialsErrorMessage, result.ErrorMessage);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData("  ")]
    public async Task VerifyConnection_WhenWorkspaceNameInvalid_ReturnWorkspaceNameInvalidErrorMessage(string workspaceName)
    {
        var configuration = CreateConfigurationWithStreamCache(StorageConfigurationConstants.OutputFormats.Csv);
        configuration[nameof(OpenMirroringConfigurationConstants.WorkspaceName)] = workspaceName;
        var jobData = new OpenMirroringConnectorConfiguration(configuration);

        var setupResult = await SetupContainer(jobData, StreamMode.Sync);
        var connector = setupResult.ConnectorMock.Object;
        setupResult.StorageFactoryMock.Setup(factory => factory.CreateStorageConfiguration(setupResult.Context, configuration, It.IsAny<string>()))
            .Returns(Task.FromResult<IStorageConfiguration>(jobData));
        var result = await connector.VerifyConnection(setupResult.Context, configuration);
        Assert.NotNull(result);
        Assert.False(result.Success);
        Assert.Equal(OpenMirroringConnector.InvalidWorkspaceErrorMessage, result.ErrorMessage);
    }

    [Fact]
    public async Task VerifyConnection_WhenWorkspaceNotFound_ReturnWorkspaceNotFoundErrorMessage()
    {
        var configuration = CreateConfigurationWithStreamCache(StorageConfigurationConstants.OutputFormats.Csv);
        configuration[nameof(OpenMirroringConfigurationConstants.WorkspaceName)] = "1";
        var jobData = new OpenMirroringConnectorConfiguration(configuration);

        var setupResult = await SetupContainer(jobData, StreamMode.Sync);
        var connector = setupResult.ConnectorMock.Object;
        setupResult.StorageFactoryMock.Setup(factory => factory.CreateStorageConfiguration(setupResult.Context, configuration, It.IsAny<string>()))
            .Returns(Task.FromResult<IStorageConfiguration>(jobData));
        var result = await connector.VerifyConnection(setupResult.Context, configuration);
        Assert.NotNull(result);
        Assert.False(result.Success);
        Assert.Equal(OpenMirroringConnector.WorkspaceNotFoundErrorMessageFormat.FormatWith("1"), result.ErrorMessage);
    }

    [Fact]
    public async Task VerifyStoreData_Sync_WithStreamCacheAndCsvFormatEscaped()
    {
        await VerifyStoreData_Sync_WithStreamCache(
            "csv",
            AssertCsvResultEscaped);
    }

    [Fact]
    public async Task VerifyStoreData_Sync_WithStreamCacheAndParquetFormatWithEscaped()
    {
        await VerifyStoreData_Sync_WithStreamCache(
            "pArQuet",
            AssertParquetResultEscaped);
    }

    [Fact]
    public async Task VerifyStoreData_Sync_WhenRepeatRunAndFileExistsUsingInternalSchedulerAndSameDataTime_CanSkip()
    {
        await VerifyStoreData_Sync_WithStreamCache(
            "parquet",
            AssertParquetResultEscaped,
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

                var firstPath = await WaitForFileToBeCreated(
                    executeExportArg.SetupContainerResult);

                var firstDataTime = await GetFileDataTime(executeExportArg, firstPath);
                await executeExportArg.ExportJob.DoRunAsync(
                    executeExportArg.ExecutionContext,
                    jobArgs);

                var secondPath = await WaitForFileToBeCreated(
                    executeExportArg.SetupContainerResult);
                var secondDataTime = await GetFileDataTime(executeExportArg, secondPath);

                Assert.Equal(firstDataTime, secondDataTime);
                return secondPath;
            });
    }

    [Fact]
    public async Task VerifyStoreData_Sync_WhenRepeatRunAndFileExistsUsingInternalSchedulerAndDifferentDataTime_CanCreateNewFile()
    {
        var initialUserData = UserData.Default;
        var firstChangeUserData = initialUserData with { Age = initialUserData.Age + 1 };
        var executionCount = 0;
        var storeDataCount = 0;
        var dateTimeList = new List<DateTimeOffset>
        {
            DefaultCurrentTime,
            new DateTimeOffset(2024, 8, 21, 4, 16, 0, TimeSpan.FromHours(5)),
        };
        await VerifyStoreData_Sync_WithStreamCache(
            "parquet",
            async (setupResult, filePath) =>
            {
                var dateTimeProvider = setupResult.DateTimeOffsetProviderMock.Object;
                await base.AssertParquetResult(setupResult, filePath, separator: "_", isArrayColumnEnabled: false, formatResult: (original) =>
                {
                    var updated = original.ToList();
                    updated[0].Columns["__rowMarker__"] = "4";
                    updated[0].Columns["Timestamp"] = dateTimeProvider.GetCurrentUtcTime().ToString("O");
                    updated[0].Columns["Epoch"] = dateTimeProvider.GetCurrentUtcTime().ToUnixTimeMilliseconds();
                    updated[0].Columns[StorageConfigurationConstants.PersistVersionKey] = 2;
                    updated[0].Columns["user_age"] = firstChangeUserData.Age.ToString();
                    return updated;
                });
            },
            async executeExportArg =>
            {
                executionCount = 0;
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

                var firstPath = await WaitForFileToBeCreated(
                    executeExportArg.SetupContainerResult);

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
            },
            storeData: async (setupResult, data) =>
            {
                await setupResult.ConnectorMock.Object.StoreData(setupResult.Context, setupResult.StreamModel, data);
                await ModifyHistoryTimeToBeCurrentTime(setupResult, data);
                if (storeDataCount == 0)
                {
                    executionCount++;
                }
                storeDataCount++;
            },
            getConnectorEntityData: () =>
            {
                var initialEntityData = CreateBaseConnectorEntityData(StreamMode.Sync, VersionChangeType.Added, persistVersion: 1, userData: initialUserData);
                var firstChangeEntityData = CreateBaseConnectorEntityData(StreamMode.Sync, VersionChangeType.Changed, persistVersion: 2, userData: firstChangeUserData);

                return new[] { initialEntityData, firstChangeEntityData };
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
            "parquet",
            AssertParquetResultEscapedWithRowMarker,
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

                var firstPath = await WaitForFileToBeCreated(
                    executeExportArg.SetupContainerResult);

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
    public async Task VerifyStoreData_Sync_WithStreamCacheFirstExportNoRowMarker()
    {
        await VerifyStoreData_Sync_WithStreamCache(
            "pArQuet",
            AssertParquetResultEscaped);
    }

    [Fact]
    public async Task VerifyStoreData_Sync_WithStreamCacheSubsequentExportHasRowMarker()
    {
        var executionCount = 0;
        var dateTimeList = new List<DateTimeOffset>
        {
            DefaultCurrentTime,
            new DateTimeOffset(2024, 8, 21, 4, 16, 0, TimeSpan.FromHours(5)),
        };
        await VerifyStoreData_Sync_WithStreamCache(
            "parquet",
            AssertParquetResultEscapedWithRowMarker,
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

                var firstPath = await WaitForFileToBeCreated(
                    executeExportArg.SetupContainerResult);

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
    public async Task VerifyStoreData_Sync_WithStreamCacheCanUseTableName()
    {
        await VerifyStoreData_Sync_WithStreamCache(
            "parquet",
            AssertParquetResultEscaped,
            configureAuthentication: (dictionary) =>
            {
                dictionary[nameof(OpenMirroringConfigurationConstants.TableName)] = "MyTable";
            });
    }

    [Fact]
    public async Task Archive_Sync_CanDeleteDirectoryWhenUseTableName()
    {
        await VerifyStoreData_Sync_WithStreamCache(
            "parquet",
            async (setupResult, filePath) =>
            {
                await AssertParquetResultEscaped(setupResult, filePath);
                var connector = setupResult.ConnectorMock.Object;
                await connector.ArchiveContainer(setupResult.Context, setupResult.StreamModel);

                var client = GetDataLakeClient(setupResult);
                var fsClient = client.GetFileSystemClient(GetFileSystemName(setupResult));
                var directoryClient = fsClient.GetDirectoryClient($"{setupResult.StorageConfiguration.RootDirectoryPath}/ToBeArchived");
                var exists = await directoryClient.ExistsAsync();
                Assert.False(exists);
            },
            configureAuthentication: (dictionary) =>
            {
                dictionary[nameof(OpenMirroringConfigurationConstants.TableName)] = "ToBeArchived";
            });

    }

    private async Task AssertParquetResultEscapedWithRowMarker(
        SetupContainerResult setupContainerResult,
        ExportedFilePath filePath)
    {
        await base.AssertParquetResult(setupContainerResult, filePath, separator: "_", isArrayColumnEnabled: false, formatResult: (original) =>
        {
            var result = original.ToList();
            original.First().Columns["__rowMarker__"] = "4";
            return result;
        });
    }

    private protected override StorageExportEntitiesJobBase CreateExportJob(SetupContainerResult setupResult)
    {
        var exportJob = new OpenMirroringExportEntitiesJob(
            setupResult.ApplicationContext,
            setupResult.StreamRepositoryMock.Object,
            setupResult.ConstantsMock.Object,
            setupResult.StorageFactoryMock.Object,
            setupResult.DateTimeOffsetProviderMock.Object);
        return exportJob;
    }

    private static DataLakeServiceClient GetDataLakeClient(OpenMirroringConnectorConfiguration jobData)
    {
        var sharedKeyCredential = new ClientSecretCredential(jobData.TenantId, jobData.ClientId, jobData.ClientSecret);
        return new DataLakeServiceClient(
            new Uri("https://onelake.dfs.fabric.microsoft.com"),
            sharedKeyCredential);
    }

    private protected override Dictionary<string, object> CreateConfigurationWithoutStreamCache()
    {
        var tenantId = Environment.GetEnvironmentVariable("FABRICOPENMIRRORING_TENANTID");
        var clientId = Environment.GetEnvironmentVariable("FABRICOPENMIRRORING_CLIENTID");
        var clientSecretEncoded = Environment.GetEnvironmentVariable("FABRICOPENMIRRORING_CLIENTSECRET");
        var workspaceName = Environment.GetEnvironmentVariable("FABRICOPENMIRRORING_WORKSPACENAME");
        var mirroredDatabaseName = Environment.GetEnvironmentVariable("FABRICOPENMIRRORING_MIRROREDDATABASENAME");

        var clientSecretString = Encoding.UTF8.GetString(Convert.FromBase64String(clientSecretEncoded));
        var maskedSecret = string.IsNullOrWhiteSpace(clientSecretEncoded) ? string.Empty
            : $"{clientSecretEncoded[0..3]}{new string('*', Math.Max(clientSecretEncoded.Length - 3, 0))}";
        TestOutputHelper.WriteLine(
            "Using TenantId: '{0}', ClientId: '{1}', ClientSecret: '{2}', WorkspaceName: '{3}', MirroredDatabaseName: '{4}'.",
            tenantId,
            clientId,
            maskedSecret,
            workspaceName,
            mirroredDatabaseName);
        Assert.NotNull(tenantId);
        Assert.NotNull(clientId);
        Assert.NotNull(clientSecretString);
        Assert.NotNull(workspaceName);
        Assert.NotNull(mirroredDatabaseName);

        var directoryName = $"xunit-{DateTime.Now.Ticks}";
        return new Dictionary<string, object>()
        {
            { nameof(OpenMirroringConfigurationConstants.TenantId), tenantId },
            { nameof(OpenMirroringConfigurationConstants.ClientId), clientId },
            { nameof(OpenMirroringConfigurationConstants.ClientSecret), clientSecretString },
            { nameof(OpenMirroringConfigurationConstants.WorkspaceName), workspaceName },
            { nameof(OpenMirroringConfigurationConstants.MirroredDatabaseName), mirroredDatabaseName },
            { nameof(OpenMirroringConfigurationConstants.ShouldCreateMirroredDatabase), false },
        };
    }

    private protected override Dictionary<string, object> CreateConfigurationWithStreamCache(string format)
    {
        var baseConfiguration = CreateConfigurationWithoutStreamCache();
        var streamCacheConnectionStringEncoded = Environment.GetEnvironmentVariable("FABRICOPENMIRRORING_STREAMCACHE");
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

    protected override Mock<OpenMirroringConnector> GetConnectorMock(
        ApplicationContext applicationContext,
        Mock<IDateTimeOffsetProvider> mockDateTimeOffsetProvider,
        Mock<IOpenMirroringConfigurationConstants> constantsMock,
        Mock<OpenMirroringFactory> jobDataFactory)
    {
        var mockConnector = new Mock<OpenMirroringConnector>(
            new Mock<ILogger<OpenMirroringConnector>>().Object,
            applicationContext,
            constantsMock.Object,
            jobDataFactory.Object,
            mockDateTimeOffsetProvider.Object);
        return mockConnector;
    }

    protected override Mock<OpenMirroringFactory> CreateStorageFactoryMock(
        WindsorContainer container,
        Mock<IDateTimeOffsetProvider> mockDateTimeOffsetProvider)
    {
        var dataFactoryMock = new Mock<OpenMirroringFactory>();
        dataFactoryMock.Setup(x => x.CreateStorageClient(It.IsAny<ExecutionContext>(), It.IsAny<IStorageConfiguration>()))
            .Returns<ExecutionContext, IStorageConfiguration>((_, data) => Task.FromResult<IStorageClient>(new OpenMirroringClient(NullLogger<OpenMirroringClient>.Instance, mockDateTimeOffsetProvider.Object, data as OpenMirroringConnectorConfiguration)));
        return dataFactoryMock;
    }

    private protected override Task<ExportedFilePath> WaitForFileToBeCreated(
        SetupContainerResult setupContainerResult,
        Func<IList<ExportedFilePath>, IList<ExportedFilePath>> filterPaths = null,
        Func<SetupContainerResult, string, string> getDirectoryName = null)
    {
        return base.WaitForFileToBeCreated(setupContainerResult, filterPaths: (paths) =>
        {
            var metadataFiltered = paths.Where(path => !path.Name.EndsWith("/_metadata.json") && !path.Name.EndsWith("_partnerEvents.json")).ToList();
            return filterPaths?.Invoke(metadataFiltered) ?? metadataFiltered;
        },
        getDirectoryName);
    }

    private protected override DataLakeServiceClient GetDataLakeClient(SetupContainerResult setupContainerResult)
    {
        return GetDataLakeClient(setupContainerResult.StorageConfiguration as OpenMirroringConnectorConfiguration);
    }

    private protected override string GetDirectoryName(SetupContainerResult setupContainerResult)
    {
        var config = setupContainerResult.StorageConfiguration as OpenMirroringConnectorConfiguration;
        if (!string.IsNullOrEmpty(config.TableName))
        {
            return $"{config.RootDirectoryPath}/{config.TableName}";
        }

        return $"{config.RootDirectoryPath}/{setupContainerResult.StreamModel.Id:N}";
    }

    private protected override StorageConfigurationBase CreateStorageConfiguration(Dictionary<string, object> configuration)
    {
        return new OpenMirroringConnectorConfiguration(configuration);
    }
}

