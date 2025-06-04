using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Models;

using CluedIn.Connector.OneLake.Connector;
using CluedIn.Connector.DataLake.Common;
using CluedIn.Connector.DataLake.Common.Connector;
using CluedIn.Connector.DataLake.Common.Tests.Integration;
using CluedIn.Core;
using CluedIn.Core.Data.Parts;
using CluedIn.Core.Streams.Models;

using Microsoft.Extensions.Logging;

using Moq;

using Xunit;
using Xunit.Abstractions;

using Encoding = System.Text.Encoding;
using Azure.Identity;

namespace CluedIn.Connector.OneLake.Tests.Integration;

public class OneLakeConnectorTests : DataLakeConnectorTestsBase<OneLakeConnector, OneLakeJobDataFactory, IOneLakeConstants>
{
    protected override Guid DataLakeProviderId => OneLakeConstants.DataLakeProviderId;
    protected override bool IsFixedFileSystem => true;

    public OneLakeConnectorTests(ITestOutputHelper testOutputHelper)
        : base(testOutputHelper)
    {
    }

    [Fact]
    public async Task VerifyStoreData_EventStream()
    {
        var configuration = CreateConfigurationWithoutStreamCache();
        var jobData = new OneLakeConnectorJobData(configuration);

        var setupResult = await SetupContainer(jobData, StreamMode.EventStream);
        var connector = setupResult.ConnectorMock.Object;

        var data = CreateBaseConnectorEntityData(StreamMode.EventStream, VersionChangeType.Added);
        await connector.StoreData(setupResult.Context, setupResult.StreamModel, data);
        await AssertImmediateOutputResult(
            jobData.FileSystemName,
            jobData.RootDirectoryPath,
            GetDataLakeClient(jobData),
            assertMethod: async (fileClient) =>
            {
                await AssertJsonResult(fileClient, StreamMode.EventStream, VersionChangeType.Added);
            });
    }

    [Fact]
    public async void VerifyStoreData_Sync_WithoutStreamCache()
    {
        var configuration = CreateConfigurationWithoutStreamCache();
        var jobData = new OneLakeConnectorJobData(configuration);

        var setupResult = await SetupContainer(jobData, StreamMode.Sync);
        var connector = setupResult.ConnectorMock.Object;

        var data = CreateBaseConnectorEntityData(StreamMode.Sync, VersionChangeType.Added);
        await connector.StoreData(setupResult.Context, setupResult.StreamModel, data);
        await AssertImmediateOutputResult(
            jobData.FileSystemName,
            jobData.RootDirectoryPath,
            GetDataLakeClient(jobData),
            assertMethod: async (fileClient) =>
            {
                await AssertJsonResult(fileClient, StreamMode.Sync, VersionChangeType.Added, isSingleObject: true);
            });
    }

    [Fact]
    public async Task VerifyStoreData_Sync_WithStreamCacheAndJsonFormat()
    {
        await VerifyStoreData_Sync_WithStreamCache(
            "JSON",
            assertMethod: async (fileClient, _, _) =>
            {
                await AssertJsonResult(fileClient, StreamMode.Sync, VersionChangeType.Added);
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
                values.Add(nameof(DataLakeConstants.ShouldEscapeVocabularyKeys), false);
                values.Add(nameof(DataLakeConstants.ShouldWriteGuidAsString), false);
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
                values.Add(nameof(DataLakeConstants.ShouldEscapeVocabularyKeys), true);
                values.Add(nameof(DataLakeConstants.ShouldWriteGuidAsString), true);
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
                values.Add(nameof(DataLakeConstants.ShouldEscapeVocabularyKeys), false);
                values.Add(nameof(DataLakeConstants.ShouldWriteGuidAsString), false);
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
                values.Add(nameof(DataLakeConstants.ShouldEscapeVocabularyKeys), true);
                values.Add(nameof(DataLakeConstants.ShouldWriteGuidAsString), true);
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
                values.Add(nameof(DataLakeConstants.ShouldEscapeVocabularyKeys), false);
                values.Add(nameof(DataLakeConstants.ShouldWriteGuidAsString), false);
                values.Add(nameof(DataLakeConstants.IsArrayColumnsEnabled), true);
            });
    }

    [Fact]
    public async Task VerifyStoreData_Sync_WithStreamCacheAndParquetCanLoadToTable()
    {
        var tableName = Guid.NewGuid().ToString("N");
        await VerifyStoreData_Sync_WithStreamCache(
            "pArQuet",
            async (fileClient, fileSystemClient, setupContainerResult) =>
            {
                await AssertParquetResultEscaped(fileClient, fileSystemClient, setupContainerResult);
                var jobData = setupContainerResult.DataLakeJobData as OneLakeConnectorJobData;
                var dataLakeClient = GetDataLakeClient(jobData);
                var directoryName = $"{jobData.ItemName}.Lakehouse/Tables/{tableName}";

                var tableFile = await WaitForFileToBeCreated(
                    jobData.FileSystemName,
                    directoryName,
                    dataLakeClient,
                    paths => paths.Where(path => path.Name.EndsWith("parquet", StringComparison.OrdinalIgnoreCase)).ToList());

                Assert.NotNull(tableFile);
                await fileSystemClient.DeleteDirectoryAsync(directoryName);
            },
            configureAuthentication: (values) =>
            {
                values.Add(nameof(DataLakeConstants.ShouldEscapeVocabularyKeys), true);
                values.Add(nameof(DataLakeConstants.ShouldWriteGuidAsString), true);
                values.Add(nameof(OneLakeConstants.ShouldLoadToTable), true);
                values.Add(nameof(OneLakeConstants.TableName), tableName);
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
                var jobArgs = new DataLakeJobArgs
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
                    executeExportArg.FileSystemName,
                    executeExportArg.DirectoryName,
                executeExportArg.Client);

                var firstDataTime = await GetFileDataTime(executeExportArg, firstPath);
                await executeExportArg.ExportJob.DoRunAsync(
                    executeExportArg.ExecutionContext,
                    jobArgs);

                var secondPath = await WaitForFileToBeCreated(
                    executeExportArg.FileSystemName,
                    executeExportArg.DirectoryName,
                    executeExportArg.Client);
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
                var jobArgs = new DataLakeJobArgs
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
                    executeExportArg.FileSystemName,
                    executeExportArg.DirectoryName,
                executeExportArg.Client);

                var firstDataTime = await GetFileDataTime(executeExportArg, firstPath);
                await executeExportArg.ExportJob.DoRunAsync(
                    executeExportArg.ExecutionContext,
                    jobArgs);

                var secondPath = await WaitForFileToBeCreated(
                    executeExportArg.FileSystemName,
                    executeExportArg.DirectoryName,
                    executeExportArg.Client,
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
                var jobArgs = new DataLakeJobArgs
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
                    executeExportArg.FileSystemName,
                    executeExportArg.DirectoryName,
                executeExportArg.Client);

                var firstDataTime = await GetFileDataTime(executeExportArg, firstPath);
                await executeExportArg.ExportJob.DoRunAsync(
                    executeExportArg.ExecutionContext,
                    jobArgs);

                var secondPath = await WaitForFileToBeCreated(
                    executeExportArg.FileSystemName,
                    executeExportArg.DirectoryName,
                    executeExportArg.Client,
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

    private async Task VerifyStoreData_Sync_WithStreamCache(
        string format,
        Func<DataLakeFileClient, DataLakeFileSystemClient, SetupContainerResult, Task> assertMethod,
        Func<ExecuteExportArg, Task<PathItem>> executeExport = null,
        Action<Mock<IDateTimeOffsetProvider>> configureTimeProvider = null,
        Action<Dictionary<string, object>> configureAuthentication = null)
    {
        var configuration = CreateConfigurationWithStreamCache(format);
        configureAuthentication?.Invoke(configuration);
        var jobData = new OneLakeConnectorJobData(configuration);

        var setupResult = await SetupContainer(jobData, StreamMode.Sync);
        var connector = setupResult.ConnectorMock.Object;

        var data = CreateBaseConnectorEntityData(StreamMode.Sync, VersionChangeType.Added);
        await connector.StoreData(setupResult.Context, setupResult.StreamModel, data);
        var exportJob = CreateExportJob(setupResult);

        await AssertExportJobOutputFileContents(
            jobData.FileSystemName,
            jobData.RootDirectoryPath,
            setupResult,
            GetDataLakeClient(jobData),
            exportJob,
            assertMethod);
    }

    private protected override DataLakeExportEntitiesJobBase CreateExportJob(SetupContainerResult setupResult)
    {
        var logger = new Mock<ILogger<OneLakeClient>>();
        var dataLakeClient = new OneLakeClient(logger.Object);
        var exportJob = new OneLakeExportEntitiesJob(
            setupResult.ApplicationContext,
            setupResult.StreamRepositoryMock.Object,
            dataLakeClient,
            setupResult.ConstantsMock.Object,
            setupResult.JobDataFactoryMock.Object,
            setupResult.DateTimeOffsetProviderMock.Object);
        return exportJob;
    }

    private static DataLakeServiceClient GetDataLakeClient(OneLakeConnectorJobData jobData)
    {
        var sharedKeyCredential = GetCredential(jobData);
        return new DataLakeServiceClient(
            new Uri("https://onelake.dfs.fabric.microsoft.com"),
            sharedKeyCredential);
    }

    private static ClientSecretCredential GetCredential(OneLakeConnectorJobData jobData)
    {
        return new ClientSecretCredential(jobData.TenantId, jobData.ClientId, jobData.ClientSecret);
    }

    private Dictionary<string, object> CreateConfigurationWithoutStreamCache()
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
            { nameof(OneLakeConstants.TenantId), tenantId },
            { nameof(OneLakeConstants.ClientId), clientId },
            { nameof(OneLakeConstants.ClientSecret), clientSecretString },
            { nameof(OneLakeConstants.WorkspaceName), workspaceName },
            { nameof(OneLakeConstants.ItemName), itemName },
            { nameof(OneLakeConstants.ItemFolder), $"Files/{directoryName}" },
            { nameof(OneLakeConstants.ItemType), "Lakehouse" },
        };
    }

    private Dictionary<string, object> CreateConfigurationWithStreamCache(string format)
    {
        var baseConfiguration = CreateConfigurationWithoutStreamCache();
        var streamCacheConnectionStringEncoded = Environment.GetEnvironmentVariable("ONELAKE_STREAMCACHE");
        var streamCacheConnectionString = Encoding.UTF8.GetString(Convert.FromBase64String(streamCacheConnectionStringEncoded));
        Console.WriteLine(streamCacheConnectionString);
        Assert.NotNull(streamCacheConnectionString);

        var updatedConfiguration = new Dictionary<string, object>(baseConfiguration)
        {
            { nameof(DataLakeConstants.IsStreamCacheEnabled), true },
            { nameof(DataLakeConstants.StreamCacheConnectionString), streamCacheConnectionString },
            { nameof(DataLakeConstants.OutputFormat), format },
            { nameof(DataLakeConstants.UseCurrentTimeForExport), true },
            { nameof(DataLakeConstants.ContainerName), "test" },
        };
        return updatedConfiguration;
    }

    protected override Mock<OneLakeConnector> GetConnectorMock(
        Mock<IDateTimeOffsetProvider> mockDateTimeOffsetProvider,
        Mock<IOneLakeConstants> constantsMock,
        Mock<OneLakeJobDataFactory> jobDataFactory)
    {
        var logger = new Mock<ILogger<OneLakeClient>>();
        var mockConnector = new Mock<OneLakeConnector>(
            new Mock<ILogger<OneLakeConnector>>().Object,
            new OneLakeClient(logger.Object),
            constantsMock.Object,
            jobDataFactory.Object,
            mockDateTimeOffsetProvider.Object);
        return mockConnector;
    }
}

