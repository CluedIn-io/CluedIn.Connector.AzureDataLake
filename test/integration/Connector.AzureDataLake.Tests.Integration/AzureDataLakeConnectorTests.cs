using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

using Azure.Storage;
using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Models;

using CluedIn.Connector.AzureDataLake.Connector;
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

namespace CluedIn.Connector.AzureDataLake.Tests.Integration;

public class AzureDataLakeConnectorTests : DataLakeConnectorTestsBase<AzureDataLakeConnector, AzureDataLakeJobDataFactory, IAzureDataLakeConstants>
{
    protected override Guid DataLakeProviderId => AzureDataLakeConstants.DataLakeProviderId;

    public AzureDataLakeConnectorTests(ITestOutputHelper testOutputHelper)
        : base(testOutputHelper)
    {
    }

    [Fact]
    public async Task VerifyStoreData_EventStream()
    {
        var configuration = CreateConfigurationWithoutStreamCache();
        var jobData = new AzureDataLakeConnectorJobData(configuration);

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
        var jobData = new AzureDataLakeConnectorJobData(configuration);

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
        await VerifyStoreData_Sync_WithStreamCache("csv", AssertCsvResultUnescaped);
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
        await VerifyStoreData_Sync_WithStreamCache("pArQuet", AssertParquetResultUnescaped);
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
                values.Add(nameof(DataLakeConstants.IsArrayColumnsEnabled), true);
            });
    }

    [Fact]
    public async Task VerifyStoreData_Sync_WhenRepeatRunAndFileExistsUsingInternalSchedulerAndSameDataTime_CanSkip()
    {
        await VerifyStoreData_Sync_WithStreamCache(
            "csv",
            AssertCsvResultUnescaped,
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
            AssertCsvResultUnescaped,
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
            AssertCsvResultUnescaped,
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

    [Fact]
    public async Task GetContainers_InvalidParamsTest()
    {
        var configuration = CreateConfigurationWithoutStreamCache();
        var jobData = new AzureDataLakeConnectorJobData(configuration);

        var setupResult = await SetupContainer(jobData, StreamMode.Sync);
        var connector = setupResult.ConnectorMock.Object;
        var containers = await connector.GetContainers(setupResult.Context, setupResult.ProviderDefinition.Id);
        Assert.Null(containers);

        //This is an existing container in the Azure Data Lake account
        //There are existing files in the directory
        //Changing this or removing the files will cause the test to fail
        configuration[AzureDataLakeConstants.FileSystemName] = "apac-container";
        configuration[AzureDataLakeConstants.DirectoryName] = "TestExport01";

        containers = await connector.GetContainers(setupResult.Context, setupResult.ProviderDefinition.Id);
        Assert.NotNull(containers);
    }

    [Fact]
    public async Task GetContainers_HasValuesTest()
    {
        var configuration = CreateConfigurationWithoutStreamCache();
        //This is an existing container in the Azure Data Lake account
        //There are existing files in the directory
        //Changing this or removing the files will cause the test to fail
        configuration[AzureDataLakeConstants.FileSystemName] = "apac-container";
        configuration[AzureDataLakeConstants.DirectoryName] = "TestExport01";
        var jobData = new AzureDataLakeConnectorJobData(configuration);

        var setupResult = await SetupContainer(jobData, StreamMode.Sync);
        var connector = setupResult.ConnectorMock.Object;
        var containers = await connector.GetContainers(setupResult.Context, setupResult.ProviderDefinition.Id);

        Assert.NotNull(containers);
        Assert.NotEmpty(containers);
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
        var jobData = new AzureDataLakeConnectorJobData(configuration);

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
        var azureDataLakeClient = new AzureDataLakeClient();
        var exportJob = new AzureDataLakeExportEntitiesJob(
            setupResult.ApplicationContext,
            setupResult.StreamRepositoryMock.Object,
            azureDataLakeClient,
            setupResult.ConstantsMock.Object,
            setupResult.JobDataFactoryMock.Object,
            setupResult.DateTimeOffsetProviderMock.Object);
        return exportJob;
    }



    private static DataLakeServiceClient GetDataLakeClient(AzureDataLakeConnectorJobData jobData)
    {
        return new DataLakeServiceClient(
            new Uri($"https://{jobData.AccountName}.dfs.core.windows.net"),
        new StorageSharedKeyCredential(jobData.AccountName, jobData.AccountKey));
    }

    private Dictionary<string, object> CreateConfigurationWithoutStreamCache()
    {
        var accountName = Environment.GetEnvironmentVariable("ADL2_ACCOUNTNAME");
        Assert.NotNull(accountName);
        var accountKey = Environment.GetEnvironmentVariable("ADL2_ACCOUNTKEY");
        Assert.NotNull(accountKey);

        var fileSystemName = $"xunit-fs-{DateTime.Now.Ticks}";
        var directoryName = $"xunit-{DateTime.Now.Ticks}";
        return new Dictionary<string, object>()
        {
            { nameof(AzureDataLakeConstants.AccountName), accountName },
            { nameof(AzureDataLakeConstants.AccountKey), accountKey },
            { nameof(AzureDataLakeConstants.FileSystemName), fileSystemName },
            { nameof(AzureDataLakeConstants.DirectoryName), directoryName },
        };
    }

    private Dictionary<string, object> CreateConfigurationWithStreamCache(string format)
    {
        var baseConfiguration = CreateConfigurationWithoutStreamCache();
        var streamCacheConnectionStringEncoded = Environment.GetEnvironmentVariable("ADL2_STREAMCACHE");
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

    protected override Mock<AzureDataLakeConnector> GetConnectorMock(
        Mock<IDateTimeOffsetProvider> mockDateTimeOffsetProvider,
        Mock<IAzureDataLakeConstants> constantsMock,
        Mock<AzureDataLakeJobDataFactory> jobDataFactory)
    {
        var mockConnector = new Mock<AzureDataLakeConnector>(
            new Mock<ILogger<AzureDataLakeConnector>>().Object,
            new AzureDataLakeClient(),
            constantsMock.Object,
            jobDataFactory.Object,
            mockDateTimeOffsetProvider.Object);
        return mockConnector;
    }
}

