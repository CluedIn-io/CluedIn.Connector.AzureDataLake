using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Models;

using CluedIn.Connector.FabricOpenMirroring.Connector;
using CluedIn.Connector.DataLake.Common;
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
using CluedIn.Connector.DataLake.Common.Connector;

namespace CluedIn.Connector.FabricOpenMirroring.Tests.Integration;

public class OpenMirroringConnectorTests : DataLakeConnectorTestsBase<OpenMirroringConnector, OpenMirroringJobDataFactory, IOpenMirroringConstants>
{
    protected override Guid DataLakeProviderId => OpenMirroringConstants.DataLakeProviderId;
    protected override bool IsFixedFileSystem => true;

    public OpenMirroringConnectorTests(ITestOutputHelper testOutputHelper)
        : base(testOutputHelper)
    {
    }

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
    public async Task VerifyStoreData_Sync_WhenRepeatRunAndFileExistsUsingInternalSchedulerAndDifferentDataTime_CanCreateNewFile()
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
            "parquet",
            AssertParquetResultEscapedWithRowMarker,
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

    private async Task AssertParquetResultEscapedWithRowMarker(
        DataLakeFileClient fileClient,
        DataLakeFileSystemClient fileSystemClient,
        SetupContainerResult setupContainerResult)
    {
        await base.AssertParquetResult(fileClient, separator: "_", isArrayColumnEnabled: false, formatResult: (original) =>
        {
            var result = original.ToList();
            original.First().Columns["__rowMarker__"] = "4";
            return result;
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
        var jobData = new OpenMirroringConnectorJobData(configuration);

        var setupResult = await SetupContainer(jobData, StreamMode.Sync, configureTimeProvider);
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
            assertMethod,
            executeExport);
    }

    private protected override DataLakeExportEntitiesJobBase CreateExportJob(SetupContainerResult setupResult)
    {
        var logger = new Mock<ILogger<OpenMirroringClient>>();
        var dataLakeClient = new OpenMirroringClient(logger.Object, setupResult.DateTimeOffsetProviderMock.Object);
        var exportJob = new OpenMirroringExportEntitiesJob(
            setupResult.ApplicationContext,
            setupResult.StreamRepositoryMock.Object,
            dataLakeClient,
            setupResult.ConstantsMock.Object,
            setupResult.JobDataFactoryMock.Object,
            setupResult.DateTimeOffsetProviderMock.Object);
        return exportJob;
    }

    private static DataLakeServiceClient GetDataLakeClient(OpenMirroringConnectorJobData jobData)
    {
        var sharedKeyCredential = new ClientSecretCredential(jobData.TenantId, jobData.ClientId, jobData.ClientSecret);
        return new DataLakeServiceClient(
            new Uri("https://onelake.dfs.fabric.microsoft.com"),
            sharedKeyCredential);
    }

    private Dictionary<string, object> CreateConfigurationWithoutStreamCache()
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
            { nameof(OpenMirroringConstants.TenantId), tenantId },
            { nameof(OpenMirroringConstants.ClientId), clientId },
            { nameof(OpenMirroringConstants.ClientSecret), clientSecretString },
            { nameof(OpenMirroringConstants.WorkspaceName), workspaceName },
            { nameof(OpenMirroringConstants.MirroredDatabaseName), mirroredDatabaseName },
            { nameof(OpenMirroringConstants.ShouldCreateMirroredDatabase), false },
        };
    }

    private Dictionary<string, object> CreateConfigurationWithStreamCache(string format)
    {
        var baseConfiguration = CreateConfigurationWithoutStreamCache();
        var streamCacheConnectionStringEncoded = Environment.GetEnvironmentVariable("FABRICOPENMIRRORING_STREAMCACHE");
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

    protected override Mock<OpenMirroringConnector> GetConnectorMock(
        Mock<IDateTimeOffsetProvider> mockDateTimeOffsetProvider,
        Mock<IOpenMirroringConstants> constantsMock,
        Mock<OpenMirroringJobDataFactory> jobDataFactory)
    {
        var logger = new Mock<ILogger<OpenMirroringClient>>();
        var mockConnector = new Mock<OpenMirroringConnector>(
            new Mock<ILogger<OpenMirroringConnector>>().Object,
            new OpenMirroringClient(logger.Object, mockDateTimeOffsetProvider.Object),
            constantsMock.Object,
            jobDataFactory.Object,
            mockDateTimeOffsetProvider.Object);
        return mockConnector;
    }

    protected override Task<PathItem> WaitForFileToBeCreated(string fileSystemName, string directoryName, DataLakeServiceClient client, Func<IList<PathItem>, IList<PathItem>> filterPaths = null)
    {
        return base.WaitForFileToBeCreated(fileSystemName, directoryName, client, filterPaths: (paths) =>
        {
            var metadataFiltered = paths.Where(path => !path.Name.EndsWith("/_metadata.json") && !path.Name.EndsWith("_partnerEvents.json")).ToList();
            return filterPaths?.Invoke(metadataFiltered) ?? metadataFiltered;
        });
    }
}

