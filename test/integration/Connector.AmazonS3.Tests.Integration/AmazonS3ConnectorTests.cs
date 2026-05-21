using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

using Amazon;
using Amazon.S3;
using Amazon.S3.Model;

using Castle.Windsor;

using CluedIn.Connector.AmazonS3.Connector;
using CluedIn.Connector.FileStorage.Common;
using CluedIn.Connector.FileStorage.Common.Connector;
using CluedIn.Connector.FileStorage.Common.Tests.Integration;
using CluedIn.Core;
using CluedIn.Core.Data.Parts;
using CluedIn.Core.Streams.Models;

using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

using Moq;

using Newtonsoft.Json;

using Xunit;
using Xunit.Abstractions;
using ExecutionContext = CluedIn.Core.ExecutionContext;

namespace CluedIn.Connector.AmazonS3.Tests.Integration;

/// <summary>
/// Integration tests for the Amazon S3 connector.
/// Requires environment variables: S3_ACCESSKEY, S3_SECRETKEY, S3_REGION, S3_BUCKETNAME
/// Tests will fail if environment variables are not set.
/// </summary>
public class AmazonS3ConnectorTests : StorageConnectorTestsBase<AmazonS3Connector, AmazonS3StorageFactory, IAmazonS3ConfigurationConstants>
{
    protected override Guid StorageProviderId => AmazonS3ConfigurationConstants.S3ProviderId;

    public AmazonS3ConnectorTests(ITestOutputHelper testOutputHelper)
        : base(testOutputHelper)
    {
    }

    //[Fact]
    //public async Task VerifyConnection_WhenValidCredentials_ReturnSuccess()
    //{
    //    var configuration = CreateConfigurationWithoutStreamCache();
    //    var storageConfiguration = new AmazonS3ConnectorConfiguration(configuration);

    //    var setupResult = await SetupContainer(storageConfiguration, StreamMode.EventStream);
    //    var connector = setupResult.ConnectorMock.Object;
    //    setupResult.StorageFactoryMock.Setup(factory => factory.CreateStorageConfiguration(setupResult.Context, configuration, It.IsAny<string>()))
    //        .Returns(Task.FromResult<IStorageConfiguration>(storageConfiguration));
    //    var result = await connector.VerifyConnection(setupResult.Context, configuration);
    //    Assert.NotNull(result);
    //    Assert.True(result.Success);
    //}

    //[Theory]
    //[InlineData("")]
    //[InlineData(" ")]
    //[InlineData(null)]
    //public async Task VerifyConnection_WhenInvalidAccessKey_ReturnInvalidAccessKeyErrorMessage(string accessKey)
    //{
    //    var configuration = CreateConfigurationWithoutStreamCache();
    //    configuration[nameof(AmazonS3ConfigurationConstants.AccessKey)] = accessKey;
    //    var storageConfiguration = new AmazonS3ConnectorConfiguration(configuration);

    //    var setupResult = await SetupContainer(storageConfiguration, StreamMode.EventStream);
    //    var connector = setupResult.ConnectorMock.Object;
    //    setupResult.StorageFactoryMock.Setup(factory => factory.CreateStorageConfiguration(setupResult.Context, configuration, It.IsAny<string>()))
    //        .Returns(Task.FromResult<IStorageConfiguration>(storageConfiguration));
    //    var result = await connector.VerifyConnection(setupResult.Context, configuration);
    //    Assert.NotNull(result);
    //    Assert.False(result.Success);
    //    Assert.Equal(AmazonS3Connector.InvalidAccessKeyErrorMessage, result.ErrorMessage);
    //}

    //[Theory]
    //[InlineData("")]
    //[InlineData(" ")]
    //[InlineData(null)]
    //public async Task VerifyConnection_WhenInvalidSecretKey_ReturnInvalidSecretKeyErrorMessage(string secretKey)
    //{
    //    var configuration = CreateConfigurationWithoutStreamCache();
    //    configuration[nameof(AmazonS3ConfigurationConstants.SecretKey)] = secretKey;
    //    var storageConfiguration = new AmazonS3ConnectorConfiguration(configuration);

    //    var setupResult = await SetupContainer(storageConfiguration, StreamMode.EventStream);
    //    var connector = setupResult.ConnectorMock.Object;
    //    setupResult.StorageFactoryMock.Setup(factory => factory.CreateStorageConfiguration(setupResult.Context, configuration, It.IsAny<string>()))
    //        .Returns(Task.FromResult<IStorageConfiguration>(storageConfiguration));
    //    var result = await connector.VerifyConnection(setupResult.Context, configuration);
    //    Assert.NotNull(result);
    //    Assert.False(result.Success);
    //    Assert.Equal(AmazonS3Connector.InvalidSecretKeyErrorMessage, result.ErrorMessage);
    //}

    //[Theory]
    //[InlineData("")]
    //[InlineData(" ")]
    //[InlineData(null)]
    //[InlineData("ALLCAPS")]
    //[InlineData("my_bucket")]
    //[InlineData("ab")]
    //public async Task VerifyConnection_WhenInvalidBucketName_ReturnInvalidBucketNameErrorMessage(string bucketName)
    //{
    //    var configuration = CreateConfigurationWithoutStreamCache();
    //    configuration[nameof(AmazonS3ConfigurationConstants.BucketName)] = bucketName;
    //    var storageConfiguration = new AmazonS3ConnectorConfiguration(configuration);

    //    var setupResult = await SetupContainer(storageConfiguration, StreamMode.EventStream);
    //    var connector = setupResult.ConnectorMock.Object;
    //    setupResult.StorageFactoryMock.Setup(factory => factory.CreateStorageConfiguration(setupResult.Context, configuration, It.IsAny<string>()))
    //        .Returns(Task.FromResult<IStorageConfiguration>(storageConfiguration));
    //    var result = await connector.VerifyConnection(setupResult.Context, configuration);
    //    Assert.NotNull(result);
    //    Assert.False(result.Success);
    //    Assert.Equal(AmazonS3Connector.InvalidBucketNameErrorMessage, result.ErrorMessage);
    //}

    //[Theory]
    //[InlineData(null)]
    //[InlineData("")]
    //[InlineData(" ")]
    //[InlineData("  ")]
    //public async Task VerifyConnection_WhenInvalidRegion_ReturnInvalidRegionErrorMessage(string region)
    //{
    //    var configuration = CreateConfigurationWithoutStreamCache();
    //    configuration[nameof(AmazonS3ConfigurationConstants.Region)] = region;
    //    var storageConfiguration = new AmazonS3ConnectorConfiguration(configuration);

    //    var setupResult = await SetupContainer(storageConfiguration, StreamMode.EventStream);
    //    var connector = setupResult.ConnectorMock.Object;
    //    setupResult.StorageFactoryMock.Setup(factory => factory.CreateStorageConfiguration(setupResult.Context, configuration, It.IsAny<string>()))
    //        .Returns(Task.FromResult<IStorageConfiguration>(storageConfiguration));
    //    var result = await connector.VerifyConnection(setupResult.Context, configuration);
    //    Assert.NotNull(result);
    //    Assert.False(result.Success);
    //    Assert.Equal(AmazonS3Connector.InvalidRegionErrorMessage, result.ErrorMessage);
    //}

    //[Fact]
    //public async Task VerifyStoreData_EventStream()
    //{
    //    var configuration = CreateConfigurationWithoutStreamCache();
    //    var storageConfiguration = new AmazonS3ConnectorConfiguration(configuration);

    //    var setupResult = await SetupContainer(storageConfiguration, StreamMode.EventStream);
    //    var connector = setupResult.ConnectorMock.Object;

    //    var data = CreateBaseConnectorEntityData(StreamMode.EventStream, VersionChangeType.Added);
    //    await connector.StoreData(setupResult.Context, setupResult.StreamModel, data);
    //    await AssertImmediateOutputResult(
    //        setupResult,
    //        assertMethod: async (setupResult, filePath) =>
    //        {
    //            await AssertJsonResult(setupResult, filePath, StreamMode.EventStream, VersionChangeType.Added);
    //        });
    //}

    //[Fact]
    //public async Task VerifyStoreData_Sync_WithoutStreamCache()
    //{
    //    var configuration = CreateConfigurationWithoutStreamCache();
    //    var storageConfiguration = new AmazonS3ConnectorConfiguration(configuration);

    //    var setupResult = await SetupContainer(storageConfiguration, StreamMode.Sync);
    //    var connector = setupResult.ConnectorMock.Object;

    //    var data = CreateBaseConnectorEntityData(StreamMode.Sync, VersionChangeType.Added);
    //    await connector.StoreData(setupResult.Context, setupResult.StreamModel, data);
    //    await AssertImmediateOutputResult(
    //        setupResult,
    //        assertMethod: async (setupResult, filePath) =>
    //        {
    //            await AssertJsonResult(setupResult, filePath, StreamMode.Sync, VersionChangeType.Added, isSingleObject: true);
    //        });
    //}

    //[Fact]
    //public async Task VerifyStoreData_Sync_WithStreamCacheAndJsonFormat()
    //{
    //    await VerifyStoreData_Sync_WithStreamCache(
    //        "JSON",
    //        assertMethod: async (setupResult, filePath) =>
    //        {
    //            await AssertJsonResult(setupResult, filePath, StreamMode.Sync, VersionChangeType.Added);
    //        });
    //}

    //[Fact]
    //public async Task VerifyStoreData_Sync_WithStreamCacheAndCsvFormatUnescaped()
    //{
    //    await VerifyStoreData_Sync_WithStreamCache(
    //        "csv",
    //        AssertCsvResultUnescaped,
    //        configureAuthentication: (values) =>
    //        {
    //            values.Add(nameof(StorageConfigurationConstants.ShouldEscapeVocabularyKeys), false);
    //            values.Add(nameof(StorageConfigurationConstants.ShouldWriteGuidAsString), false);
    //        });
    //}

    //[Fact]
    //public async Task VerifyStoreData_Sync_WithStreamCacheAndCsvFormatEscaped()
    //{
    //    await VerifyStoreData_Sync_WithStreamCache(
    //        "csv",
    //        AssertCsvResultEscaped,
    //        configureAuthentication: (values) =>
    //        {
    //            values.Add(nameof(StorageConfigurationConstants.ShouldEscapeVocabularyKeys), true);
    //            values.Add(nameof(StorageConfigurationConstants.ShouldWriteGuidAsString), true);
    //        });
    //}

    //[Fact]
    //public async Task VerifyStoreData_Sync_WithStreamCacheAndParquetFormatUnescaped()
    //{
    //    await VerifyStoreData_Sync_WithStreamCache(
    //        "pArQuet",
    //        AssertParquetResultUnescaped,
    //        configureAuthentication: (values) =>
    //        {
    //            values.Add(nameof(StorageConfigurationConstants.ShouldEscapeVocabularyKeys), false);
    //            values.Add(nameof(StorageConfigurationConstants.ShouldWriteGuidAsString), false);
    //        });
    //}

    //[Fact]
    //public async Task VerifyStoreData_Sync_WithStreamCacheAndParquetFormatWithEscaped()
    //{
    //    await VerifyStoreData_Sync_WithStreamCache(
    //        "pArQuet",
    //        AssertParquetResultEscaped,
    //        configureAuthentication: (values) =>
    //        {
    //            values.Add(nameof(StorageConfigurationConstants.ShouldEscapeVocabularyKeys), true);
    //            values.Add(nameof(StorageConfigurationConstants.ShouldWriteGuidAsString), true);
    //        });
    //}

    //[Fact]
    //public async Task VerifyStoreData_Sync_WithStreamCacheAndParquetFormatWithArrayColumnsEnabled()
    //{
    //    await VerifyStoreData_Sync_WithStreamCache(
    //        "pArQuet",
    //        AssertParquetResultArrayColumnEnabled,
    //        configureAuthentication: (values) =>
    //        {
    //            values.Add(nameof(StorageConfigurationConstants.ShouldEscapeVocabularyKeys), false);
    //            values.Add(nameof(StorageConfigurationConstants.ShouldWriteGuidAsString), false);
    //            values.Add(nameof(StorageConfigurationConstants.IsArrayColumnsEnabled), true);
    //        });
    //}

    //[Fact]
    //public async Task VerifyStoreData_Sync_WhenRepeatRunAndFileExistsUsingInternalSchedulerAndSameDataTime_CanSkip()
    //{
    //    await VerifyStoreData_Sync_WithStreamCache(
    //        "csv",
    //        AssertCsvResultEscaped,
    //        async executeExportArg =>
    //        {
    //            var jobArgs = new StorageJobArgs
    //            {
    //                OrganizationId = executeExportArg.Organization.Id.ToString(),
    //                Schedule = "0 0/1 * * *",
    //                Message = executeExportArg.StreamId.ToString(),
    //                IsTriggeredFromJobServer = false,
    //            };
    //            await executeExportArg.ExportJob.DoRunAsync(
    //                executeExportArg.ExecutionContext,
    //                jobArgs);

    //            var firstPath = await WaitForFileToBeCreated(executeExportArg.SetupContainerResult);

    //            var firstDataTime = await GetFileDataTime(executeExportArg, firstPath);
    //            await executeExportArg.ExportJob.DoRunAsync(
    //                executeExportArg.ExecutionContext,
    //                jobArgs);

    //            var secondPath = await WaitForFileToBeCreated(executeExportArg.SetupContainerResult);
    //            var secondDataTime = await GetFileDataTime(executeExportArg, secondPath);

    //            Assert.Equal(firstDataTime, secondDataTime);
    //            return secondPath;
    //        },
    //        configureAuthentication: (values) =>
    //        {
    //            values.Add(nameof(StorageConfigurationConstants.ShouldEscapeVocabularyKeys), true);
    //        });
    //}

    //[Fact]
    //public async Task VerifyStoreData_Sync_WhenRepeatRunAndFileExistsUsingInternalSchedulerAndDifferentDataTime_CanOverwrite()
    //{
    //    var executionCount = 0;
    //    var dateTimeList = new List<DateTimeOffset>
    //    {
    //        DefaultCurrentTime,
    //        new DateTimeOffset(2024, 8, 21, 4, 16, 0, TimeSpan.FromHours(5)),
    //    };
    //    await VerifyStoreData_Sync_WithStreamCache(
    //        "csv",
    //        AssertCsvResultEscaped,
    //        async executeExportArg =>
    //        {
    //            var jobArgs = new StorageJobArgs
    //            {
    //                OrganizationId = executeExportArg.Organization.Id.ToString(),
    //                Schedule = "0 0 1-31 * *",
    //                Message = executeExportArg.StreamId.ToString(),
    //                IsTriggeredFromJobServer = false,
    //            };
    //            await executeExportArg.ExportJob.DoRunAsync(
    //                executeExportArg.ExecutionContext,
    //                jobArgs);

    //            executionCount++;

    //            var firstPath = await WaitForFileToBeCreated(executeExportArg.SetupContainerResult);

    //            var firstDataTime = await GetFileDataTime(executeExportArg, firstPath);
    //            await executeExportArg.ExportJob.DoRunAsync(
    //                executeExportArg.ExecutionContext,
    //                jobArgs);

    //            var secondPath = await WaitForFileToBeCreated(
    //                executeExportArg.SetupContainerResult,
    //                filterPaths: paths =>
    //                {
    //                    return paths.Where(path => path.Name != firstPath.Name).ToList();
    //                });
    //            var secondDataTime = await GetFileDataTime(executeExportArg, secondPath);

    //            Assert.NotEqual(firstDataTime, secondDataTime);
    //            return secondPath;
    //        },
    //        mockDateTimeOffsetProvider =>
    //        {
    //            mockDateTimeOffsetProvider.Setup(x => x.GetCurrentUtcTime())
    //                .Returns(() =>
    //                {
    //                    return dateTimeList[executionCount];
    //                });
    //        },
    //        configureAuthentication: (values) =>
    //        {
    //            values.Add(nameof(StorageConfigurationConstants.ShouldEscapeVocabularyKeys), true);
    //        });
    //}

    [Fact]
    public async Task VerifyStoreData_Sync_WhenRepeatRunAndFileExistsUsingJobServer_CanCreateNewFile()
    {
        Console.WriteLine("AmazonS3ConnectorTests - " + this.GetHashCode() + " - VerifyStoreData_Sync_WhenRepeatRunAndFileExistsUsingJobServer_CanCreateNewFile");
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
                Console.WriteLine("AmazonS3ConnectorTests - " + this.GetHashCode() + " - VerifyStoreData_Sync_WhenRepeatRunAndFileExistsUsingJobServer_CanCreateNewFile - ExecuteExport");
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
            },
            configureAuthentication: (values) =>
            {
                values.Add(nameof(StorageConfigurationConstants.ShouldEscapeVocabularyKeys), true);
            });
    }

    //[Fact]
    //public async Task VerifyStoreData_Sync_WithStreamCacheCanIgnoreWhenChangedAfterDeletion()
    //{
    //    await VerifyStoreData_Sync_WithStreamCache("csv",
    //        async (setupResult, filePath) => await AssertCsvResult(setupResult, filePath, "_", (rows) =>
    //        {
    //            var removed = rows.ToList();
    //            removed.Clear();
    //            return removed;
    //        }),
    //        getConnectorEntityData: () =>
    //        {
    //            var initialUserData = UserData.Default;
    //            var removedUserData = initialUserData with { Age = initialUserData.Age + 1 };
    //            var readdAfterDeletionUserData = initialUserData with { Age = initialUserData.Age + 2 };

    //            var initialEntityData = CreateBaseConnectorEntityData(StreamMode.Sync, VersionChangeType.Added, persistVersion: 1, userData: initialUserData);
    //            var removedEntityData = CreateBaseConnectorEntityData(StreamMode.Sync, VersionChangeType.Removed, persistVersion: 2, userData: removedUserData);
    //            var readdAfterDeletionEntityData = CreateBaseConnectorEntityData(StreamMode.Sync, VersionChangeType.Changed, persistVersion: 3, userData: readdAfterDeletionUserData);

    //            // Intermediate version is outdated when final version is stored, so it should be ignored and not cause the export to fail
    //            return new[] { initialEntityData, removedEntityData, readdAfterDeletionEntityData };
    //        },
    //        configureAuthentication: (values) =>
    //        {
    //            values.Add(nameof(StorageConfigurationConstants.ShouldEscapeVocabularyKeys), true);
    //            values.Add(nameof(StorageConfigurationConstants.ShouldWriteGuidAsString), true);
    //        });
    //}

    private protected override StorageExportEntitiesJobBase CreateExportJob(SetupContainerResult setupResult)
    {

        Console.WriteLine("AmazonS3ConnectorTests - " + this.GetHashCode() + " - CreateExportJob");
        var exportJob = new AmazonS3ExportEntitiesJob(
            setupResult.ApplicationContext,
            setupResult.StreamRepositoryMock.Object,
            setupResult.ConstantsMock.Object,
            setupResult.StorageFactoryMock.Object,
            setupResult.DateTimeOffsetProviderMock.Object);
        return exportJob;
    }

    private protected override Dictionary<string, object> CreateConfigurationWithoutStreamCache()
    {
        var accessKey = Environment.GetEnvironmentVariable("S3_ACCESSKEY");
        var secretKey = Environment.GetEnvironmentVariable("S3_SECRETKEY");
        var region = Environment.GetEnvironmentVariable("S3_REGION");
        var bucketName = Environment.GetEnvironmentVariable("S3_BUCKETNAME");

        var maskedSecret = string.IsNullOrWhiteSpace(secretKey) ? string.Empty
            : $"{secretKey[0..3]}{new string('*', Math.Max(secretKey.Length - 3, 0))}";
        TestOutputHelper.WriteLine(
            "Using AccessKey: '{0}', SecretKey: '{1}', Region: '{2}', BucketName: '{3}'.",
            accessKey,
            maskedSecret,
            region,
            bucketName);
        Assert.NotNull(accessKey);
        Assert.NotNull(maskedSecret);
        Assert.NotNull(region);
        Assert.NotNull(bucketName);

        var testPrefix = $"xunit-prefix-{DateTime.Now.Ticks}";
        return new Dictionary<string, object>()
        {
            { AmazonS3ConfigurationConstants.AccessKey, accessKey },
            { AmazonS3ConfigurationConstants.SecretKey, secretKey },
            { AmazonS3ConfigurationConstants.Region, region },
            { AmazonS3ConfigurationConstants.BucketName, bucketName },
            { AmazonS3ConfigurationConstants.DirectoryName, testPrefix },
        };
    }

    protected override Mock<AmazonS3Connector> GetConnectorMock(
        ApplicationContext applicationContext,
        Mock<IDateTimeOffsetProvider> mockDateTimeOffsetProvider,
        Mock<IAmazonS3ConfigurationConstants> constantsMock,
        Mock<AmazonS3StorageFactory> storageConfigurationFactory)
    {
        Console.WriteLine("AmazonS3ConnectorTests - " + this.GetHashCode() + " - GetConnectorMock");
        var logger = new Mock<ILogger<AmazonS3Connector>>();
        logger.Setup(x => x.IsEnabled(It.IsAny<LogLevel>())).Returns(true);
        logger.Setup(x => x.Log(
            It.IsAny<LogLevel>(),
            It.IsAny<EventId>(),
            It.IsAny<It.IsAnyType>(),
            It.IsAny<Exception>(),
            It.IsAny<Func<It.IsAnyType, Exception, string>>()))
            .Callback(new InvocationAction(invocation =>
            {
                // 3. Extract the message and write to Console
                var logLevel = invocation.Arguments[0];
                var state = invocation.Arguments[2];
                var exception = (Exception)invocation.Arguments[3];
                var formatter = invocation.Arguments[4];

                // Use the formatter to get the actual string message
                var delegateFormatter = (Delegate)formatter;
                var message = delegateFormatter.DynamicInvoke(state, exception);

                Console.WriteLine($"[{logLevel}] {message}");
            }));
        var mockConnector = new Mock<AmazonS3Connector>(
            logger.Object,
            applicationContext,
            constantsMock.Object,
            storageConfigurationFactory.Object,
            mockDateTimeOffsetProvider.Object);
        return mockConnector;
    }

    protected override Mock<AmazonS3StorageFactory> CreateStorageFactoryMock(
        WindsorContainer container,
        ApplicationContext applicationContext,
        Mock<IDateTimeOffsetProvider> mockDateTimeOffsetProvider)
    {
        var storageFactory = new Mock<AmazonS3StorageFactory>();
        storageFactory.Setup(x => x.CreateStorageClient(It.IsAny<ExecutionContext>(), It.IsAny<IStorageConfiguration>()))
            .Returns<ExecutionContext, IStorageConfiguration>((_, data) => Task.FromResult<IStorageClient>(new AmazonS3StorageClient(NullLogger<AmazonS3StorageClient>.Instance, data as AmazonS3ConnectorConfiguration)));
        return storageFactory;
    }

    private static readonly object S3ClientLock = new();
    private static readonly Dictionary<string, IAmazonS3> S3Clients = new();

    private protected virtual IAmazonS3 CreateS3Client(SetupContainerResult setupContainerResult)
    {
        var config = setupContainerResult.StorageConfiguration as AmazonS3ConnectorConfiguration;
        var clientKey = $"{config.AccessKey}|{config.SecretKey}|{config.Region}";

        lock (S3ClientLock)
        {
            if (!S3Clients.TryGetValue(clientKey, out var client))
            {
                client = new AmazonS3Client(config.AccessKey, config.SecretKey, RegionEndpoint.GetBySystemName(config.Region));
                S3Clients[clientKey] = client;
            }

            return client;
        }
    }

    private protected virtual string GetBucketName(SetupContainerResult setupContainerResult)
    {
        var config = setupContainerResult.StorageConfiguration as AmazonS3ConnectorConfiguration;
        return config.BucketName;
    }
    private protected virtual string GetDirectoryName(SetupContainerResult setupContainerResult)
    {
        var config = setupContainerResult.StorageConfiguration as AmazonS3ConnectorConfiguration;
        return config.DirectoryName;
    }

    private protected override StorageConfigurationBase CreateStorageConfiguration(Dictionary<string, object> configuration)
    {
        return new AmazonS3ConnectorConfiguration(configuration);
    }

    private protected override async Task<ExportedFilePath> WaitForFileToBeCreated(
        SetupContainerResult setupContainerResult,
        Func<IList<ExportedFilePath>, IList<ExportedFilePath>> filterPaths = null,
        Func<SetupContainerResult, string> getDirectoryName = null)
    {
        ExportedFilePath path;
        var startTime = DateTime.Now;
        var timeoutTime = startTime.AddSeconds(30);
        while (true)
        {
            if (DateTime.Now > timeoutTime)
            {
                throw new TimeoutException();
            }

            var client = CreateS3Client(setupContainerResult);
            var directoryName = getDirectoryName == null ? GetDirectoryName(setupContainerResult) : getDirectoryName(setupContainerResult);


            var s3Objects = await GetFiles(client, directoryName, GetBucketName(setupContainerResult), timeoutTime);

            var paths = s3Objects.Select(p =>
            {
                var fileName = Path.GetFileName(p.Key);
                var directoryPath = p.Key[0..^fileName.Length];
                var trimmedDirectoryPath = directoryPath.EndsWith("/") ? directoryPath[0..^1] : directoryPath;
                return new ExportedFilePath(fileName, trimmedDirectoryPath, p.Size);
            }).ToList();

            paths = filterPaths == null ? paths : filterPaths(paths).ToList();

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
    async Task<List<S3Object>> GetFiles(IAmazonS3 client, string prefix, string bucketName, DateTime timeoutTime)
    {

        var listRequest = new ListObjectsV2Request
        {
            BucketName = bucketName,
            Prefix = string.IsNullOrEmpty(prefix) ? null : prefix + "/",
        };

        var result = new List<S3Object>();
        ListObjectsV2Response response;
        do
        {
            if (DateTime.Now > timeoutTime)
            {
                _testOutputHelper.WriteLine("Timeout while waiting for S3 objects with prefix '{0}' in bucket '{1}'.", prefix, bucketName);
                _testOutputHelper.WriteLine("Results {0}", JsonConvert.SerializeObject(result, Formatting.Indented));
                throw new TimeoutException("Timeout while waiting for S3 objects.");
            }

            response = await client.ListObjectsV2Async(listRequest);

            foreach (var s3Object in response.S3Objects)
            {
                if (!s3Object.Key.EndsWith("/"))
                {
                    result.Add(s3Object);
                }
            }

            listRequest.ContinuationToken = response.NextContinuationToken;
        } while (response?.IsTruncated == true);

        return result;
    }

    private protected override async Task<Stream> GetContents(SetupContainerResult setupContainerResult, ExportedFilePath path)
    {
        var client = CreateS3Client(setupContainerResult);
        var bucket = GetBucketName(setupContainerResult);
        var key = $"{path.DirectoryPath}/{path.Name}";

        var getResponse = await client.GetObjectAsync(bucket, key);
        var memoryStream = new MemoryStream();
        await getResponse.ResponseStream.CopyToAsync(memoryStream);
        memoryStream.Position = 0;
        return memoryStream;
    }

    private protected override async Task CleanUpExportedFile(SetupContainerResult setupContainerResult, ExportedFilePath path)
    {
        var client = CreateS3Client(setupContainerResult);
        var bucket = GetBucketName(setupContainerResult);
        var key = $"{path.DirectoryPath}/{path.Name}";
        try
        {
            await client.DeleteObjectAsync(bucket, key);
        }
        catch (AmazonS3Exception ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            // File doesn't exist, nothing to delete
        }
    }

    private protected override async Task CleanUpAfterImmediateOutputTest(SetupContainerResult setupContainerResult)
    {
        var client = CreateS3Client(setupContainerResult);
        var bucket = GetBucketName(setupContainerResult);
        var directoryName = GetDirectoryName(setupContainerResult);
        var listRequest = new ListObjectsV2Request
        {
            BucketName = bucket,
            Prefix = directoryName + "/",
        };

        ListObjectsV2Response response;
        do
        {
            response = await client.ListObjectsV2Async(listRequest);
            var objects = response.S3Objects;

            if (objects != null && objects.Any())
            {
                var deleteRequest = new DeleteObjectsRequest
                {
                    BucketName = bucket,
                    Objects = response.S3Objects.Select(o => new KeyVersion { Key = o.Key }).ToList(),
                };

                await client.DeleteObjectsAsync(deleteRequest);
            }

            listRequest.ContinuationToken = response.NextContinuationToken;
        } while (response?.IsTruncated == true);
    }

    private protected override async Task CleanUpAfterFileOutputTest(SetupContainerResult setupContainerResult)
    {
        await CleanUpAfterImmediateOutputTest(setupContainerResult);
        var streamModel = setupContainerResult.StreamModel;
        var jobData = setupContainerResult.StorageConfiguration;
        await DeleteTable(streamModel.Id, jobData.StreamCacheConnectionString);
    }

    private protected override async Task WaitForFileToBeDeleted(SetupContainerResult setupContainerResult, ExportedFilePath path)
    {
        var d = DateTime.Now;
        while (true)
        {
            if (DateTime.Now > d.AddSeconds(30))
            {
                throw new TimeoutException("Timeout waiting for file to be deleted");
            }

            var client = CreateS3Client(setupContainerResult);
            var bucket = GetBucketName(setupContainerResult);
            var key = $"{path.DirectoryPath}/{path.Name}";

            try
            {
                await client.GetObjectMetadataAsync(bucket, key);
                await Task.Delay(1000);
                continue;
            }
            catch (AmazonS3Exception ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
                break;
            }
        }
    }

    private protected override async Task<DateTimeOffset> GetFileDataTime(ExecuteExportArg executeExportArg, ExportedFilePath path)
    {
        var client = CreateS3Client(executeExportArg.SetupContainerResult);
        var bucket = GetBucketName(executeExportArg.SetupContainerResult);
        var key = $"{path.DirectoryPath}/{path.Name}";
        var metadata = await client.GetObjectMetadataAsync(bucket, key);
        var metadataDict = new Dictionary<string, string>();
        foreach (var metaKey in metadata.Metadata.Keys)
        {
            metadataDict[metaKey.Replace("x-amz-meta-", string.Empty)] = metadata.Metadata[metaKey];
        }

        var fileDataTime = metadataDict["datatime"];

        return DateTimeOffset.Parse(fileDataTime);
    }
}

