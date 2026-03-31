using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

using Amazon;
using Amazon.S3;
using Amazon.S3.Model;

using Azure.Identity;

using Castle.MicroKernel.Registration;
using Castle.Windsor;

using CluedIn.Connector.AmazonS3.Connector;
using CluedIn.Connector.DataLake.Common.Tests.Integration;
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

using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

using Moq;

using Xunit;
using Xunit.Abstractions;
using Xunit.Sdk;

using ExecutionContext = CluedIn.Core.ExecutionContext;

namespace CluedIn.Connector.AmazonS3.Tests.Integration;

/// <summary>
/// Integration tests for the Amazon S3 connector.
/// Requires environment variables: S3_ACCESSKEY, S3_SECRETKEY, S3_REGION, S3_BUCKETNAME
/// Tests will fail if environment variables are not set.
/// </summary>
//public class AmazonS3ConnectorTests : IAsyncLifetime
//{
//    private readonly ITestOutputHelper _testOutputHelper;
//    private readonly List<string> _keysToCleanup = new();
//    private string _testPrefix;

//    public AmazonS3ConnectorTests(ITestOutputHelper testOutputHelper)
//    {
//        _testOutputHelper = testOutputHelper;
//    }

//    public Task InitializeAsync()
//    {
//        _testPrefix = $"xunit-{DateTime.UtcNow.Ticks}";
//        return Task.CompletedTask;
//    }

//    public async Task DisposeAsync()
//    {
//        try
//        {
//            var s3 = CreateS3Client();
//            if (s3 == null) return;
//            var bucket = GetBucketName();

//            foreach (var key in _keysToCleanup)
//            {
//                try { await s3.DeleteObjectAsync(bucket, key); } catch { /* best effort */ }
//            }
//        }
//        catch { /* best effort */ }
//    }

//    [Fact]
//    public async Task VerifyConnection_WhenValid_ReturnsSuccess()
//    {
//        var (connector, context, configuration, jobData) = SetupConnector();

//        var result = await connector.VerifyConnection(context, configuration);

//        Assert.NotNull(result);
//        Assert.True(result.Success);
//    }

//    [Theory]
//    [InlineData("")]
//    [InlineData(" ")]
//    [InlineData(null)]
//    public async Task VerifyConnection_WhenInvalidAccessKey_ReturnError(string accessKey)
//    {
//        var (connector, context, configuration, _) = SetupConnector();

//        configuration[AmazonS3ConfigurationConstants.AccessKey] = accessKey;
//        var jobData = new AmazonS3ConnectorConfiguration(configuration);
//        SetupJobDataFactory(connector, configuration, jobData);

//        var result = await connector.VerifyConnection(context, configuration);

//        Assert.NotNull(result);
//        Assert.False(result.Success);
//        Assert.Equal(AmazonS3Connector.InvalidAccessKeyErrorMessage, result.ErrorMessage);
//    }

//    [Theory]
//    [InlineData("")]
//    [InlineData(" ")]
//    [InlineData(null)]
//    public async Task VerifyConnection_WhenInvalidSecretKey_ReturnError(string secretKey)
//    {
//        var (connector, context, configuration, _) = SetupConnector();

//        configuration[AmazonS3ConfigurationConstants.SecretKey] = secretKey;
//        var jobData = new AmazonS3ConnectorConfiguration(configuration);
//        SetupJobDataFactory(connector, configuration, jobData);

//        var result = await connector.VerifyConnection(context, configuration);

//        Assert.NotNull(result);
//        Assert.False(result.Success);
//        Assert.Equal(AmazonS3Connector.InvalidSecretKeyErrorMessage, result.ErrorMessage);
//    }

//    [Theory]
//    [InlineData("")]
//    [InlineData(" ")]
//    [InlineData(null)]
//    [InlineData("ALLCAPS")]
//    [InlineData("my_bucket")]
//    [InlineData("ab")]
//    public async Task VerifyConnection_WhenInvalidBucketName_ReturnError(string bucketName)
//    {
//        var (connector, context, configuration, _) = SetupConnector();

//        configuration[AmazonS3ConfigurationConstants.BucketName] = bucketName;
//        var jobData = new AmazonS3ConnectorConfiguration(configuration);
//        SetupJobDataFactory(connector, configuration, jobData);

//        var result = await connector.VerifyConnection(context, configuration);

//        Assert.NotNull(result);
//        Assert.False(result.Success);
//        Assert.Equal(AmazonS3Connector.InvalidBucketNameErrorMessage, result.ErrorMessage);
//    }

//    [Theory]
//    [InlineData("")]
//    [InlineData(" ")]
//    [InlineData(null)]
//    public async Task VerifyConnection_WhenInvalidRegion_ReturnError(string region)
//    {
//        var (connector, context, configuration, _) = SetupConnector();

//        configuration[AmazonS3ConfigurationConstants.Region] = region;
//        var jobData = new AmazonS3ConnectorConfiguration(configuration);
//        SetupJobDataFactory(connector, configuration, jobData);

//        var result = await connector.VerifyConnection(context, configuration);

//        Assert.NotNull(result);
//        Assert.False(result.Success);
//        Assert.Equal(AmazonS3Connector.InvalidRegionErrorMessage, result.ErrorMessage);
//    }

//    [Fact]
//    public async Task StoreData_EventStream_WritesJsonToS3()
//    {
//        var (connector, context, configuration, jobData) = SetupConnector(StreamMode.EventStream);

//        var streamModel = CreateStreamModel(StreamMode.EventStream, Guid.Parse("c444cda8-d9b5-45cc-a82d-fef28e08d55c"));
//        var data = CreateBaseConnectorEntityData(StreamMode.EventStream, VersionChangeType.Added);

//        await connector.StoreData(context, streamModel, data);

//        // Verify file was written to S3
//        var s3 = CreateS3Client();
//        var bucket = GetBucketName();
//        var prefix = jobData.RootDirectoryPath;

//        var listResponse = await s3.ListObjectsV2Async(new ListObjectsV2Request
//        {
//            BucketName = bucket,
//            Prefix = prefix + "/",
//        });

//        Assert.NotEmpty(listResponse.S3Objects);
//        _keysToCleanup.AddRange(listResponse.S3Objects.Select(o => o.Key));

//        var obj = listResponse.S3Objects.First();
//        var getResponse = await s3.GetObjectAsync(bucket, obj.Key);
//        using var reader = new StreamReader(getResponse.ResponseStream);
//        var content = await reader.ReadToEndAsync();

//        Assert.Contains("Jean Luc Picard", content);
//        Assert.Contains("f55c66dc-7881-55c9-889f-344992e71cb8", content);
//    }

//    [Fact]
//    public async Task StoreData_Sync_WritesJsonToS3()
//    {
//        var (connector, context, configuration, jobData) = SetupConnector(StreamMode.Sync);

//        var streamModel = CreateStreamModel(StreamMode.Sync, Guid.Parse("c444cda8-d9b5-45cc-a82d-fef28e08d55c"));
//        var data = CreateBaseConnectorEntityData(StreamMode.Sync, VersionChangeType.Added);

//        await connector.StoreData(context, streamModel, data);

//        var s3 = CreateS3Client();
//        var bucket = GetBucketName();
//        var prefix = jobData.RootDirectoryPath;

//        var listResponse = await s3.ListObjectsV2Async(new ListObjectsV2Request
//        {
//            BucketName = bucket,
//            Prefix = prefix + "/",
//        });

//        Assert.NotEmpty(listResponse.S3Objects);
//        _keysToCleanup.AddRange(listResponse.S3Objects.Select(o => o.Key));

//        var obj = listResponse.S3Objects.First();
//        var getResponse = await s3.GetObjectAsync(bucket, obj.Key);
//        using var reader = new StreamReader(getResponse.ResponseStream);
//        var content = await reader.ReadToEndAsync();

//        Assert.Contains("Jean Luc Picard", content);
//    }

//    [Fact]
//    public async Task GetContainers_WhenBucketEmpty_ReturnsEmptyOrNull()
//    {
//        var (connector, context, configuration, jobData) = SetupConnector();

//        var containers = await connector.GetContainers(context, Guid.Parse("c444cda8-d9b5-45cc-a82d-fef28e08d55c"));

//        // Either null or empty is acceptable for an empty directory
//        Assert.True(containers == null || !containers.Any());
//    }

//    #region Setup Helpers

//    private (AmazonS3Connector Connector, ExecutionContext Context, Dictionary<string, object> Configuration, AmazonS3ConnectorConfiguration JobData) SetupConnector(StreamMode streamMode = StreamMode.EventStream)
//    {
//        var configuration = CreateConfiguration();
//        var jobData = new AmazonS3ConnectorConfiguration(configuration);
//        var providerDefinitionId = Guid.Parse("c444cda8-d9b5-45cc-a82d-fef28e08d55c");

//        var container = new WindsorContainer();
//        var applicationContext = new ApplicationContext(container);
//        var mockDateTimeOffsetProvider = new Mock<IDateTimeOffsetProvider>();
//        mockDateTimeOffsetProvider.Setup(x => x.GetCurrentUtcTime())
//            .Returns(new DateTimeOffset(2024, 8, 21, 3, 16, 0, TimeSpan.FromHours(5)));

//        var cache = new Mock<InMemoryApplicationCache>(MockBehavior.Loose, container) { CallBase = true };
//        container.Register(Component.For<IApplicationCache>().Instance(cache.Object));

//        var systemConnectionStrings = new Mock<ISystemConnectionStrings>();
//        container.Register(Component.For<ISystemConnectionStrings>().Instance(systemConnectionStrings.Object));
//        container.Register(Component.For<SystemContext>().Instance(new SystemContext(container)));
//        container.Register(Component.For<ILogger<ExecutionContext>>().Instance(new Mock<ILogger<ExecutionContext>>().Object));
//        container.Register(Component.For<ILogger<OrganizationDataStores>>().Instance(new Mock<ILogger<OrganizationDataStores>>().Object));

//        var organizationDataShard = new Mock<IOrganizationDataShard>();
//        systemConnectionStrings.Setup(x => x.SystemOrganizationDataShard).Returns(organizationDataShard.Object);

//        var organizationId = Guid.NewGuid();
//        var organization = new Organization(applicationContext, organizationId);
//        var organizationRepository = new Mock<IOrganizationRepository>();
//        organizationRepository.Setup(x => x.GetOrganization(It.IsAny<ExecutionContext>(), It.IsAny<Guid>())).Returns(organization);
//        container.Register(Component.For<IOrganizationRepository>().Instance(organizationRepository.Object));

//        var providerDefinitionDataStore = new Mock<IRelationalDataStore<ProviderDefinition>>();
//        providerDefinitionDataStore.Setup(store => store.GetByIdAsync(It.IsAny<ExecutionContext>(), providerDefinitionId))
//            .ReturnsAsync(new ProviderDefinition { IsEnabled = true, ProviderId = AmazonS3ConfigurationConstants.S3ProviderId, Id = providerDefinitionId });
//        container.Register(Component.For<IRelationalDataStore<ProviderDefinition>>().Instance(providerDefinitionDataStore.Object));

//        var streamRepository = new Mock<IStreamRepository>();
//        container.Register(Component.For<IStreamRepository>().Instance(streamRepository.Object));

//        var context = new ExecutionContext(applicationContext, organization, new Mock<ILogger<ExecutionContext>>().Object);

//        var constantsMock = new Mock<IAmazonS3ConfigurationConstants>();
//        constantsMock.Setup(x => x.CacheRecordsThresholdKeyName).Returns("abc");
//        constantsMock.Setup(x => x.CacheRecordsThresholdDefaultValue).Returns(50);
//        constantsMock.Setup(x => x.CacheSyncIntervalKeyName).Returns("abc");
//        constantsMock.Setup(x => x.CacheSyncIntervalDefaultValue).Returns(2000);
//        constantsMock.Setup(x => x.ProviderId).Returns(AmazonS3ConfigurationConstants.S3ProviderId);

//        var jobDataFactoryMock = new Mock<AmazonS3Factory>();
//        jobDataFactoryMock.Setup(x => x.CreateStorageConfiguration(It.IsAny<ExecutionContext>(), providerDefinitionId, It.IsAny<string>()))
//            .ReturnsAsync(jobData);
//        jobDataFactoryMock.Setup(x => x.CreateStorageConfiguration(It.IsAny<ExecutionContext>(), It.IsAny<IDictionary<string, object>>(), It.IsAny<string>()))
//            .ReturnsAsync(jobData);

//        var client = new AmazonS3Client();
//        var connector = new AmazonS3Connector(
//            new Mock<ILogger<AmazonS3Connector>>().Object,
//            applicationContext,
//            constantsMock.Object,
//            jobDataFactoryMock.Object,
//            mockDateTimeOffsetProvider.Object);

//        return (connector, context, configuration, jobData);
//    }

//    private static void SetupJobDataFactory(AmazonS3Connector connector, Dictionary<string, object> configuration, AmazonS3ConnectorConfiguration jobData)
//    {
//        // The connector uses the factory internally via VerifyConnection; the factory mock is already set up
//        // to return the original jobData. For parameter-override tests, the connector calls
//        // GetConfiguration with the provided dictionary which returns the overridden jobData.
//    }

//    private Dictionary<string, object> CreateConfiguration()
//    {
//        var accessKey = Environment.GetEnvironmentVariable("S3_ACCESSKEY");
//        Assert.NotNull(accessKey);
//        var secretKey = Environment.GetEnvironmentVariable("S3_SECRETKEY");
//        Assert.NotNull(secretKey);
//        var region = Environment.GetEnvironmentVariable("S3_REGION");
//        Assert.NotNull(region);
//        var bucketName = Environment.GetEnvironmentVariable("S3_BUCKETNAME");
//        Assert.NotNull(bucketName);

//        return new Dictionary<string, object>
//        {
//            { AmazonS3ConfigurationConstants.AccessKey, accessKey },
//            { AmazonS3ConfigurationConstants.SecretKey, secretKey },
//            { AmazonS3ConfigurationConstants.Region, region },
//            { AmazonS3ConfigurationConstants.BucketName, bucketName },
//            { AmazonS3ConfigurationConstants.DirectoryName, _testPrefix },
//        };
//    }

//    private static IAmazonS3 CreateS3Client()
//    {
//        var accessKey = Environment.GetEnvironmentVariable("S3_ACCESSKEY");
//        var secretKey = Environment.GetEnvironmentVariable("S3_SECRETKEY");
//        var region = Environment.GetEnvironmentVariable("S3_REGION");
//        if (string.IsNullOrEmpty(accessKey) || string.IsNullOrEmpty(secretKey) || string.IsNullOrEmpty(region))
//            return null;
//        return new Amazon.S3.AmazonS3Client(accessKey, secretKey, RegionEndpoint.GetBySystemName(region));
//    }

//    private static string GetBucketName()
//    {
//        return Environment.GetEnvironmentVariable("S3_BUCKETNAME");
//    }

//    private static StreamModel CreateStreamModel(StreamMode mode, Guid providerDefinitionId)
//    {
//        return new StreamModel
//        {
//            Id = Guid.NewGuid(),
//            ConnectorProviderDefinitionId = providerDefinitionId,
//            ContainerName = "test",
//            Mode = mode,
//            ExportIncomingEdges = true,
//            ExportOutgoingEdges = true,
//            Status = StreamStatus.Started,
//            OrganizationId = Guid.NewGuid(),
//        };
//    }

//    private static ConnectorEntityData CreateBaseConnectorEntityData(StreamMode streamMode, VersionChangeType versionChangeType)
//    {
//        var data = new ConnectorEntityData(versionChangeType, streamMode,
//            Guid.Parse("f55c66dc-7881-55c9-889f-344992e71cb8"),
//            new ConnectorEntityPersistInfo("etypzcezkiehwq8vw4oqog==", 1), null,
//            EntityCode.FromKey("/Person#Acceptance:7c5591cf-861a-4642-861d-3b02485854a0"),
//            "/Person",
//            new[]
//            {
//                new ConnectorPropertyData("user.lastName", "Picard",
//                    new VocabularyKeyConnectorPropertyDataType(new VocabularyKey("user.lastName"))),
//                new ConnectorPropertyData("Name", "Jean Luc Picard",
//                    new EntityPropertyConnectorPropertyDataType(typeof(string))),
//            },
//            new[] { EntityCode.FromKey("/Person#Acceptance:7c5591cf-861a-4642-861d-3b02485854a0") },
//            new[]
//            {
//                new EntityEdge(
//                    new EntityReference(EntityCode.FromKey("/Person#Acceptance:7c5591cf-861a-4642-861d-3b02485854a0")),
//                    new EntityReference(EntityCode.FromKey("/EntityA#Somewhere:1234")), "/EntityA")
//            },
//            new[]
//            {
//                new EntityEdge(new EntityReference(EntityCode.FromKey("/EntityB#Somewhere:5678")),
//                    new EntityReference(EntityCode.FromKey("/Person#Acceptance:7c5591cf-861a-4642-861d-3b02485854a0")),
//                    "/EntityB")
//            });
//        return data;
//    }

//    #endregion
//}
public class AmazonS3ConnectorTests : StorageConnectorTestsBase<AmazonS3Connector, AmazonS3Factory, IAmazonS3ConfigurationConstants>
{
    protected override Guid StorageProviderId => AmazonS3ConfigurationConstants.S3ProviderId;

    public AmazonS3ConnectorTests(ITestOutputHelper testOutputHelper)
        : base(testOutputHelper)
    {
    }

    [Fact]
    public async Task VerifyConnection_WhenValidCredentials_ReturnSuccess()
    {
        var configuration = CreateConfigurationWithoutStreamCache();
        var storageConfiguration = new AmazonS3ConnectorConfiguration(configuration);

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
        configuration[nameof(AmazonS3ConfigurationConstants.TenantId)] = "1";
        var storageConfiguration = new AmazonS3ConnectorConfiguration(configuration);

        var setupResult = await SetupContainer(storageConfiguration, StreamMode.EventStream);
        var connector = setupResult.ConnectorMock.Object;
        setupResult.StorageFactoryMock.Setup(factory => factory.CreateStorageConfiguration(setupResult.Context, configuration, It.IsAny<string>()))
            .Returns(Task.FromResult<IStorageConfiguration>(storageConfiguration));
        var result = await connector.VerifyConnection(setupResult.Context, configuration);
        Assert.NotNull(result);
        Assert.False(result.Success);
        Assert.Equal(AmazonS3Connector.InvalidCredentialsErrorMessage, result.ErrorMessage);
    }

    [Fact]
    public async Task VerifyConnection_WhenInvalidClientId_ReturnInvalidCredentialsErrorMessage()
    {
        var configuration = CreateConfigurationWithoutStreamCache();
        configuration[nameof(AmazonS3ConfigurationConstants.ClientId)] = "1";
        var storageConfiguration = new AmazonS3ConnectorConfiguration(configuration);

        var setupResult = await SetupContainer(storageConfiguration, StreamMode.EventStream);
        var connector = setupResult.ConnectorMock.Object;
        setupResult.StorageFactoryMock.Setup(factory => factory.CreateStorageConfiguration(setupResult.Context, configuration, It.IsAny<string>()))
            .Returns(Task.FromResult<IStorageConfiguration>(storageConfiguration));
        var result = await connector.VerifyConnection(setupResult.Context, configuration);
        Assert.NotNull(result);
        Assert.False(result.Success);
        Assert.Equal(AmazonS3Connector.InvalidCredentialsErrorMessage, result.ErrorMessage);
    }

    [Fact]
    public async Task VerifyConnection_WhenInvalidClientSecret_ReturnInvalidCredentialsErrorMessage()
    {
        var configuration = CreateConfigurationWithoutStreamCache();
        configuration[nameof(AmazonS3ConfigurationConstants.ClientSecret)] = "1";
        var storageConfiguration = new AmazonS3ConnectorConfiguration(configuration);

        var setupResult = await SetupContainer(storageConfiguration, StreamMode.EventStream);
        var connector = setupResult.ConnectorMock.Object;
        setupResult.StorageFactoryMock.Setup(factory => factory.CreateStorageConfiguration(setupResult.Context, configuration, It.IsAny<string>()))
            .Returns(Task.FromResult<IStorageConfiguration>(storageConfiguration));
        var result = await connector.VerifyConnection(setupResult.Context, configuration);
        Assert.NotNull(result);
        Assert.False(result.Success);
        Assert.Equal(AmazonS3Connector.InvalidCredentialsErrorMessage, result.ErrorMessage);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData("  ")]
    public async Task VerifyConnection_WhenWorkspaceNameInvalid_ReturnWorkspaceNameInvalidErrorMessage(string workspaceName)
    {
        var configuration = CreateConfigurationWithoutStreamCache();
        configuration[nameof(AmazonS3ConfigurationConstants.WorkspaceName)] = workspaceName;
        var storageConfiguration = new AmazonS3ConnectorConfiguration(configuration);

        var setupResult = await SetupContainer(storageConfiguration, StreamMode.EventStream);
        var connector = setupResult.ConnectorMock.Object;
        setupResult.StorageFactoryMock.Setup(factory => factory.CreateStorageConfiguration(setupResult.Context, configuration, It.IsAny<string>()))
            .Returns(Task.FromResult<IStorageConfiguration>(storageConfiguration));
        var result = await connector.VerifyConnection(setupResult.Context, configuration);
        Assert.NotNull(result);
        Assert.False(result.Success);
        Assert.Equal(AmazonS3Connector.InvalidWorkspaceErrorMessage, result.ErrorMessage);
    }

    [Fact]
    public async Task VerifyConnection_WhenWorkspaceNotFound_ReturnWorkspaceNotFoundErrorMessage()
    {
        var configuration = CreateConfigurationWithoutStreamCache();
        configuration[nameof(AmazonS3ConfigurationConstants.WorkspaceName)] = "1";
        var storageConfiguration = new AmazonS3ConnectorConfiguration(configuration);

        var setupResult = await SetupContainer(storageConfiguration, StreamMode.EventStream);
        var connector = setupResult.ConnectorMock.Object;
        setupResult.StorageFactoryMock.Setup(factory => factory.CreateStorageConfiguration(setupResult.Context, configuration, It.IsAny<string>()))
            .Returns(Task.FromResult<IStorageConfiguration>(storageConfiguration));
        var result = await connector.VerifyConnection(setupResult.Context, configuration);
        Assert.NotNull(result);
        Assert.False(result.Success);
        Assert.Equal(AmazonS3Connector.WorkspaceNotFoundErrorMessageFormat.FormatWith("1"), result.ErrorMessage);
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
        configuration[nameof(AmazonS3ConfigurationConstants.ItemFolder)] = itemFolder;
        var storageConfiguration = new AmazonS3ConnectorConfiguration(configuration);

        var setupResult = await SetupContainer(storageConfiguration, StreamMode.EventStream);
        var connector = setupResult.ConnectorMock.Object;
        setupResult.StorageFactoryMock.Setup(factory => factory.CreateStorageConfiguration(setupResult.Context, configuration, It.IsAny<string>()))
            .Returns(Task.FromResult<IStorageConfiguration>(storageConfiguration));
        var result = await connector.VerifyConnection(setupResult.Context, configuration);
        Assert.NotNull(result);
        Assert.False(result.Success);
        Assert.Equal(AmazonS3Connector.InvalidFolderErrorMessage, result.ErrorMessage);
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
        configuration[nameof(AmazonS3ConfigurationConstants.ItemFolder)] = itemFolder;
        var storageConfiguration = new AmazonS3ConnectorConfiguration(configuration);

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
        var storageConfiguration = new AmazonS3ConnectorConfiguration(configuration);

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
        var storageConfiguration = new AmazonS3ConnectorConfiguration(configuration);

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
                var storageConfiguration = setupResult.StorageConfiguration as AmazonS3ConnectorConfiguration;
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
                values.Add(nameof(AmazonS3ConfigurationConstants.ShouldLoadToTable), true);
                values.Add(nameof(AmazonS3ConfigurationConstants.TableName), tableName);
                values[nameof(AmazonS3ConfigurationConstants.ContainerName)] = tableName;
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
        var logger = new Mock<ILogger<AmazonS3Client>>();
        var exportJob = new AmazonS3ExportEntitiesJob(
            setupResult.ApplicationContext,
            setupResult.StreamRepositoryMock.Object,
            setupResult.ConstantsMock.Object,
            setupResult.StorageFactoryMock.Object,
            setupResult.DateTimeOffsetProviderMock.Object);
        return exportJob;
    }

    private static DataLakeServiceClient GetDataLakeClient(AmazonS3ConnectorConfiguration storageConfiguration)
    {
        var sharedKeyCredential = GetCredential(storageConfiguration);
        return new DataLakeServiceClient(
            new Uri("https://amazons3.dfs.fabric.microsoft.com"),
            sharedKeyCredential);
    }

    private static ClientSecretCredential GetCredential(AmazonS3ConnectorConfiguration storageConfiguration)
    {
        return new ClientSecretCredential(storageConfiguration.TenantId, storageConfiguration.ClientId, storageConfiguration.ClientSecret);
    }

    private protected override Dictionary<string, object> CreateConfigurationWithoutStreamCache()
    {
        var tenantId = Environment.GetEnvironmentVariable("AMAZONS3_TENANTID");
        var clientId = Environment.GetEnvironmentVariable("AMAZONS3_CLIENTID");
        var clientSecretEncoded = Environment.GetEnvironmentVariable("AMAZONS3_CLIENTSECRET");
        var workspaceName = Environment.GetEnvironmentVariable("AMAZONS3_WORKSPACENAME");
        var itemName = Environment.GetEnvironmentVariable("AMAZONS3_ITEMNAME");

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
            { nameof(AmazonS3ConfigurationConstants.TenantId), tenantId },
            { nameof(AmazonS3ConfigurationConstants.ClientId), clientId },
            { nameof(AmazonS3ConfigurationConstants.ClientSecret), clientSecretString },
            { nameof(AmazonS3ConfigurationConstants.WorkspaceName), workspaceName },
            { nameof(AmazonS3ConfigurationConstants.ItemName), itemName },
            { nameof(AmazonS3ConfigurationConstants.ItemFolder), $"Files/{directoryName}" },
            { nameof(AmazonS3ConfigurationConstants.ItemType), "Lakehouse" },
        };
    }

    private protected override Dictionary<string, object> CreateConfigurationWithStreamCache(string format)
    {
        var baseConfiguration = CreateConfigurationWithoutStreamCache();
        var streamCacheConnectionStringEncoded = Environment.GetEnvironmentVariable("AMAZONS3_STREAMCACHE");
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

    protected override Mock<AmazonS3Connector> GetConnectorMock(
        ApplicationContext applicationContext,
        Mock<IDateTimeOffsetProvider> mockDateTimeOffsetProvider,
        Mock<IAmazonS3ConfigurationConstants> constantsMock,
        Mock<AmazonS3Factory> storageConfigurationFactory)
    {
        var mockConnector = new Mock<AmazonS3Connector>(
            new Mock<ILogger<AmazonS3Connector>>().Object,
            applicationContext,
            constantsMock.Object,
            storageConfigurationFactory.Object,
            mockDateTimeOffsetProvider.Object);
        return mockConnector;
    }

    protected override Mock<AmazonS3Factory> CreateStorageFactoryMock(
        WindsorContainer container,
        Mock<IDateTimeOffsetProvider> mockDateTimeOffsetProvider)
    {
        //container.Register(Component.For<AmazonS3Factory>().ImplementedBy<AmazonS3Factory>().LifestyleSingleton());
        var storageFactory = new Mock<AmazonS3Factory>();
        storageFactory.Setup(x => x.CreateStorageClient(It.IsAny<ExecutionContext>(), It.IsAny<IStorageConfiguration>()))
            .Returns<ExecutionContext, IStorageConfiguration>((_, data) => Task.FromResult<IStorageClient>(new AmazonS3Client(NullLogger<AmazonS3Client>.Instance, data as AmazonS3ConnectorConfiguration)));
        return storageFactory;
    }

    private protected override DataLakeServiceClient GetDataLakeClient(SetupContainerResult setupContainerResult)
    {
        return GetDataLakeClient(setupContainerResult.StorageConfiguration as AmazonS3ConnectorConfiguration);
    }

    private protected override string GetDirectoryName(SetupContainerResult setupContainerResult)
    {
        var config = setupContainerResult.StorageConfiguration as AmazonS3ConnectorConfiguration;
        return $"{config.ItemName}.Lakehouse/{config.ItemFolder}";
    }

    private protected override StorageConfigurationBase CreateStorageConfiguration(Dictionary<string, object> configuration)
    {
        return new AmazonS3ConnectorConfiguration(configuration);
    }
}

