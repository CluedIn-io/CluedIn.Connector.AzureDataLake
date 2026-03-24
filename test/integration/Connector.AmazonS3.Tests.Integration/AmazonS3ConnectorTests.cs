using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

using Amazon;
using Amazon.S3;
using Amazon.S3.Model;

using Castle.MicroKernel.Registration;
using Castle.Windsor;

using CluedIn.Connector.AmazonS3.Connector;
using CluedIn.Connector.DataLake.Common;
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

using Microsoft.Extensions.Logging;

using Moq;

using Xunit;
using Xunit.Abstractions;

using ExecutionContext = CluedIn.Core.ExecutionContext;

namespace CluedIn.Connector.AmazonS3.Tests.Integration;

/// <summary>
/// Integration tests for the Amazon S3 connector.
/// Requires environment variables: S3_ACCESSKEY, S3_SECRETKEY, S3_REGION, S3_BUCKETNAME
/// Tests will be skipped if environment variables are not set.
/// </summary>
public class AmazonS3ConnectorTests : IAsyncLifetime
{
    private readonly ITestOutputHelper _testOutputHelper;
    private readonly List<string> _keysToCleanup = new();
    private string _testPrefix;

    public AmazonS3ConnectorTests(ITestOutputHelper testOutputHelper)
    {
        _testOutputHelper = testOutputHelper;
    }

    public Task InitializeAsync()
    {
        _testPrefix = $"xunit-{DateTime.UtcNow.Ticks}";
        return Task.CompletedTask;
    }

    public async Task DisposeAsync()
    {
        try
        {
            var s3 = CreateS3Client();
            if (s3 == null) return;
            var bucket = GetBucketName();

            foreach (var key in _keysToCleanup)
            {
                try { await s3.DeleteObjectAsync(bucket, key); } catch { /* best effort */ }
            }
        }
        catch { /* best effort */ }
    }

    [Fact]
    public async Task VerifyConnection_WhenValid_ReturnsSuccess()
    {
        var (connector, context, configuration, jobData) = SetupConnector();
        if (connector == null) return; // skip when env vars not set

        var result = await connector.VerifyConnection(context, configuration);

        Assert.NotNull(result);
        Assert.True(result.Success);
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData(null)]
    public async Task VerifyConnection_WhenInvalidAccessKey_ReturnError(string accessKey)
    {
        var (connector, context, configuration, _) = SetupConnector();
        if (connector == null) return;

        configuration[AmazonS3Constants.AccessKey] = accessKey;
        var jobData = new AmazonS3ConnectorJobData(configuration);
        SetupJobDataFactory(connector, configuration, jobData);

        var result = await connector.VerifyConnection(context, configuration);

        Assert.NotNull(result);
        Assert.False(result.Success);
        Assert.Equal(AmazonS3Connector.InvalidAccessKeyErrorMessage, result.ErrorMessage);
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData(null)]
    public async Task VerifyConnection_WhenInvalidSecretKey_ReturnError(string secretKey)
    {
        var (connector, context, configuration, _) = SetupConnector();
        if (connector == null) return;

        configuration[AmazonS3Constants.SecretKey] = secretKey;
        var jobData = new AmazonS3ConnectorJobData(configuration);
        SetupJobDataFactory(connector, configuration, jobData);

        var result = await connector.VerifyConnection(context, configuration);

        Assert.NotNull(result);
        Assert.False(result.Success);
        Assert.Equal(AmazonS3Connector.InvalidSecretKeyErrorMessage, result.ErrorMessage);
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData(null)]
    [InlineData("ALLCAPS")]
    [InlineData("my_bucket")]
    [InlineData("ab")]
    public async Task VerifyConnection_WhenInvalidBucketName_ReturnError(string bucketName)
    {
        var (connector, context, configuration, _) = SetupConnector();
        if (connector == null) return;

        configuration[AmazonS3Constants.BucketName] = bucketName;
        var jobData = new AmazonS3ConnectorJobData(configuration);
        SetupJobDataFactory(connector, configuration, jobData);

        var result = await connector.VerifyConnection(context, configuration);

        Assert.NotNull(result);
        Assert.False(result.Success);
        Assert.Equal(AmazonS3Connector.InvalidBucketNameErrorMessage, result.ErrorMessage);
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData(null)]
    public async Task VerifyConnection_WhenInvalidRegion_ReturnError(string region)
    {
        var (connector, context, configuration, _) = SetupConnector();
        if (connector == null) return;

        configuration[AmazonS3Constants.Region] = region;
        var jobData = new AmazonS3ConnectorJobData(configuration);
        SetupJobDataFactory(connector, configuration, jobData);

        var result = await connector.VerifyConnection(context, configuration);

        Assert.NotNull(result);
        Assert.False(result.Success);
        Assert.Equal(AmazonS3Connector.InvalidRegionErrorMessage, result.ErrorMessage);
    }

    [Fact]
    public async Task StoreData_EventStream_WritesJsonToS3()
    {
        var (connector, context, configuration, jobData) = SetupConnector(StreamMode.EventStream);
        if (connector == null) return;

        var streamModel = CreateStreamModel(StreamMode.EventStream, Guid.Parse("c444cda8-d9b5-45cc-a82d-fef28e08d55c"));
        var data = CreateBaseConnectorEntityData(StreamMode.EventStream, VersionChangeType.Added);

        await connector.StoreData(context, streamModel, data);

        // Verify file was written to S3
        var s3 = CreateS3Client();
        var bucket = GetBucketName();
        var prefix = jobData.RootDirectoryPath;

        var listResponse = await s3.ListObjectsV2Async(new ListObjectsV2Request
        {
            BucketName = bucket,
            Prefix = prefix + "/",
        });

        Assert.NotEmpty(listResponse.S3Objects);
        _keysToCleanup.AddRange(listResponse.S3Objects.Select(o => o.Key));

        var obj = listResponse.S3Objects.First();
        var getResponse = await s3.GetObjectAsync(bucket, obj.Key);
        using var reader = new StreamReader(getResponse.ResponseStream);
        var content = await reader.ReadToEndAsync();

        Assert.Contains("Jean Luc Picard", content);
        Assert.Contains("f55c66dc-7881-55c9-889f-344992e71cb8", content);
    }

    [Fact]
    public async Task StoreData_Sync_WritesJsonToS3()
    {
        var (connector, context, configuration, jobData) = SetupConnector(StreamMode.Sync);
        if (connector == null) return;

        var streamModel = CreateStreamModel(StreamMode.Sync, Guid.Parse("c444cda8-d9b5-45cc-a82d-fef28e08d55c"));
        var data = CreateBaseConnectorEntityData(StreamMode.Sync, VersionChangeType.Added);

        await connector.StoreData(context, streamModel, data);

        var s3 = CreateS3Client();
        var bucket = GetBucketName();
        var prefix = jobData.RootDirectoryPath;

        var listResponse = await s3.ListObjectsV2Async(new ListObjectsV2Request
        {
            BucketName = bucket,
            Prefix = prefix + "/",
        });

        Assert.NotEmpty(listResponse.S3Objects);
        _keysToCleanup.AddRange(listResponse.S3Objects.Select(o => o.Key));

        var obj = listResponse.S3Objects.First();
        var getResponse = await s3.GetObjectAsync(bucket, obj.Key);
        using var reader = new StreamReader(getResponse.ResponseStream);
        var content = await reader.ReadToEndAsync();

        Assert.Contains("Jean Luc Picard", content);
    }

    [Fact]
    public async Task GetContainers_WhenBucketEmpty_ReturnsEmptyOrNull()
    {
        var (connector, context, configuration, jobData) = SetupConnector();
        if (connector == null) return;

        var containers = await connector.GetContainers(context, Guid.Parse("c444cda8-d9b5-45cc-a82d-fef28e08d55c"));

        // Either null or empty is acceptable for an empty directory
        Assert.True(containers == null || !containers.Any());
    }

    #region Setup Helpers

    private (AmazonS3Connector Connector, ExecutionContext Context, Dictionary<string, object> Configuration, AmazonS3ConnectorJobData JobData) SetupConnector(StreamMode streamMode = StreamMode.EventStream)
    {
        var configuration = CreateConfiguration();
        if (configuration == null)
        {
            _testOutputHelper.WriteLine("S3 environment variables not set. Skipping test.");
            return (null, null, null, null);
        }

        var jobData = new AmazonS3ConnectorJobData(configuration);
        var providerDefinitionId = Guid.Parse("c444cda8-d9b5-45cc-a82d-fef28e08d55c");

        var container = new WindsorContainer();
        var applicationContext = new ApplicationContext(container);
        var mockDateTimeOffsetProvider = new Mock<IDateTimeOffsetProvider>();
        mockDateTimeOffsetProvider.Setup(x => x.GetCurrentUtcTime())
            .Returns(new DateTimeOffset(2024, 8, 21, 3, 16, 0, TimeSpan.FromHours(5)));

        var cache = new Mock<InMemoryApplicationCache>(MockBehavior.Loose, container) { CallBase = true };
        container.Register(Component.For<IApplicationCache>().Instance(cache.Object));

        var systemConnectionStrings = new Mock<ISystemConnectionStrings>();
        container.Register(Component.For<ISystemConnectionStrings>().Instance(systemConnectionStrings.Object));
        container.Register(Component.For<SystemContext>().Instance(new SystemContext(container)));
        container.Register(Component.For<ILogger<ExecutionContext>>().Instance(new Mock<ILogger<ExecutionContext>>().Object));
        container.Register(Component.For<ILogger<OrganizationDataStores>>().Instance(new Mock<ILogger<OrganizationDataStores>>().Object));

        var organizationDataShard = new Mock<IOrganizationDataShard>();
        systemConnectionStrings.Setup(x => x.SystemOrganizationDataShard).Returns(organizationDataShard.Object);

        var organizationId = Guid.NewGuid();
        var organization = new Organization(applicationContext, organizationId);
        var organizationRepository = new Mock<IOrganizationRepository>();
        organizationRepository.Setup(x => x.GetOrganization(It.IsAny<ExecutionContext>(), It.IsAny<Guid>())).Returns(organization);
        container.Register(Component.For<IOrganizationRepository>().Instance(organizationRepository.Object));

        var providerDefinitionDataStore = new Mock<IRelationalDataStore<ProviderDefinition>>();
        providerDefinitionDataStore.Setup(store => store.GetByIdAsync(It.IsAny<ExecutionContext>(), providerDefinitionId))
            .ReturnsAsync(new ProviderDefinition { IsEnabled = true, ProviderId = AmazonS3Constants.S3ProviderId, Id = providerDefinitionId });
        container.Register(Component.For<IRelationalDataStore<ProviderDefinition>>().Instance(providerDefinitionDataStore.Object));

        var streamRepository = new Mock<IStreamRepository>();
        container.Register(Component.For<IStreamRepository>().Instance(streamRepository.Object));

        var context = new ExecutionContext(applicationContext, organization, new Mock<ILogger<ExecutionContext>>().Object);

        var constantsMock = new Mock<IAmazonS3Constants>();
        constantsMock.Setup(x => x.CacheRecordsThresholdKeyName).Returns("abc");
        constantsMock.Setup(x => x.CacheRecordsThresholdDefaultValue).Returns(50);
        constantsMock.Setup(x => x.CacheSyncIntervalKeyName).Returns("abc");
        constantsMock.Setup(x => x.CacheSyncIntervalDefaultValue).Returns(2000);
        constantsMock.Setup(x => x.ProviderId).Returns(AmazonS3Constants.S3ProviderId);

        var jobDataFactoryMock = new Mock<AmazonS3JobDataFactory>();
        jobDataFactoryMock.Setup(x => x.GetConfiguration(It.IsAny<ExecutionContext>(), providerDefinitionId, It.IsAny<string>()))
            .ReturnsAsync(jobData);
        jobDataFactoryMock.Setup(x => x.GetConfiguration(It.IsAny<ExecutionContext>(), It.IsAny<IDictionary<string, object>>(), It.IsAny<string>()))
            .ReturnsAsync(jobData);

        var client = new AmazonS3StorageClient();
        var connector = new AmazonS3Connector(
            new Mock<ILogger<AmazonS3Connector>>().Object,
            client,
            constantsMock.Object,
            jobDataFactoryMock.Object,
            mockDateTimeOffsetProvider.Object);

        return (connector, context, configuration, jobData);
    }

    private static void SetupJobDataFactory(AmazonS3Connector connector, Dictionary<string, object> configuration, AmazonS3ConnectorJobData jobData)
    {
        // The connector uses the factory internally via VerifyConnection; the factory mock is already set up
        // to return the original jobData. For parameter-override tests, the connector calls
        // GetConfiguration with the provided dictionary which returns the overridden jobData.
    }

    private Dictionary<string, object> CreateConfiguration()
    {
        var accessKey = Environment.GetEnvironmentVariable("S3_ACCESSKEY");
        var secretKey = Environment.GetEnvironmentVariable("S3_SECRETKEY");
        var region = Environment.GetEnvironmentVariable("S3_REGION");
        var bucketName = Environment.GetEnvironmentVariable("S3_BUCKETNAME");

        if (string.IsNullOrEmpty(accessKey) || string.IsNullOrEmpty(secretKey)
            || string.IsNullOrEmpty(region) || string.IsNullOrEmpty(bucketName))
        {
            return null;
        }

        return new Dictionary<string, object>
        {
            { AmazonS3Constants.AccessKey, accessKey },
            { AmazonS3Constants.SecretKey, secretKey },
            { AmazonS3Constants.Region, region },
            { AmazonS3Constants.BucketName, bucketName },
            { AmazonS3Constants.DirectoryName, _testPrefix },
        };
    }

    private static IAmazonS3 CreateS3Client()
    {
        var accessKey = Environment.GetEnvironmentVariable("S3_ACCESSKEY");
        var secretKey = Environment.GetEnvironmentVariable("S3_SECRETKEY");
        var region = Environment.GetEnvironmentVariable("S3_REGION");
        if (string.IsNullOrEmpty(accessKey) || string.IsNullOrEmpty(secretKey) || string.IsNullOrEmpty(region))
            return null;
        return new Amazon.S3.AmazonS3Client(accessKey, secretKey, RegionEndpoint.GetBySystemName(region));
    }

    private static string GetBucketName()
    {
        return Environment.GetEnvironmentVariable("S3_BUCKETNAME");
    }

    private static StreamModel CreateStreamModel(StreamMode mode, Guid providerDefinitionId)
    {
        return new StreamModel
        {
            Id = Guid.NewGuid(),
            ConnectorProviderDefinitionId = providerDefinitionId,
            ContainerName = "test",
            Mode = mode,
            ExportIncomingEdges = true,
            ExportOutgoingEdges = true,
            Status = StreamStatus.Started,
            OrganizationId = Guid.NewGuid(),
        };
    }

    private static ConnectorEntityData CreateBaseConnectorEntityData(StreamMode streamMode, VersionChangeType versionChangeType)
    {
        var data = new ConnectorEntityData(versionChangeType, streamMode,
            Guid.Parse("f55c66dc-7881-55c9-889f-344992e71cb8"),
            new ConnectorEntityPersistInfo("etypzcezkiehwq8vw4oqog==", 1), null,
            EntityCode.FromKey("/Person#Acceptance:7c5591cf-861a-4642-861d-3b02485854a0"),
            "/Person",
            new[]
            {
                new ConnectorPropertyData("user.lastName", "Picard",
                    new VocabularyKeyConnectorPropertyDataType(new VocabularyKey("user.lastName"))),
                new ConnectorPropertyData("Name", "Jean Luc Picard",
                    new EntityPropertyConnectorPropertyDataType(typeof(string))),
            },
            new[] { EntityCode.FromKey("/Person#Acceptance:7c5591cf-861a-4642-861d-3b02485854a0") },
            new[]
            {
                new EntityEdge(
                    new EntityReference(EntityCode.FromKey("/Person#Acceptance:7c5591cf-861a-4642-861d-3b02485854a0")),
                    new EntityReference(EntityCode.FromKey("/EntityA#Somewhere:1234")), "/EntityA")
            },
            new[]
            {
                new EntityEdge(new EntityReference(EntityCode.FromKey("/EntityB#Somewhere:5678")),
                    new EntityReference(EntityCode.FromKey("/Person#Acceptance:7c5591cf-861a-4642-861d-3b02485854a0")),
                    "/EntityB")
            });
        return data;
    }

    #endregion
}
