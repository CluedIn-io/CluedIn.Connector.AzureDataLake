using Azure.Storage;
using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Models;
using Castle.MicroKernel.Registration;
using Castle.Windsor;
using CluedIn.Connector.AzureDataLake.Connector;
using CluedIn.Connector.Common.Caching;
using CluedIn.Core;
using CluedIn.Core.Accounts;
using CluedIn.Core.Caching;
using CluedIn.Core.Connectors;
using CluedIn.Core.DataStore;
using Microsoft.Extensions.Logging;
using Moq;
using Newtonsoft.Json;
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;
using ExecutionContext = CluedIn.Core.ExecutionContext;

namespace CluedIn.Connector.AzureDataLake.Tests.Integration
{

    public class AzureDataLakeConnectorTests
    {
        private readonly ITestOutputHelper _testOutputHelper;

        public AzureDataLakeConnectorTests(ITestOutputHelper testOutputHelper)
        {
            _testOutputHelper = testOutputHelper;
        }

        [Fact]
        public void AzureDataLakeConnectorJobData_CouldBeDeserialized()
        {
            var configString = "{\"AccountName\":\"stdlrok1\",\"AccountKey\":\"***\",\"FileSystemName\":\"newfs\",\"DirectoryName\":\"dmytroTest\",\"ContainerName\":\"TargetContainer\",\"Configurations\":{\"AccountName\":\"stdlrok1\",\"AccountKey\":\"***\",\"FileSystemName\":\"newfs\",\"DirectoryName\":\"dmytroTest\",\"firstTime\":true},\"CrawlType\":0,\"TargetHost\":null,\"TargetCredentials\":null,\"TargetApiKey\":null,\"LastCrawlFinishTime\":\"0001-01-01T00:00:00+00:00\",\"LastestCursors\":null,\"IsFirstCrawl\":false,\"ExpectedTaskCount\":0,\"IgnoreNextCrawl\":false,\"ExpectedStatistics\":null,\"ExpectedTime\":\"00:00:00\",\"ExpectedData\":0,\"Errors\":null}";
            var jobData = JsonConvert.DeserializeObject<AzureDataLakeConnectorJobData>(configString);

            Assert.NotNull(jobData.Configurations);
        }
        
        [Fact]
        public async void VerifyStoreData()
        {
            var organizationId = Guid.NewGuid();
            var providerDefinitionId = Guid.NewGuid();

            var container = new WindsorContainer();

            container.Register(Component.For<ILogger<OrganizationDataStores>>()
                .Instance(new Mock<ILogger<OrganizationDataStores>>().Object));

            container.Register(Component.For<SystemContext>().Instance(new SystemContext(container)));
            container.Register(Component.For<IApplicationCache>().Instance(new Mock<IApplicationCache>().Object));

            var applicationContext = new ApplicationContext(container);
            var organization = new Organization(applicationContext, organizationId);

            var logger = new Mock<ILogger>().Object;

            var context = new ExecutionContext(applicationContext, organization, logger);

            var azureDataLakeConstantsMock = new Mock<IAzureDataLakeConstants>();
            azureDataLakeConstantsMock.Setup(x => x.CacheRecordsThresholdKeyName).Returns("abc");
            azureDataLakeConstantsMock.Setup(x => x.CacheRecordsThresholdDefaultValue).Returns(50);
            azureDataLakeConstantsMock.Setup(x => x.CacheSyncIntervalKeyName).Returns("abc");
            azureDataLakeConstantsMock.Setup(x => x.CacheSyncIntervalDefaultValue).Returns(2000);

            var accountName = Environment.GetEnvironmentVariable("ADL2_ACCOUNTNAME");
            Assert.NotNull(accountName);
            var accountKey = Environment.GetEnvironmentVariable("ADL2_ACCOUNTKEY");
            Assert.NotNull(accountKey);

            var fileSystemName = $"xunit-fs-{DateTime.Now.Ticks}";
            var directoryName = $"xunit-{DateTime.Now.Ticks}";

            var connectorConnectionMock = new Mock<IConnectorConnection>();
            connectorConnectionMock.Setup(x => x.Authentication).Returns(new Dictionary<string, object>()
            {
                { "AccountName", accountName },
                { "AccountKey", accountKey },
                { "FileSystemName", fileSystemName },
                { "DirectoryName", directoryName },
            });

            var connectorMock = new Mock<AzureDataLakeConnector>(
                new Mock<IConfigurationRepository>().Object,
                new Mock<ILogger<AzureDataLakeConnector>>().Object,
                new AzureDataLakeClient(),
                azureDataLakeConstantsMock.Object,
                new InMemoryCachingService<IDictionary<string, object>, AzureDataLakeConnectorJobData>()
            );
            connectorMock.Setup(x => x.GetAuthenticationDetails(context, providerDefinitionId))
                .Returns(Task.FromResult(connectorConnectionMock.Object));
            connectorMock.CallBase = true;

            var connector = connectorMock.Object;

            var data = new Dictionary<string, object>();
            data.Add("test", "hello world");

            await connector.StoreData(context, providerDefinitionId, null, data);

            var client = new DataLakeServiceClient(new Uri($"https://{accountName}.dfs.core.windows.net"),
                new StorageSharedKeyCredential(accountName, accountKey));

            PathItem path;
            DataLakeFileSystemClient fsClient;

            var d = DateTime.Now;
            while (true)
            {
                if (DateTime.Now > d.AddSeconds(30))
                {
                    throw new TimeoutException();
                }

                if(client.GetFileSystems().All(fs => fs.Name != fileSystemName))
                {
                    continue;
                }

                fsClient = client.GetFileSystemClient(fileSystemName);

                var paths = fsClient.GetPaths(recursive: true)
                    .Where(p => p.IsDirectory == false)
                    .Where(p => p.Name.Contains(directoryName))
                    .ToArray();

                if (paths.Length == 0)
                {
                    Thread.Sleep(1000);
                    continue;
                }

                path = paths.Single();

                if (path.ContentLength > 0)
                {
                    break;
                }
            }

            var fileClient = fsClient.GetFileClient(path.Name);

            var content = new StreamReader(fileClient.Read().Value.Content).ReadToEnd();

            _testOutputHelper.WriteLine(content);

            Assert.Equal($@"[
  {{
    ""test"": ""hello world"",
    ""ProviderDefinitionId"": ""{providerDefinitionId}"",
    ""ContainerName"": null
  }}
]", content);

            await fsClient.GetDirectoryClient(directoryName).DeleteAsync();
        }

        [Fact]
        public async void VerifyStoreEdge()
        {
            var organizationId = Guid.NewGuid();
            var providerDefinitionId = Guid.NewGuid();

            var container = new WindsorContainer();

            container.Register(Component.For<ILogger<OrganizationDataStores>>()
                .Instance(new Mock<ILogger<OrganizationDataStores>>().Object));

            container.Register(Component.For<SystemContext>().Instance(new SystemContext(container)));
            container.Register(Component.For<IApplicationCache>().Instance(new Mock<IApplicationCache>().Object));

            var applicationContext = new ApplicationContext(container);
            var organization = new Organization(applicationContext, organizationId);

            var logger = new Mock<ILogger>().Object;

            var context = new ExecutionContext(applicationContext, organization, logger);

            var azureDataLakeConstantsMock = new Mock<IAzureDataLakeConstants>();
            azureDataLakeConstantsMock.Setup(x => x.CacheRecordsThresholdKeyName).Returns("abc");
            azureDataLakeConstantsMock.Setup(x => x.CacheRecordsThresholdDefaultValue).Returns(50);
            azureDataLakeConstantsMock.Setup(x => x.CacheSyncIntervalKeyName).Returns("abc");
            azureDataLakeConstantsMock.Setup(x => x.CacheSyncIntervalDefaultValue).Returns(1000);

            var environmentVariables = Environment.GetEnvironmentVariables();
            foreach (DictionaryEntry environmentVariable in environmentVariables)
            {
                Console.WriteLine($"{environmentVariable.Key}:{environmentVariable.Value}");
            }

            var accountName = Environment.GetEnvironmentVariable("ADL2_ACCOUNTNAME");
            Assert.NotNull(accountName);
            var accountKey = Environment.GetEnvironmentVariable("ADL2_ACCOUNTKEY");
            Assert.NotNull(accountKey);

            var fileSystemName = "test";
            var directoryName = $"xunit-{DateTime.Now.Ticks}";

            var connectorConnectionMock = new Mock<IConnectorConnection>();
            connectorConnectionMock.Setup(x => x.Authentication).Returns(new Dictionary<string, object>()
            {
                { "AccountName", accountName },
                { "AccountKey", accountKey },
                { "FileSystemName", fileSystemName },
                { "DirectoryName", directoryName },
            });

            var connectorMock = new Mock<AzureDataLakeConnector>(
                new Mock<IConfigurationRepository>().Object,
                new Mock<ILogger<AzureDataLakeConnector>>().Object,
                new AzureDataLakeClient(),
                azureDataLakeConstantsMock.Object,
                new InMemoryCachingService<IDictionary<string, object>, AzureDataLakeConnectorJobData>()
            );
            connectorMock.Setup(x => x.GetAuthenticationDetails(context, providerDefinitionId))
                .Returns(Task.FromResult(connectorConnectionMock.Object));
            connectorMock.CallBase = true;

            var connector = connectorMock.Object;

            await connector.StoreEdgeData(context, providerDefinitionId, null, "code", new[] { "Edge1", "Edge2" });

            var client = new DataLakeServiceClient(new Uri($"https://{accountName}.dfs.core.windows.net"),
                new StorageSharedKeyCredential(accountName, accountKey));

            PathItem path;
            DataLakeFileSystemClient fsClient;

            var d = DateTime.Now;
            while (true)
            {
                if (DateTime.Now > d.AddSeconds(30))
                {
                    throw new TimeoutException();
                }

                if (client.GetFileSystems().All(fs => fs.Name != fileSystemName))
                {
                    continue;
                }

                fsClient = client.GetFileSystemClient(fileSystemName);

                var paths = fsClient.GetPaths(recursive: true)
                    .Where(p => p.IsDirectory == false)
                    .Where(p => p.Name.Contains(directoryName))
                    .ToArray();

                if (paths.Length == 0)
                {
                    Thread.Sleep(1000);
                    continue;
                }

                path = paths.Single();

                if (path.ContentLength > 0)
                {
                    break;
                }
            }

            var fileClient = fsClient.GetFileClient(path.Name);

            var content = new StreamReader(fileClient.Read().Value.Content).ReadToEnd();

            _testOutputHelper.WriteLine(content);

            Assert.Equal($@"[
  {{
    ""ProviderDefinitionId"": ""{providerDefinitionId}"",
    ""ContainerName"": null,
    ""OriginEntityCode"": ""code"",
    ""Edges"": [
      ""Edge1"",
      ""Edge2""
    ]
  }}
]", content);

            await client.DeleteFileSystemAsync(fileSystemName);
        }
    }
}
