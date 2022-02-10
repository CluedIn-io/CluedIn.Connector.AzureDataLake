using CluedIn.Connector.AzureDataLake;
using CluedIn.Connector.AzureDataLake.Connector;
using CluedIn.Connector.Common.Caching;
using CluedIn.Core.DataStore;
using Microsoft.Extensions.Logging;
using Moq;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;

namespace Connector.AzureDataLake.Tests.Integration
{

    public class AzureDataLakeConnectorTests
    {
        private AzureDataLakeConnector PrepareConnector()
        {
            Environment.SetEnvironmentVariable("CLUEDIN_CONNECTIONSTRINGS__CLUEDINENTITIES", "Data source=127.0.0.1;Initial catalog=DataStore.Db.OpenCommunication;User Id=sa;Password=***;MultipleActiveResultSets=True;connection timeout=0;Max Pool Size=200;Pooling=True");
            var streamConfigurations = new Dictionary<string, object>()
            {
                { "AccountName", "stdlrok1" },
                { "AccountKey", "***" },
                { "FileSystemName", "newfs" },
                { "DirectoryName", "dmytroTest" },
            };
            var configRepoMock = new Mock<IConfigurationRepository>();
            configRepoMock.Setup(x => x.GetConfigurationById(It.IsAny<CluedIn.Core.ExecutionContext>(), It.IsAny<Guid>())).Returns(streamConfigurations);
            var configRepo = configRepoMock.Object;
            var logger = new Mock<ILogger<AzureDataLakeConnector>>().Object;
            var client = new AzureDataLakeClient();
            var constants = new AzureDataLakeConstants();
            var cachingService = SqlServerCachingService<IDictionary<string, object>, AzureDataLakeConnectorJobData>.CreateCachingService(nameof(AzureDataLakeConnector)).GetAwaiter().GetResult();

            var connector = new AzureDataLakeConnector(configRepo, logger, client, constants, cachingService);

            return connector;
        }

        [Fact]
        public async Task Connector_SyncDebug()
        {
            var connector = PrepareConnector();
            await connector.Sync();

            Assert.True(true);
        }

        [Fact]
        public void AzureDataLakeConnectorJobData_CouldBeDeserialized()
        {
            var configString = "{\"AccountName\":\"stdlrok1\",\"AccountKey\":\"***\",\"FileSystemName\":\"newfs\",\"DirectoryName\":\"dmytroTest\",\"ContainerName\":\"TargetContainer\",\"Configurations\":{\"AccountName\":\"stdlrok1\",\"AccountKey\":\"***\",\"FileSystemName\":\"newfs\",\"DirectoryName\":\"dmytroTest\",\"firstTime\":true},\"CrawlType\":0,\"TargetHost\":null,\"TargetCredentials\":null,\"TargetApiKey\":null,\"LastCrawlFinishTime\":\"0001-01-01T00:00:00+00:00\",\"LastestCursors\":null,\"IsFirstCrawl\":false,\"ExpectedTaskCount\":0,\"IgnoreNextCrawl\":false,\"ExpectedStatistics\":null,\"ExpectedTime\":\"00:00:00\",\"ExpectedData\":0,\"Errors\":null}";
            var jobData = JsonConvert.DeserializeObject<AzureDataLakeConnectorJobData>(configString);

            Assert.NotNull(jobData.Configurations);
        }
    }
}
