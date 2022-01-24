using System.Collections.Generic;
using System.Threading.Tasks;
using AutoFixture.Xunit2;
using CluedIn.Connector.AzureDataLake;
using CluedIn.Connector.AzureDataLake.Connector;
using CluedIn.Connector.Common.Caching;
using CluedIn.Core.DataStore;
using Microsoft.Extensions.Logging;
using Moq;
using Shouldly;
using Xunit;

namespace Connector.AzureDataLakeTests
{
    public class AzureDataLakeConnectorTest
    {
        private readonly Mock<IConfigurationRepository> repository;
        public readonly Mock<ILogger<AzureDataLakeConnector>> logger;
        private readonly Mock<ICachingService<IDictionary<string, object>, AzureDataLakeConnectorJobData>> cachingService;
        private readonly AzureDataLakeConnector connector;

        public AzureDataLakeConnectorTest()
        {
            repository = new Mock<IConfigurationRepository>();
            logger = new Mock<ILogger<AzureDataLakeConnector>>();
            cachingService = new Mock<ICachingService<IDictionary<string, object>, AzureDataLakeConnectorJobData>>();
            connector = new AzureDataLakeConnector(repository.Object, logger.Object, new AzureDataLakeClient(), new AzureDataLakeConstants(), cachingService.Object);
        }

        [Fact]
        public void VerifyConnectionTest()
        {
            var authenticationData = new Dictionary<string, object>
            {
                { "AccountName", "name" },
                { "AccountKey", "token" },
                { "FileSystemName", "file" },
                { "DirectoryName", "dir" },
            };
            connector.VerifyConnection(null, authenticationData).Result.ShouldBeTrue();
        }
    }
}
