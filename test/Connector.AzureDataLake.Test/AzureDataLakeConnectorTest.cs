using System.Collections.Generic;
using System.Threading.Tasks;
using AutoFixture.Xunit2;
using CluedIn.Connector.AzureDataLake;
using CluedIn.Connector.AzureDataLake.Connector;
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
        private readonly AzureDataLakeConnector connector;

        public AzureDataLakeConnectorTest()
        {
            repository = new Mock<IConfigurationRepository>();
            logger = new Mock<ILogger<AzureDataLakeConnector>>();
            connector = new AzureDataLakeConnector(repository.Object, logger.Object, new AzureDataLakeClient(), new AzureDataLakeConstants());
        }

        [Fact]
        public void VerifyConnectionTest()
        {
            var authenticationData = new Dictionary<string, object>
            {
                { "AccountName", "adlsg2testingapac" },
                { "AccountKey", "Z2doSCJflj6NP+6J0r4uwP/ydLs9VhZdqevtesYyvVpjzR1+shCcMbYaCvsJ++Lb4Mr6bpySvb3DlmUSdf1G3g==" },
                { "FileSystemName", "chadfilessystem" },
                { "DirectoryName", "chaddir" },
            };
            connector.VerifyConnection(null, authenticationData).Result.ShouldBeTrue();
        }
    }
}
