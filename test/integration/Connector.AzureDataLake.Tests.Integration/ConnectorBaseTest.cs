using Azure.Storage.Files.DataLake;
using Azure.Storage;
using System;
using Xunit;
using CluedIn.Core.Accounts;
using CluedIn.Core;
using Castle.Windsor;
using Moq;
using Microsoft.Extensions.Logging;
using CluedIn.Core.Caching;
using Castle.MicroKernel.Registration;
using CluedIn.Core.Connectors;
using System.Collections.Generic;
using CluedIn.Connector.AzureDataLake.Connector;
using System.Threading.Tasks;
using CluedIn.Core.Streams.Models;
using Azure.Storage.Files.DataLake.Models;
using System.Linq;
using CluedIn.Core.Data.Relational;
using CluedIn.Core.DataStore.Entities.MicroServices;
using Microsoft.EntityFrameworkCore;

namespace CluedIn.Connector.AzureDataLake.Tests.Integration;

public class ConnectorBaseTest
{
    internal readonly string Adl2_accountName;
    internal readonly string Adl2_accountKey;
    internal readonly string Adl2_fileSystemName;
    internal readonly string Adl2_directoryName;
    internal readonly DataLakeServiceClient DataLakeServiceClient;

    internal ExecutionContext Context;
    internal AzureDataLakeConnector AzureDataLakeConnector;
    internal IReadOnlyStreamModel StreamModel;

    private Guid _providerDefinitionId;

    public ConnectorBaseTest()
    {
        Adl2_accountName = Environment.GetEnvironmentVariable("ADL2_ACCOUNTNAME") ?? "adlsg2testingapac";
        Assert.NotNull(Adl2_accountName);
        Adl2_accountKey = Environment.GetEnvironmentVariable("ADL2_ACCOUNTKEY") ?? "Z2doSCJflj6NP+6J0r4uwP/ydLs9VhZdqevtesYyvVpjzR1+shCcMbYaCvsJ++Lb4Mr6bpySvb3DlmUSdf1G3g==";
        Assert.NotNull(Adl2_accountKey);

        Adl2_fileSystemName = $"xunit-fs-{DateTime.Now.Ticks}";
        Adl2_directoryName = $"xunit-{DateTime.Now.Ticks}";

        DataLakeServiceClient = new DataLakeServiceClient(new Uri($"https://{Adl2_accountName}.dfs.core.windows.net"),
            new StorageSharedKeyCredential(Adl2_accountName, Adl2_accountKey));
    }

    internal void InitializeMockObjects()
    {
        var organizationId = Guid.NewGuid();
        _providerDefinitionId = Guid.Parse("c444cda8-d9b5-45cc-a82d-fef28e08d55c");

        var container = new WindsorContainer();
        container.Register(Component.For<ILogger<OrganizationDataStores>>()
            .Instance(Mock.Of<ILogger<OrganizationDataStores>>()));
        container.Register(Component.For<SystemContext>()
            .Instance(new SystemContext(container)));
        container.Register(Component.For<IApplicationCache>()
            .Instance(Mock.Of<IApplicationCache>()));

        var applicationContext = new ApplicationContext(container);
        var organization = new Organization(applicationContext, organizationId);
        Context = new ExecutionContext(applicationContext, organization, Mock.Of<ILogger>());

        var azureDataLakeConstantsMock = new Mock<IAzureDataLakeConstants>();
        azureDataLakeConstantsMock.Setup(x => x.CacheRecordsThresholdKeyName).Returns("abc");
        azureDataLakeConstantsMock.Setup(x => x.CacheRecordsThresholdDefaultValue).Returns(50);
        azureDataLakeConstantsMock.Setup(x => x.CacheSyncIntervalKeyName).Returns("abc");
        azureDataLakeConstantsMock.Setup(x => x.CacheSyncIntervalDefaultValue).Returns(10000);

        var connectorConnectionMock = new Mock<IConnectorConnectionV2>();
        connectorConnectionMock.Setup(x => x.Authentication).Returns(new Dictionary<string, object>()
            {
                { "AccountName", Adl2_accountName },
                { "AccountKey", Adl2_accountKey },
                { "FileSystemName", Adl2_fileSystemName },
                { "DirectoryName", Adl2_directoryName },
            });

        var connectorMock = new Mock<AzureDataLakeConnector>(
            Mock.Of<ILogger<AzureDataLakeConnector>>(),
            new AzureDataLakeClient(),
            azureDataLakeConstantsMock.Object,
            Mock.Of<DbContextOptions<MicroServiceEntities>>()
        );
        connectorMock.Setup(x => x.GetAuthenticationDetails(Context, _providerDefinitionId))
            .Returns(Task.FromResult(connectorConnectionMock.Object));
        connectorMock.CallBase = true;

        AzureDataLakeConnector = connectorMock.Object;
    }

    internal void InitializeStreamModel(StreamMode streamMode, string containerName = "test")
    {
        var streamModel = new Mock<IReadOnlyStreamModel>();
        streamModel.Setup(x => x.OrganizationId).Returns(Context.Organization.Id);
        streamModel.Setup(x => x.ConnectorProviderDefinitionId).Returns(_providerDefinitionId);
        streamModel.Setup(x => x.ContainerName).Returns(containerName);
        streamModel.Setup(x => x.Mode).Returns(streamMode);

        StreamModel = streamModel.Object;
    }

    internal PathItem[] GetPathItems()
    {
        var d = DateTime.Now;
        while (true)
        {
            if (DateTime.Now > d.AddSeconds(30))
            {
                throw new TimeoutException();
            }

            if (DataLakeServiceClient.GetFileSystems().All(fs => fs.Name != Adl2_fileSystemName))
            {
                continue;
            }

            var fsClient = DataLakeServiceClient.GetFileSystemClient(Adl2_fileSystemName);

            var paths = fsClient.GetPaths(recursive: true)
                .Where(p => p.IsDirectory == false)
                .Where(p => p.Name.Contains(Adl2_directoryName))
                .ToArray();

            if (paths.Length == 0)
            {
                System.Threading.Thread.Sleep(1000);
                continue;
            }

            return paths;
        }
    }
}
