using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using Castle.Windsor;

using CluedIn.Connector.FileStorage.Common;
using CluedIn.Core;
using CluedIn.Core.Providers;

using Microsoft.Extensions.Logging.Abstractions;

using Moq;

using Xunit;

namespace CluedIn.Connector.FabricOpenMirroring.Tests.Integration;

public class OpenMirroringConnectorProviderTests
{
    private readonly OpenMirroringConnectorProvider _provider;

    public OpenMirroringConnectorProviderTests()
    {
        var container = new WindsorContainer();
        var applicationContext = new ApplicationContext(container);
        var constantsMock = new Mock<IOpenMirroringConfigurationConstants>();
        constantsMock.Setup(x => x.ProviderId).Returns(OpenMirroringConfigurationConstants.DataLakeProviderId);
        constantsMock.Setup(x => x.CreateProviderMetadata()).Returns(new ProviderMetadata
        {
            Id = OpenMirroringConfigurationConstants.DataLakeProviderId,
            Name = "FabricOpenMirroring",
        });
        constantsMock.Setup(x => x.MirroredDatabaseCreationRetryIntervalKeyName).Returns("test");
        constantsMock.Setup(x => x.MirroredDatabaseCreationRetryIntervalDefaultValue).Returns(10);

        _provider = new OpenMirroringConnectorProvider(
            applicationContext,
            constantsMock.Object,
            NullLogger<OpenMirroringConnectorProvider>.Instance);
    }

    [Fact]
    public async Task GetCrawlJobData_WhenMirroredDatabaseNameIsEmpty_GeneratesAutoName()
    {
        // Arrange
        var providerDefinitionId = Guid.NewGuid();
        var configuration = new Dictionary<string, object>
        {
            { nameof(OpenMirroringConfigurationConstants.MirroredDatabaseName), "" },
            { nameof(OpenMirroringConfigurationConstants.WorkspaceName), "MyWorkspace" },
        };

        // Act
        var result = await _provider.GetCrawlJobData(null, configuration, Guid.NewGuid(), Guid.NewGuid(), providerDefinitionId);

        // Assert
        var wrapper = Assert.IsType<CrawlJobDataWrapper>(result);
        var dbName = wrapper.Configurations[nameof(OpenMirroringConfigurationConstants.MirroredDatabaseName)] as string;
        Assert.Equal($"CluedIn_ExportTarget_{providerDefinitionId:N}", dbName);
    }

    [Fact]
    public async Task GetCrawlJobData_WhenMirroredDatabaseNameIsWhitespace_GeneratesAutoName()
    {
        // Arrange
        var providerDefinitionId = Guid.NewGuid();
        var configuration = new Dictionary<string, object>
        {
            { nameof(OpenMirroringConfigurationConstants.MirroredDatabaseName), "   " },
            { nameof(OpenMirroringConfigurationConstants.WorkspaceName), "MyWorkspace" },
        };

        // Act
        var result = await _provider.GetCrawlJobData(null, configuration, Guid.NewGuid(), Guid.NewGuid(), providerDefinitionId);

        // Assert
        var wrapper = Assert.IsType<CrawlJobDataWrapper>(result);
        var dbName = wrapper.Configurations[nameof(OpenMirroringConfigurationConstants.MirroredDatabaseName)] as string;
        Assert.Equal($"CluedIn_ExportTarget_{providerDefinitionId:N}", dbName);
    }

    [Fact]
    public async Task GetCrawlJobData_WhenMirroredDatabaseNameNotPresent_GeneratesAutoName()
    {
        // Arrange
        var providerDefinitionId = Guid.NewGuid();
        var configuration = new Dictionary<string, object>
        {
            { nameof(OpenMirroringConfigurationConstants.WorkspaceName), "MyWorkspace" },
        };

        // Act
        var result = await _provider.GetCrawlJobData(null, configuration, Guid.NewGuid(), Guid.NewGuid(), providerDefinitionId);

        // Assert
        var wrapper = Assert.IsType<CrawlJobDataWrapper>(result);
        var dbName = wrapper.Configurations[nameof(OpenMirroringConfigurationConstants.MirroredDatabaseName)] as string;
        Assert.Equal($"CluedIn_ExportTarget_{providerDefinitionId:N}", dbName);
    }

    [Fact]
    public async Task GetCrawlJobData_WhenMirroredDatabaseNameHasWhitespace_TrimsDatabaseName()
    {
        // Arrange
        var providerDefinitionId = Guid.NewGuid();
        var configuration = new Dictionary<string, object>
        {
            { nameof(OpenMirroringConfigurationConstants.MirroredDatabaseName), "  MyDatabase  " },
            { nameof(OpenMirroringConfigurationConstants.WorkspaceName), "MyWorkspace" },
        };

        // Act
        var result = await _provider.GetCrawlJobData(null, configuration, Guid.NewGuid(), Guid.NewGuid(), providerDefinitionId);

        // Assert
        var wrapper = Assert.IsType<CrawlJobDataWrapper>(result);
        var dbName = wrapper.Configurations[nameof(OpenMirroringConfigurationConstants.MirroredDatabaseName)] as string;
        Assert.Equal("MyDatabase", dbName);
    }

    [Fact]
    public async Task GetCrawlJobData_WhenWorkspaceNameHasWhitespace_TrimsWorkspaceName()
    {
        // Arrange
        var providerDefinitionId = Guid.NewGuid();
        var configuration = new Dictionary<string, object>
        {
            { nameof(OpenMirroringConfigurationConstants.MirroredDatabaseName), "MyDatabase" },
            { nameof(OpenMirroringConfigurationConstants.WorkspaceName), "  MyWorkspace  " },
        };

        // Act
        var result = await _provider.GetCrawlJobData(null, configuration, Guid.NewGuid(), Guid.NewGuid(), providerDefinitionId);

        // Assert
        var wrapper = Assert.IsType<CrawlJobDataWrapper>(result);
        var workspaceName = wrapper.Configurations[nameof(OpenMirroringConfigurationConstants.WorkspaceName)] as string;
        Assert.Equal("MyWorkspace", workspaceName);
    }

    [Fact]
    public async Task GetCrawlJobData_WhenValidDatabaseName_DoesNotModifyName()
    {
        // Arrange
        var providerDefinitionId = Guid.NewGuid();
        var configuration = new Dictionary<string, object>
        {
            { nameof(OpenMirroringConfigurationConstants.MirroredDatabaseName), "MyDatabase" },
            { nameof(OpenMirroringConfigurationConstants.WorkspaceName), "MyWorkspace" },
        };

        // Act
        var result = await _provider.GetCrawlJobData(null, configuration, Guid.NewGuid(), Guid.NewGuid(), providerDefinitionId);

        // Assert
        var wrapper = Assert.IsType<CrawlJobDataWrapper>(result);
        var dbName = wrapper.Configurations[nameof(OpenMirroringConfigurationConstants.MirroredDatabaseName)] as string;
        Assert.Equal("MyDatabase", dbName);
    }
}
