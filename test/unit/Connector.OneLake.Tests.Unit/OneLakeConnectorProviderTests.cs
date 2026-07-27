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

namespace CluedIn.Connector.OneLake.Tests.Unit;

public class OneLakeConnectorProviderTests
{
    private readonly OneLakeConnectorProvider _provider;

    public OneLakeConnectorProviderTests()
    {
        var container = new WindsorContainer();
        var applicationContext = new ApplicationContext(container);
        var constantsMock = new Mock<IOneLakeConfigurationConstants>();
        constantsMock.Setup(x => x.ProviderId).Returns(OneLakeConfigurationConstants.DataLakeProviderId);
        constantsMock.Setup(x => x.CreateProviderMetadata()).Returns(new ProviderMetadata
        {
            Id = OneLakeConfigurationConstants.DataLakeProviderId,
            Name = "OneLake",
        });

        _provider = new OneLakeConnectorProvider(
            applicationContext,
            constantsMock.Object,
            NullLogger<OneLakeConnectorProvider>.Instance);
    }

    [Theory]
    [InlineData("  MyWorkspace  ", "MyWorkspace")]
    [InlineData("MyWorkspace", "MyWorkspace")]
    [InlineData(" ", "")]
    public async Task GetCrawlJobData_TrimsWorkspaceName(string input, string expected)
    {
        var configuration = new Dictionary<string, object>
        {
            { nameof(OneLakeConfigurationConstants.WorkspaceName), input },
            { nameof(OneLakeConfigurationConstants.ItemName), "item" },
            { nameof(OneLakeConfigurationConstants.ItemFolder), "folder" },
        };

        var result = await _provider.GetCrawlJobData(null, configuration, Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid());

        var wrapper = Assert.IsType<CrawlJobDataWrapper>(result);
        Assert.Equal(expected, wrapper.Configurations[nameof(OneLakeConfigurationConstants.WorkspaceName)]);
    }

    [Theory]
    [InlineData("  MyItem  ", "MyItem")]
    [InlineData("MyItem", "MyItem")]
    public async Task GetCrawlJobData_TrimsItemName(string input, string expected)
    {
        var configuration = new Dictionary<string, object>
        {
            { nameof(OneLakeConfigurationConstants.WorkspaceName), "workspace" },
            { nameof(OneLakeConfigurationConstants.ItemName), input },
            { nameof(OneLakeConfigurationConstants.ItemFolder), "folder" },
        };

        var result = await _provider.GetCrawlJobData(null, configuration, Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid());

        var wrapper = Assert.IsType<CrawlJobDataWrapper>(result);
        Assert.Equal(expected, wrapper.Configurations[nameof(OneLakeConfigurationConstants.ItemName)]);
    }

    [Theory]
    [InlineData("  My Folder  ", "My Folder")]
    [InlineData("folder", "folder")]
    [InlineData(" folder with spaces ", "folder with spaces")]
    public async Task GetCrawlJobData_TrimsItemFolder(string input, string expected)
    {
        var configuration = new Dictionary<string, object>
        {
            { nameof(OneLakeConfigurationConstants.WorkspaceName), "workspace" },
            { nameof(OneLakeConfigurationConstants.ItemName), "item" },
            { nameof(OneLakeConfigurationConstants.ItemFolder), input },
        };

        var result = await _provider.GetCrawlJobData(null, configuration, Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid());

        var wrapper = Assert.IsType<CrawlJobDataWrapper>(result);
        Assert.Equal(expected, wrapper.Configurations[nameof(OneLakeConfigurationConstants.ItemFolder)]);
    }

    [Fact]
    public async Task GetCrawlJobData_WhenItemFolderContainsSpaces_PreservesInternalSpaces()
    {
        var configuration = new Dictionary<string, object>
        {
            { nameof(OneLakeConfigurationConstants.WorkspaceName), "workspace" },
            { nameof(OneLakeConfigurationConstants.ItemName), "item" },
            { nameof(OneLakeConfigurationConstants.ItemFolder), "  my folder name  " },
        };

        var result = await _provider.GetCrawlJobData(null, configuration, Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid());

        var wrapper = Assert.IsType<CrawlJobDataWrapper>(result);
        Assert.Equal("my folder name", wrapper.Configurations[nameof(OneLakeConfigurationConstants.ItemFolder)]);
    }
}
