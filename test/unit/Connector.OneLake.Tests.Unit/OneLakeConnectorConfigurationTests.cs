using System.Collections.Generic;

using Xunit;

namespace CluedIn.Connector.OneLake.Tests.Unit;

public class OneLakeConnectorConfigurationTests
{
    private static OneLakeConnectorConfiguration CreateConfiguration(
        string workspaceName = "workspace",
        string itemName = "item",
        string itemType = "Lakehouse",
        string itemFolder = "Files")
    {
        var configurations = new Dictionary<string, object>
        {
            { OneLakeConfigurationConstants.WorkspaceName, workspaceName },
            { OneLakeConfigurationConstants.ItemName, itemName },
            { OneLakeConfigurationConstants.ItemType, itemType },
            { OneLakeConfigurationConstants.ItemFolder, itemFolder },
            { OneLakeConfigurationConstants.ClientId, "client-id" },
            { OneLakeConfigurationConstants.ClientSecret, "client-secret" },
            { OneLakeConfigurationConstants.TenantId, "tenant-id" },
        };

        return new OneLakeConnectorConfiguration(configurations);
    }

    [Theory]
    [InlineData("  MyWorkspace  ", "MyWorkspace")]
    [InlineData("MyWorkspace", "MyWorkspace")]
    [InlineData(" ", "")]
    public void WorkspaceName_IsTrimmed(string input, string expected)
    {
        var config = CreateConfiguration(workspaceName: input);
        Assert.Equal(expected, config.WorkspaceName);
    }

    [Theory]
    [InlineData("  MyItem  ", "MyItem")]
    [InlineData("MyItem", "MyItem")]
    [InlineData(" ", "")]
    public void ItemName_IsTrimmed(string input, string expected)
    {
        var config = CreateConfiguration(itemName: input);
        Assert.Equal(expected, config.ItemName);
    }

    [Theory]
    [InlineData("  My Folder  ", "My Folder")]
    [InlineData("folder", "folder")]
    [InlineData(" folder with spaces ", "folder with spaces")]
    public void ItemFolder_IsTrimmed(string input, string expected)
    {
        var config = CreateConfiguration(itemFolder: input);
        Assert.Equal(expected, config.ItemFolder);
    }

    [Fact]
    public void ItemFolder_WithInternalSpaces_PreservesInternalSpaces()
    {
        var config = CreateConfiguration(itemFolder: "  my folder name  ");
        Assert.Equal("my folder name", config.ItemFolder);
    }

    [Fact]
    public void RootDirectoryPath_UsesTrimmedValues()
    {
        var config = CreateConfiguration(itemName: "  myItem  ", itemType: "Lakehouse", itemFolder: "  myFolder  ");
        Assert.Equal("myItem.Lakehouse/myFolder", config.RootDirectoryPath);
    }
}
